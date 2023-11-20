package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type Trade struct {
	Time    int64
	Price   float64
	Volume  float64
	Side    string
	Type    string
	Misc    string
	TradeID int64
}

func main() {
	err := godotenv.Load("env/psqllogin.env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	// TO DO:
	// If table for pair !exists
	// Get first data for pair since=0
	// loop until last=current_time ? last won't ever be current time...
	// loop until api sends number of trades less than 1000?
	// If table for pair exists
	// Get pair since=last_entry
	// loop until
	// Write data into table
	KrakenSpotPairs := GetKrakenSpotPairs()
	var DesiredPair string
	// TO DO:
	// Allow user to enter q for quit or ? for list of pairs
	// Keep prompting user for inputs until pair is found in list of tradeable assets
	for {
		DesiredPair = strings.ToUpper(GetUserDesiredPair())
		if PairExists(DesiredPair, KrakenSpotPairs) {
			break
		} else {
			fmt.Println("Pair not found, try checking spelling or entering another pair.")
		}
	}

	// connect to postgres table
	// required to create database outside of this program using Shell
	// and create timescaledb extension if not exists
	ctx := context.Background()
	connStr := "postgres://" + os.Getenv("USER") + ":" + os.Getenv("PASS") + "@" + os.Getenv("HOST") + ":" + os.Getenv("PORT") + "/" + os.Getenv("DBNAME")
	log.Println(connStr)
	dbpool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Printf("Unable to connect to database: %v\n", os.Stderr)
		os.Exit(1)
	}
	defer dbpool.Close()

	// check if table exists with dbname
	var tableExists bool
	tableName := os.Getenv("DBNAME") + "_kraken_trades"
	queryTableExists := `SELECT EXISTS (
		SELECT 1
		FROM information_schema.tables
		WHERE table_name = '` + tableName + `'
		) AS table_existence;`
	err = dbpool.QueryRow(ctx, queryTableExists).Scan(&tableExists)
	if err != nil {
		log.Printf("QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	log.Println(tableExists)

	var querySince = 0
	// create hypertable if not exists
	if !tableExists {
		log.Println("Table does not exist. Creating table...")
		queryCreateTable := `CREATE TABLE ` + tableName + `(
			time BIGINT NOT NULL,
			price DOUBLE PRECISION,
			volume DOUBLE PRECISION,
			side TEXT,
			type TEXT,
			misc TEXT,
			trade_id BIGINT
			);
			`
		_, err = dbpool.Exec(ctx, queryCreateTable)
		if err != nil {
			log.Printf("Unable to create the '%s' table: %v\n", tableName, err)
			os.Exit(1)
		}
		queryCreateHypertable := `SELECT create_hypertable('` + tableName + `', 'time', chunk_time_interval => 86400000000000);`
		_, err = dbpool.Exec(ctx, queryCreateHypertable)
		if err != nil {
			log.Printf("Unable to upgrade the table '%s' to hypertable: %v\n", tableName, err)
			os.Exit(1)
		}

		infc := GetKrakenTrades(querySince)
		tradesList := infc[Keys(infc)[0]].([]interface{})
		tradesListLength := len(tradesList)
		var trades []Trade
		for _, el := range tradesList {
			var t Trade
			t.Time = int64(el.([]interface{})[2].(float64) * math.Pow(10, 9))
			t.Price, err = strconv.ParseFloat(el.([]interface{})[0].(string), 64)
			if err != nil {
				log.Fatal("Error parsing string to float")
			}
			t.Volume, err = strconv.ParseFloat(el.([]interface{})[1].(string), 64)
			if err != nil {
				log.Fatal("Error parsing string to float")
			}
			t.Side = el.([]interface{})[3].(string)
			t.Type = el.([]interface{})[4].(string)
			t.Misc = el.([]interface{})[5].(string)
			t.TradeID = int64(el.([]interface{})[6].(float64))
			trades = append(trades, t)
		}
		queryInsertData := `
			INSERT INTO ` + tableName + ` (time, price, volume, side, type, misc, trade_id) VALUES ($1, $2, $3, $4, $5, $6, $7);
			`
		batch := &pgx.Batch{}
		for i := range trades {
			t := trades[i]
			batch.Queue(queryInsertData, t.Time, t.Price, t.Volume, t.Side, t.Type, t.Misc, t.TradeID)
		}
		batch.Queue("SELECT count(*) from " + tableName)

		br := dbpool.SendBatch(ctx, batch)
		_, err = br.Exec()
		if err != nil {
			log.Fatalf("Unable to execute statement in batch queue %v\n", err)
		}

		if tradesListLength != 1000 {
			log.Println("Trades database up to date")
		}
		log.Println("Successfully batch inserted data")
		err = br.Close()
		if err != nil {
			log.Fatalf("Unable to close batch %v\n", err)
		}
	} else {
		log.Println("Table already exists. Fetching last entry...")
		var lastTime int64
		dbpool.QueryRow(ctx, "SELECT time FROM xbtusd_kraken_trades order by trade_id desc limit 1;").Scan(&lastTime)
		log.Println(lastTime)
	}
}

// Kraken trades API gives time as a floating point decimal. This function
// returns two ints, one of the integral and one of the fractional part of the
// decimal.
func KrakenSplitDecimal(unixTime float64) (int64, int64) {
	sec := int64(math.Floor(unixTime))
	nsec := int64((unixTime - float64(sec)) * 1000000000)
	return sec, nsec
}

// Returns slice containing all keys of a map[string]interface
func Keys(m map[string]interface{}) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}

func GetKrakenTrades(since int) map[string]interface{} {
	apiQuery := "https://api.kraken.com/0/public/Trades?pair=XBTUSD&since=" + strconv.Itoa(since)
	res, err := http.Get(apiQuery)
	if err != nil {
		log.Fatal("http.Get error | ", err)
	}
	defer res.Body.Close()

	resp := map[string]interface{}{}
	var msg = []byte{}
	msg, err = io.ReadAll(res.Body)
	if err != nil {
		log.Fatal("io.ReadAll error | ", err)
	}

	json.Unmarshal(msg, &resp)
	return resp["result"].(map[string]interface{})
}

// Checks if input_pair (string) is in slice of pairs, returns true if it exists
func PairExists(input_pair string, pairs []string) bool {
	for _, pair := range pairs {
		if pair == input_pair {
			return true
		}
	}
	return false
}

// Prompts user for input (string) for desired pair and returns pair (string)
func GetUserDesiredPair() string {
	fmt.Println("Enter desired pair: ")
	var pair string
	fmt.Scanln(&pair)
	return pair
}

// Requests Kraken REST API for tradeable asset pairs and returns all pairs in
// a slice KrakenPairsList ([]string)
func GetKrakenSpotPairs() []string {
	res, err := http.Get("https://api.kraken.com/0/public/AssetPairs")
	if err != nil {
		log.Fatal("http.Get error | ", err)
	}
	defer res.Body.Close()

	resp := map[string]interface{}{}
	var msg = []byte{}
	msg, err = io.ReadAll(res.Body)
	if err != nil {
		log.Fatal("io.ReadAll error | ", err)
	}

	var KrakenPairsList []string
	json.Unmarshal(msg, &resp)
	for _, el := range resp["result"].(map[string]interface{}) {
		KrakenPairsList = append(KrakenPairsList, el.(map[string]interface{})["altname"].(string))
	}
	sort.Slice(KrakenPairsList, func(i, j int) bool {
		return KrakenPairsList[i] < KrakenPairsList[j]
	})
	// DEBUG
	// log.Println(KrakenPairsList)
	return KrakenPairsList
}
