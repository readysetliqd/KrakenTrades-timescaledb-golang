package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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

// From the Kraken API docs: Every REST API user has a "call counter" which
// starts at 0. Ledger/trade history calls increase the counter by 2. All other
// API calls increase this counter by 1 (except AddOrder, CancelOrder which
// operate on a different limiter detailed further below).
// Tier: Starter 	  | Max Counter: 15 | API Counter Decay: -0.33/sec
// Tier: Intermediate | Max Counter: 20 | API Counter Decay: -0.5/sec
// Tier: Pro		  | Max Counter: 20 | API Counter Decay: -1/sec
// This program calls for trade history but it is unclear whether the API docs
// are referring to private trade history or public market data trade history
const tradesRateLimit = 3000000000 // 1 / 0.33 * 1,000,000 (# nanosec in 1 sec)

func main() {
	err := godotenv.Load("env/psqllogin.env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	KrakenSpotPairs, KrakenSpotAltPairs := GetKrakenSpotPairs()
	var desiredPair string
	// validate DBNAME in .env file is a tradeable pair on kraken
	desiredPair = strings.ToUpper(os.Getenv("DBNAME"))
	dbName := strings.ToLower(desiredPair)
	if PairExists(desiredPair, KrakenSpotPairs) != -1 {
		log.Printf("Found pair")
	} else if i := PairExists(desiredPair, KrakenSpotAltPairs); i != -1 {
		desiredPair = KrakenSpotPairs[i]
		log.Printf("Altname for pair entered. Changing desired pair to %s", desiredPair)
	} else {
		fmt.Println("Pair not found, try checking spelling or entering another pair.")
	}

	// connect to postgres db
	// prerequisite to create database of same name as entered into psqllogin.env
	// outside of this program before running
	ctx := context.Background()
	connStr := "postgres://" + os.Getenv("USER") + ":" + os.Getenv("PASS") + "@" + os.Getenv("HOST") + ":" + os.Getenv("PORT") + "/" + dbName
	dbpool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Printf("Unable to connect to database: %v\n", os.Stderr)
		os.Exit(1)
	}
	defer dbpool.Close()

	// check if table exists with dbname
	var tableExists bool
	tableName := dbName + "_kraken_trades"
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

	var querySince int64
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
		queryUpgradeDbToTimescale := "CREATE EXTENSION IF NOT EXISTS timescaledb;"
		_, err = dbpool.Exec(ctx, queryUpgradeDbToTimescale)
		if err != nil {
			log.Printf("Unable to create timescaledb extension '%s': %v\n", dbName, err)
			os.Exit(1)
		}
		queryCreateHypertable := `SELECT create_hypertable('` + tableName + `', 'time', chunk_time_interval => 86400000000000);`
		_, err = dbpool.Exec(ctx, queryCreateHypertable)
		if err != nil {
			log.Printf("Unable to upgrade the table '%s' to hypertable: %v\n", tableName, err)
			os.Exit(1)
		}

		// set querySince to 0 for new table and begin loop to insert data
		querySince = 0
		estCompletionTime(0, desiredPair)
		for {
			tradesInfc := GetKrakenTrades(querySince, desiredPair)
			tradesList := tradesInfc[desiredPair].([]interface{})
			tradesListLength := len(tradesList)
			insertTradesToDb(tradesList, ctx, dbpool, tableName)
			if tradesListLength != 1000 {
				log.Println("Trades database is up to date")
				break
			}
			querySince, err = strconv.ParseInt(tradesInfc["last"].(string), 0, 64)
			querySince = querySince + 1
			if err != nil {
				log.Fatal("Error parsing int64 from string")
			}
			time.Sleep(tradesRateLimit)
		}
	} else { // update database if hypertable exists
		log.Println("Table already exists. Fetching last entry...")
		// set querySince to last entry in database and begin loop to insert data
		dbpool.QueryRow(ctx, "SELECT time FROM "+dbName+"_kraken_trades order by trade_id desc limit 1;").Scan(&querySince)
		var lastTradeId int64
		dbpool.QueryRow(ctx, "SELECT trade_id FROM "+dbName+"_kraken_trades ORDER BY trade_id desc limit 1;").Scan(&lastTradeId)
		estCompletionTime(lastTradeId, desiredPair)
		for {
			tradesInfc := GetKrakenTrades(querySince, desiredPair)
			tradesList := tradesInfc[desiredPair].([]interface{})
			tradesListLength := len(tradesList)
			insertTradesToDb(tradesList, ctx, dbpool, tableName)
			if tradesListLength != 1000 {
				log.Println("Trades database is up to date")
				break
			}
			querySince, err = strconv.ParseInt(tradesInfc["last"].(string), 0, 64)
			querySince = querySince + 1
			if err != nil {
				log.Fatal("Error parsing int64 from string")
			}
			time.Sleep(tradesRateLimit)
		}
	}
}

func estCompletionTime(last int64, pair string) {
	apiQuery := "https://api.kraken.com/0/public/Trades?pair=" + pair + "&count=1"
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
	tradesLeft := int64((resp["result"].(map[string]interface{})[pair].([]interface{})[0].([]interface{})[6].(float64))) - last
	totalSec := tradesLeft * tradesRateLimit / 1000000000 / 1000 // convert from nanoseconds to seconds and 1000 entries per API response
	estDay := totalSec / 86400
	estHr := (totalSec - estDay*86400) / 3600
	estMin := (totalSec - estDay*86400 - estHr*3600) / 60
	fmt.Printf("Estimated time to complete with current rate limit is %v days %v hours %v minutes\n", estDay, estHr, estMin)
	fmt.Print("Press 'Enter' to continue...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}

func insertTradesToDb(tradesList []interface{}, ctx context.Context, dbpool *pgxpool.Pool, tableName string) {
	var trades []Trade
	var err error
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

	log.Println("Successfully batch inserted data")
	err = br.Close()
	if err != nil {
		log.Fatalf("Unable to close batch %v\n", err)
	}
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

func GetKrakenTrades(since int64, pair string) map[string]interface{} {
	apiQuery := "https://api.kraken.com/0/public/Trades?pair=" + pair + "&since=" + strconv.Itoa(int(since))
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

// Checks if input_pair (string) is in slice of pairs, returns -1 if it doesn't
// exist otherwise returns index where pair was found
func PairExists(input_pair string, pairs []string) int {
	for i, pair := range pairs {
		if pair == input_pair {
			return i
		}
	}
	return -1
}

// Polls Kraken REST API for tradeable asset pairs and returns two slices ([]string)
// of their pair names and altnames
func GetKrakenSpotPairs() ([]string, []string) {
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
	var KrakenPairsAltList []string
	json.Unmarshal(msg, &resp)
	for k, el := range resp["result"].(map[string]interface{}) {
		KrakenPairsList = append(KrakenPairsList, k)
		KrakenPairsAltList = append(KrakenPairsAltList, el.(map[string]interface{})["altname"].(string))
	}
	// DEBUG
	// sort.Slice(KrakenPairsAltList, func(i, j int) bool {
	// 	return KrakenPairsAltList[i] < KrakenPairsAltList[j]
	// })
	// log.Println(KrakenPairsList)
	return KrakenPairsList, KrakenPairsAltList
}
