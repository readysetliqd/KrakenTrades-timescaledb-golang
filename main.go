package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
)

func main() {
	// TO DO:
	// Ask user for desired pair
	// Check pair is in list
	// Connect to timescaledb
	// If table for pair !exists
	// Get first data for pair since=0
	// loop until last=current_time
	// If table for pair exists
	// Get pair since=last_entry
	// loop until last=current_time
	// Write data into table
	KrakenSpotPairs := GetKrakenSpotPairs()
	var DesiredPair string
	// TO DO:
	// Allow user to enter q for quit or ? for list of pairs
	// Keep prompting user for inputs until pair is found in list of tradeable assets
	for true {
		DesiredPair = strings.ToUpper(GetUserDesiredPair())
		if PairExists(DesiredPair, KrakenSpotPairs) {
			break
		} else {
			fmt.Println("Pair not found, try checking spelling or entering another pair.")
		}
	}
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
