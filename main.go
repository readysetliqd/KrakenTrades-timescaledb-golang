package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

type Response struct {
	Error  []any `json:"error"`
	Result struct {
		Pair [][]any `json:*`
		Last string  `json:"last"`
	} `json:"result"`
}

func main() {
	// TO DO:
	// Get list of available pairs
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
	res, err := http.Get("https://api.kraken.com/0/public/Trades?pair=XBTUSD&since=0")
	if err != nil {
		log.Fatal("http.Get error | ", err)
	}
	defer res.Body.Close()

	var resp Response
	var msg = []byte{}
	msg, err = io.ReadAll(res.Body)
	if err != nil {
		log.Fatal("io.ReadAll error | ", err)
	}

	json.Unmarshal(msg, &resp)
	log.Println(resp.Result.Pair)
}
