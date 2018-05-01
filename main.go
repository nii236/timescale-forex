package main

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx"
)

const tickerURL = "http://webrates.truefx.com/rates/connect.html?f=csv"
const dbname = "forex"

func fetch() map[string]*Tick {
	resp, err := http.Get(tickerURL)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	r := csv.NewReader(resp.Body)
	records, err := r.ReadAll()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	results := map[string]*Tick{}
	for _, row := range records {
		pair := row[0]
		timestamp, err := strconv.ParseInt(row[1], 10, 64)
		bigBig, err := strconv.ParseFloat(row[2], 64)
		bigPoints, err := strconv.ParseFloat(row[3], 64)
		offerBig, err := strconv.ParseFloat(row[4], 64)
		offerPoints, err := strconv.ParseFloat(row[5], 64)
		high, err := strconv.ParseFloat(row[6], 64)
		low, err := strconv.ParseFloat(row[7], 64)

		if err != nil {
			fmt.Println("could not parse fields:", err)
			continue
		}

		tick := &Tick{
			Pair:        pair,
			Timestamp:   timestamp,
			BidBig:      bigBig,
			BidPoints:   bigPoints,
			OfferBig:    offerBig,
			OfferPoints: offerPoints,
			High:        high,
			Low:         low,
		}
		results[tick.Pair] = tick
	}

	return results
}

const dropdb = `
DROP DATABASE IF EXISTS forex;
`

const dropschema = `
DROP SCHEMA IF EXISTS public CASCADE;
`

const createdb = `
CREATE DATABASE forex;
`
const createschema = `
CREATE SCHEMA public;
`

const createextensions = `
CREATE EXTENSION IF NOT EXISTS timescaledb;
`

const migrate = `
CREATE TABLE ticks (
	time TIMESTAMP NOT NULL,
	pair VARCHAR(7) NOT NULL,
	bidBig DOUBLE PRECISION NOT NULL,
	bidPoints DOUBLE PRECISION NOT NULL,
	offerBig DOUBLE PRECISION NOT NULL,
	offerPoints DOUBLE PRECISION NOT NULL,
	high DOUBLE PRECISION NOT NULL,
	low DOUBLE PRECISION NOT NULL
  );

SELECT create_hypertable('ticks', 'time', 'pair');
`

const insert = `
INSERT INTO ticks (
	time,
	pair,
	bidBig,
	bidPoints,
	offerBig,
	offerPoints,
	high,
	low
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
`

func save(c *pgx.ConnPool, tick *Tick) error {
	_, err := c.Exec(insert, time.Unix(tick.Timestamp/1000.0, 0), tick.Pair, tick.BidBig, tick.BidPoints, tick.OfferBig, tick.OfferPoints, tick.High, tick.Low)
	if err != nil {
		return err
	}
	return nil
}

func prepDB() {

	connConfig := &pgx.ConnConfig{
		Database:  "dev",
		Host:      "localhost",
		Port:      5432,
		User:      "dev",
		Password:  "dev",
		TLSConfig: nil,
	}

	connPool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig:     *connConfig,
		AfterConnect:   nil,
		MaxConnections: 20,
		AcquireTimeout: 30 * time.Second,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	_, err = connPool.Exec(dropdb)
	if err != nil {
		fmt.Println("dropdb:", err)
		os.Exit(1)
	}

	_, err = connPool.Exec(createdb)
	if err != nil {
		fmt.Println("createdb:", err)
		os.Exit(1)
	}

}
func main() {
	ticker := time.NewTicker(1 * time.Second)
	prepDB()
	connConfig := &pgx.ConnConfig{
		Database:  "forex",
		Host:      "localhost",
		Port:      5432,
		User:      "dev",
		Password:  "dev",
		TLSConfig: nil,
	}

	connPool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig:     *connConfig,
		AfterConnect:   nil,
		MaxConnections: 20,
		AcquireTimeout: 30 * time.Second,
	})
	_, err = connPool.Exec(dropschema)
	if err != nil {
		fmt.Println("dropschema:", err)
		os.Exit(1)
	}
	_, err = connPool.Exec(createschema)
	if err != nil {
		fmt.Println("createschema:", err)
		os.Exit(1)
	}
	_, err = connPool.Exec(createextensions)
	if err != nil {
		fmt.Println("createextensions:", err)
		os.Exit(1)
	}
	_, err = connPool.Exec(migrate)
	if err != nil {
		fmt.Println("migrate:", err)
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for {
		select {
		case <-ticker.C:
			ticks := fetch()
			for _, tick := range ticks {
				err := save(connPool, tick)
				if err != nil {
					fmt.Println(err)
					continue
				}
			}
			fmt.Println("Successfully written ticks")
		}
	}
}

// Tick is a single record for a pair
type Tick struct {
	Pair        string
	Timestamp   int64
	BidBig      float64
	BidPoints   float64
	OfferBig    float64
	OfferPoints float64
	High        float64
	Low         float64
}
