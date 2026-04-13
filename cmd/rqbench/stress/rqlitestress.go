package main

import (
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"time"
)

const printQueries = false
const pretty = true
const loadTimeout = 60 * time.Second
const address = "localhost:4001"

const scheme = `[["CREATE TABLE IF NOT EXISTS mappings (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)"]]`

var client = &http.Client{Timeout: loadTimeout}

func sendRequest(address string, endpoint string, transaction bool, reqBody string) ([]byte, error) {
	if printQueries {
		fmt.Printf("rqlite: request is: %v\n", reqBody)
	}
	query := fmt.Sprintf("/db/%s?timings", endpoint)
	if pretty {
		query = query + "&pretty"
	}
	if transaction {
		query = query + "&transaction"
	}
	URL := "http://" + string(address) + query
	req, err := http.NewRequest("POST", URL, strings.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("metarqlite: bad http status code: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("rqlite: response error is: %v\n", err)
		return nil, err
	}
	if printQueries {
		fmt.Printf("rqlite: response is: %v\n", string(respBody))
	}
	return respBody, nil
}

func createSchema() error {
	_, err := sendRequest(address, "request", true, scheme)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

func createRandomMappings(count int) ([]byte, error) {
	str := strings.Builder{}
	str.WriteString(`[["INSERT INTO mappings (name) values `)
	for i := range count {
		str.WriteString(fmt.Sprintf(`('randomlyInsertedString%d')`, rand.Uint64()))
		if i != count-1 {
			str.WriteString(",")
		}
	}
	str.WriteString(`"]]`)
	reqBody := str.String()

	respBody, err := sendRequest(address, "request", true, reqBody)
	return respBody, err
}

func main() {
	if err := runMain(); err != nil {
		fmt.Printf("main: error: %v\n", err)
	}
}

func runMain() error {
	var total int
	const batchSize = 1000
	const iterateCount = 100
	const latencyCount = 50
	if err := createSchema(); err != nil {
		return fmt.Errorf("failed to create schema: %w\n", err)
	}
	fmt.Printf("will insert %d rows in batches of %d\n", batchSize*iterateCount*latencyCount, batchSize)

	go func() { // we must have readers to take RLock for measurable periods.
		for {
			time.Sleep(10 * time.Second)
			_, err := sendRequest(address, "request", true,
				`[["SELECT sum(8+length(name)) FROM mappings"]]`)
			if err != nil {
				fmt.Printf("select failed: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("S")
		}
	}()

	for range iterateCount {
		var maxTime, sumTime time.Duration
		for range latencyCount {
			now := time.Now()
			if _, err := createRandomMappings(batchSize); err != nil {
				return err
			}
			dur := time.Since(now)
			maxTime = max(maxTime, dur)
			sumTime += dur
			total += batchSize
			fmt.Printf(".")
		}
		mark := " "
		if maxTime > time.Second {
			mark = "!"
		}
		fmt.Printf(" inserted=%d %s time max=%v avg=%v\n", total, mark, maxTime, time.Duration(float64(time.Second)*(sumTime.Seconds()/latencyCount)))
	}
	fmt.Printf("inserted %d\n", total)
	return nil

}
