# rqbench
A simple benchmarking tool for rqlite.

## Build
```sh
go build -o rqbench
```

## Usage

```sh
$ rqbench -h

rqbench is a simple load testing utility for rqlite.

Usage: rqbench [arguments] <SQL statement>
  -a string
        Node address (default "localhost:4001")
  -b int
        Statements per request (default 1)
  -m int
        Print progress every m requests
  -n int
        Number of requests (default 100)
  -p string
        Endpoint to use (default "/db/execute")
  -t string
        Transport to use (default "http")
  -x    Use explicit transaction per request
 ```

 ## Examples

```sh
$ rqbench -b 10 -m 100 -n 1 -o 'CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)' 'INSERT INTO foo(name) VALUES("fiona")'
Test target: http://localhost:4001/db/execute
Total duration: 10.094572ms
Requests/sec: 99.06
Statements/sec: 990.63
$ rqbench -b 10 -m 10 -n 100 'INSERT INTO foo(name) VALUES("fiona")' -a localhost:4001
Test target: http://localhost:4001/db/execute
10 requests completed in 16.130142ms
20 requests completed in 12.778837ms
30 requests completed in 10.403782ms
40 requests completed in 9.938703ms
50 requests completed in 23.39066ms
60 requests completed in 10.311711ms
70 requests completed in 16.628355ms
80 requests completed in 20.589929ms
90 requests completed in 10.687567ms
Total duration: 1.48811818s
Requests/sec: 67.20
Statements/sec: 671.99
$ rqbench -p '/db/query?level=none' -b 2 -m 1 -n 5 "SELECT COUNT(*) FROM foo" -a localhost:4001
Test target: http://localhost:4001/db/query?level=none
1 requests completed in 484.861µs
2 requests completed in 391.401µs
3 requests completed in 254.429µs
4 requests completed in 439.457µs
Total duration: 15.16971ms
Requests/sec: 329.60
Statements/sec: 659.21

```