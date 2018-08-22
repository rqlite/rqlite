package django

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestExtract(t *testing.T) {
	ct, _ := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Mon Jan 2 15:04:05 -0700 MST 2006")
	results := map[string]int{
		"week_day":   1,
		"week":       1,
		"quarter":    1,
		"day":        2,
		"hour":       15,
		"iszero":     1,
		"minute":     4,
		"month":      1,
		"nanosecond": 0,
		"second":     5,
		"year":       2006,
		"yearday":    2,
	}
	for l, r := range results {
		if rr := _sqlite_datetime_extract(l, ct); rr != r {
			s := fmt.Sprintf("%s For %s is expected to be %d but instead got %d", l, ct, r, rr)
			log.Fatal(s)
		}
	}
}

func TestTrunc(t *testing.T) {
	ct, _ := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Mon Jan 2 15:04:05 -0700 MST 2006")
	results := map[string]string{
		"month":   "2006-01-01",
		"day":     "2006-01-02",
		"hour":    "15:00:00",
		"minute":  "15:04:00",
		"second":  "15:04:05",
		"year":    "2006-01-01",
		"quarter": "2006-01-01",
	}
	for l, r := range results {
		if rr := _sqlite_datetime_trunc(l, ct); rr != r {
			s := fmt.Sprintf("%s For %s is expected to be %s but instead got %s", l, ct, r, rr)
			log.Fatal(s)
		}
	}
}

func TestCastDate(t *testing.T) {
	ct, _ := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Mon Jan 2 15:04:05 -0700 MST 2006")
	rr := "2006-01-02"
	if r := _sqlite_datetime_cast_date(ct, "UTC"); rr != r {
		s := fmt.Sprintf("date for %s is expected to be %s, but instead got %s", ct, rr, r)
		log.Fatal(s)
	}

}
func TestCastTime(t *testing.T) {
	ct, _ := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Mon Jan 2 15:04:05 -0700 MST 2006")
	rr := "22:04:05"
	if r := _sqlite_datetime_cast_time(ct, "UTC"); rr != r {
		s := fmt.Sprintf("date for %s is expected to be %s, but instead got %s", ct, rr, r)
		log.Fatal(s)
	}
}
