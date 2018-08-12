package django

import (
	"fmt"
	"math"
	"regexp"
	"time"

	sqlite "github.com/mattn/go-sqlite3"
)

// extract the specified member (param: lookup_type) from Time (param: dt)
func _sqlite_datetime_extract(lookup_type string, dt time.Time) int {
	switch lookup_type {
	case "week_day":
		return int(dt.Weekday())
	case "week":
		_, wk := dt.ISOWeek()
		return wk
	case "quarter":
		return int(math.Ceil(float64(dt.Month()) / 3.0))
	case "day":
		return dt.Day()
	case "hour":
		return dt.Hour()
	case "iszero":
		if dt.IsZero() {
			return 0
		} else {
			return 1
		}
	case "minute":
		return dt.Minute()
	case "month":
		return int(dt.Month())
	case "nanosecond":
		return dt.Nanosecond()
	case "second":
		return dt.Second()
	case "year":
		return dt.Year()
	case "yearday":
		return dt.YearDay()
	}
	// TODO(sum12): should panic instead ?
	return -1
}

// trucate Time (param: dt) to the given granularity (param: lookup_type)
func _sqlite_datetime_trunc(lookup_type string, dt time.Time) string {
	switch lookup_type {
	case "year":
		return fmt.Sprintf("%d-01-01", dt.Year())
	case "quarter":
		month := int(dt.Month())
		month_quarter := month - (month-1)%3
		return fmt.Sprintf("%d-%02d-01", dt.Year(), month_quarter)
	case "month":
		return fmt.Sprintf("%d-%02d-01", dt.Year(), dt.Month())
	case "day":
		return fmt.Sprintf("%d-%02d-%02d", dt.Year(), dt.Month(), dt.Day())
	case "hour":
		return fmt.Sprintf("%02d:00:00", dt.Hour())
	case "minute":
		return fmt.Sprintf("%02d:%02d:00", dt.Hour(), dt.Minute())
	case "second":
		return fmt.Sprintf("%02d:%02d:%02d", dt.Hour(), dt.Minute(), dt.Second())
	default:
		//TODO(sum12): panic ?
		return ""
	}
}

// returns date part of date-time object
func _sqlite_datetime_cast_date(dt time.Time, tzname string) string {
	tz, err := time.LoadLocation(tzname)
	if err != nil {
		return ""
	}
	ndt := dt.In(tz)
	return ndt.Format("2006-01-02")
}

// returns time part of date-time object
func _sqlite_datetime_cast_time(dt time.Time, tzname string) string {
	tz, err := time.LoadLocation(tzname)
	if err != nil {
		return ""
	}
	ndt := dt.In(tz)
	return ndt.Format("15:04:05")
}

// compute diff between times and returns time in millisec
func _sqlite_time_diff(lhs, rhs time.Time) int {
	return (lhs.Second()*1000000 - rhs.Second()*1000000)
}

// does regex(param: re) on string(param: s)
func _sqlite_regexp(re, s string) (bool, error) {
	return regexp.MatchString(re, s)
}

// Computes x^y
func _sqlite_pow(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}

// ConnectHook can be passed as a param (hook) to
// mattn/go-sqlite3 connecthook-param while creating a
// new connection, making these these django-sqlite
// based function available to sqlite
func ConnectHook(conn *sqlite.SQLiteConn) error {
	funcmap := make(map[string]interface{})
	funcmap["django_date_extract"] = _sqlite_datetime_extract
	funcmap["django_date_trunc"] = _sqlite_datetime_trunc
	funcmap["django_datetime_cast_date"] = _sqlite_datetime_cast_date
	funcmap["django_datetime_cast_time"] = _sqlite_datetime_cast_time
	funcmap["django_datetime_extract"] = _sqlite_datetime_extract
	funcmap["django_datetime_trunc"] = _sqlite_datetime_trunc
	funcmap["django_time_extract"] = _sqlite_datetime_extract
	funcmap["django_time_trunc"] = _sqlite_datetime_trunc
	funcmap["django_time_diff"] = _sqlite_time_diff
	funcmap["regexp"] = _sqlite_regexp
	funcmap["django_power"] = _sqlite_pow

	for k, v := range funcmap {
		if err := conn.RegisterFunc(k, v, true); err != nil {
			return err
		}
	}

	return nil
}
