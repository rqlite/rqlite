package django

import (
	"fmt"
	"math"
	"regexp"
	"time"

	sqlite "github.com/mattn/go-sqlite3"
)

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
	/*case "loc":
	  return dt.Location()*/
	case "minute":
		return dt.Minute()
	case "month":
		return int(dt.Month())
	case "nanosecond":
		return dt.Nanosecond()
	case "second":
		return dt.Second()
	/*case "unix":
	return dt.Unix()*/
	/*case "unixnano":
	return dt.UnixNano()*/
	case "year":
		return dt.Year()
	case "yearday":
		return dt.YearDay()
	}
	// TODO(sum12): should I panic instead ?
	return -1
}

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

func _sqlite_datetime_cast_date(dt time.Time, tzname string) string {
	tz, err := time.LoadLocation(tzname)
	if err != nil {
		return ""
	}
	ndt := dt.In(tz)
	return ndt.Format("2006-01-02")
}

func _sqlite_datetime_cast_time(dt time.Time, tzname string) string {
	tz, err := time.LoadLocation(tzname)
	if err != nil {
		return ""
	}
	ndt := dt.In(tz)
	return ndt.Format("15:04:05")
}

func _sqlite_time_diff(lhs, rhs time.Time) int {
	return (lhs.Second()*1000000 - rhs.Second()*1000000)
}

/*
def _sqlite_format_dtdelta(conn, lhs, rhs):
    """
    LHS and RHS can be either:
    - An integer number of microseconds
    - A string representing a timedelta object
    - A string representing a datetime
    """
    try:
        if isinstance(lhs, int):
            lhs = str(decimal.Decimal(lhs) / decimal.Decimal(1000000))
        real_lhs = parse_duration(lhs)
        if real_lhs is None:
            real_lhs = backend_utils.typecast_timestamp(lhs)
        if isinstance(rhs, int):
            rhs = str(decimal.Decimal(rhs) / decimal.Decimal(1000000))
        real_rhs = parse_duration(rhs)
        if real_rhs is None:
            real_rhs = backend_utils.typecast_timestamp(rhs)
        if conn.strip() == '+':
            out = real_lhs + real_rhs
        else:
            out = real_lhs - real_rhs
    except (ValueError, TypeError):
        return None
    # typecast_timestamp returns a date or a datetime without timezone.
    # It will be formatted as "%Y-%m-%d" or "%Y-%m-%d %H:%M:%S[.%f]"
    return str(out)
*/

func _sqlite_regexp(re, s string) (bool, error) {
	return regexp.MatchString(re, s)
}

// Computes x^y
func _sqlite_pow(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}

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
	//funcmap["django_timestamp_diff"] = _sqlite_timestamp_diff
	//funcmap["django_format_dtdelta", 3, _sqlite_format_dtdelta)

	for k, v := range funcmap {
		if err := conn.RegisterFunc(k, v, true); err != nil {
			return err
		}
	}

	return nil
}

/*
func main() {
	sql.Register("sqlite3_custom", &sqlite.SQLiteDriver{
		ConnectHook: func(conn *sqlite.SQLiteConn) error {

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
			//funcmap["django_timestamp_diff"] = _sqlite_timestamp_diff
			//funcmap["django_format_dtdelta", 3, _sqlite_format_dtdelta)

			for k, v := range funcmap {
				if err := conn.RegisterFunc(k, v, true); err != nil {
					return err
				}
			}

			return nil
		},
	})

	db, err := sql.Open("sqlite3_custom", ":memory:")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	_, err = db.Exec("create table foo (department integer, created datetime)")
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	t := time.Now().Format("2006-01-02T15:04:05")
	//t := time.Now().Format("2006-01-02")

	fmt.Printf("insert into foo values (1, %s)\n", t)
	_, err = db.Exec(fmt.Sprintf("insert into foo values (%d, '%s')", i, t))
	if err != nil {
		log.Fatal("Failed to insert records:", err)
	}

	fmt.Printf("inserted\n")
	rows, err := db.Query("select department, django_datetime_extract('year', created) from foo")
	if err != nil {
		log.Fatal("STDDEV query error:", err)
	}
	defer rows.Close()
	for rows.Next() {
		var dept int64
		//var dev time.Time
		var yr int
		if err := rows.Scan(&dept, &yr); err != nil {
			log.Fatal(err)
		}
		//fmt.Printf("dept=%d stddev=%s\n", dept, dev.Format("15:04:05 fuck this 2006-01-02"))

		fmt.Printf("dept=%d stddev=%d\n", dept, yr)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
*/
