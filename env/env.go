package env

import (
	"os"
	"strconv"
)

// Override takes a variable and overrides its value with that of the
// environment variable passed in. If the environment variable is set
// to the empty string the variable is left unchanged.
func Override(v interface{}, env string) error {
	e := os.Getenv(env)
	if e == "" {
		return nil
	}

	switch t := v.(type) {
	case *string:
		*t = e
	case *uint64:
		i, err := strconv.ParseUint(e, 10, 64)
		if err != nil {
			return nil
		}
		*t = i
	case *bool:
		b, err := strconv.ParseBool(e)
		if err != nil {
			return nil
		}
		*t = b
	}

	return nil
}
