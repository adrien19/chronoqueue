package repository

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// ScanToStruct takes a map[string]string result from a redis.HSet result
// and scans the keys and values from the result into a given struct by its
// field tags.
func ScanToStruct(vals map[string]string, obj interface{}, fieldTag string) error {
	if len(vals)%2 != 0 {
		return errors.New("args should have an even number of items (key-val)")
	}

	ob := reflect.ValueOf(obj)
	if ob.Kind() == reflect.Ptr {
		ob = ob.Elem()
	}

	if ob.Kind() != reflect.Struct {
		return fmt.Errorf("failed to decode form values to struct, received non struct type: %T", ob)
	}

	for key, val := range vals {
		// Get the field from the struct with the matching key.
		f := getField(ob, key, fieldTag)
		if !f.IsValid() {
			continue
		}
		// Convert the string value from Redis and set it on the struct field.
		if _, err := setVal(f, val); err != nil {
			return fmt.Errorf("failed to decode `%v`, got: `%s` (%v)", key, val, err)
		}
	}
	return nil
}

func getField(ob reflect.Value, name, fieldTag string) reflect.Value {
	for i := 0; i < ob.NumField(); i++ {
		f := ob.Field(i)
		if f.IsValid() && f.CanSet() {
			tag := ob.Type().Field(i).Tag.Get(fieldTag)
			if tag == "" || tag == "-" {
				continue
			}

			tag = strings.Split(tag, ",")[0]
			if tag == name {
				return f
			}
		}
	}

	return reflect.Value{}
}

func setVal(f reflect.Value, val string) (bool, error) {
	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return false, fmt.Errorf("expected int")
		}
		f.SetInt(v)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := strconv.ParseUint(val, 10, 0)
		if err != nil {
			return false, fmt.Errorf("expected unsigned int")
		}
		f.SetUint(v)

	case reflect.Float32, reflect.Float64:
		v, err := strconv.ParseFloat(val, 0)
		if err != nil {
			return false, fmt.Errorf("expected decimal")
		}
		f.SetFloat(v)

	case reflect.String:
		f.SetString(val)

	case reflect.Slice:
		// []byte slice ([]uint8).
		if f.Type().Elem().Kind() == reflect.Uint8 {
			f.SetBytes([]byte(val))
		}

	case reflect.Bool:
		b, err := strconv.ParseBool(val)
		if err != nil {
			return false, fmt.Errorf("expected boolean")
		}
		f.SetBool(b)

	case reflect.Map:
		mapStr := map[string]string{}
		if err := json.Unmarshal([]byte(val), &mapStr); err != nil {
			fmt.Println("Error: Failed to deserialize Map from redis reply:", val)
		}
		fmt.Println("Error: Received unsupported type Map from redis reply:", mapStr)
	default:
		return false, nil
	}

	return true, nil
}
