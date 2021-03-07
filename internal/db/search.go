package db

import (
	"log"
	"strconv"
)

// Recursively check if key and value match in the given
// JSON object
func findv(key, value string, root interface{}) (bool, interface{}) {

	var found bool
	var val interface{}

	if key == "" || root == nil {
		return false, nil
	}
	// determine the json root element/object
	switch obj := root.(type) {
	// JSON child object is a list/array
	case []interface{}:
		for _, o := range obj {
			switch lobj := o.(type) {
			case []interface{}:
				found, val = findv(key, value, lobj)
				if found == true {
					return true, val
				}
			case map[string]interface{}:
				found, val = findv(key, value, lobj)
				if found == true {
					return true, val
				}
			case string:
				if lobj == value {
					return true, lobj
				}
			case int:
				sval := strconv.Itoa(lobj)
				if sval == value {
					return true, sval
				}
			case float64:
				sval := strconv.Itoa(int(lobj))
				if sval == value {
					return true, sval
				}
			}
		}
	case map[string]interface{}:
		v, ok := obj[key]
		if ok {
			switch vv := v.(type) {
			case string:
				if vv == value {
					return true, nil
				}
			case int:
				sval := strconv.Itoa(vv)
				if sval == value {
					return true, nil
				}
			case float64:
				sval := strconv.Itoa(int(vv))
				if sval == value {
					return true, nil
				}
			case []interface{}:
				found, val = findv(key, value, vv)
				if found == true {
					return true, nil
				}
			case map[string]interface{}:
				found, val = findv(key, value, vv)
				if found == true {
					return true, nil
				}
			}
			return false, nil
		}
		for k, v := range obj {
			switch mobj := v.(type) {
			case string:
				if k == key && mobj == value {
					return true, mobj
				}
			case int:
				sval := strconv.Itoa(mobj)
				if k == key && sval == value {
					return true, mobj
				}
			case float64:
				sval := strconv.Itoa(int(mobj))
				if k == key && sval == value {
					return true, mobj
				}
			case []interface{}:
				found, val = findv(key, value, mobj)
				if found == true {
					return true, val
				}
			case map[string]interface{}:
				found, val = findv(key, value, mobj)
				if found == true {
					return true, val
				}
			}
		}
	default:
		log.Println("Invalid JSON hierarchy when searching for", key)
		return false, nil
	}
	return false, nil
}

// Recursively find a key in the json object
func find(key string, root interface{}) (bool, interface{}) {

	var found bool
	var val interface{}

	if key == "" || root == nil {
		return false, nil
	}
	// determine the json root element/object
	switch obj := root.(type) {
	// JSON child object is a list/array
	case []interface{}:
		for _, o := range obj {
			switch lobj := o.(type) {
			// case string here would be the values
			// of a list (if they are strings). That
			// is not a key.
			case []interface{}:
				found, val = find(key, lobj)
				if found == true {
					return true, val
				}
			case map[string]interface{}:
				found, val = find(key, lobj)
				if found == true {
					return true, val
				}
			}
		}
	// JSON child object is a key-value map
	case map[string]interface{}:
		for k, v := range obj {
			if k == key {
				return true, v
			}
			switch mobj := v.(type) {
			case string:
				if key == mobj {
					return true, v
				}
			case []interface{}:
				found, val = find(key, mobj)
				if found == true {
					return true, val
				}
			case map[string]interface{}:
				found, val = find(key, mobj)
				if found == true {
					return true, val
				}
			}
		}
	default:
		log.Println("Invalid JSON hierarchy when searching for", key)
		return false, nil
	}
	return false, nil
}
