package db

import (
	"errors"
	"log"
	"strconv"
)

var (
	ErrInvalidDatabase      = errors.New("unknown database name")
	ErrIndexNotFound        = errors.New("index not found")
	ErrUninitializedDB      = errors.New("uninitialized database")
	ErrKeyValueNotFound     = errors.New("search key with value not found")
	ErrKeyNotFound          = errors.New("search key not found")
	ErrUnsupportedIndexType = errors.New("cannot index on type")
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

// Perform a search on the entire JSON object and look for the key with the
// corresponding value. The search is not indexed. If the key and value
// match one or more results are returned in IndexBackend.resultSet. If
// no values are found then an error is returned.
//
func (jdb *JsonDB) search(dbname, key, value string) (*IndexBackend, error) {

	var result IndexBackend
	var found bool

	jsonType, ok := jdb.dbMap[dbname]
	if !ok {
		return nil, ErrInvalidDatabase
	}

	toResult := func(valueFound string, enclObj interface{}) {
		result.resultSet = append(result.resultSet, enclObj)
	}

	result.resultSet = make([]interface{}, 0)
	if jsonType.list != nil {
		for _, lobj := range jsonType.list {
			found, _ = findv(key, value, lobj)
			if found {
				toResult(value, lobj)
			}
		}
		if len(result.resultSet) == 0 {
			return nil, ErrKeyValueNotFound
		}
		return &result, nil
	}

	if jsonType.dict != nil {
		v, ok := jsonType.dict[key]
		if ok {
			saved := true
			switch indexVal := v.(type) {
			case int:
				sval := strconv.Itoa(indexVal)
				toResult(sval, v)
			case float64:
				sval := strconv.Itoa(int(indexVal))
				toResult(sval, v)
			case string:
				toResult(indexVal, v)
			default:
				saved = false
			}
			if saved {
				return &result, nil
			}
			return nil, ErrKeyValueNotFound
		}
		for _, v := range jsonType.dict {
			found, _ = findv(key, value, v)
			if found == true {
				toResult(value, v)
			}
		}
		if len(result.resultSet) == 0 {
			return nil, ErrKeyValueNotFound
		}
		return &result, nil
	}
	return nil, ErrKeyValueNotFound
}

func (jdb *JsonDB) searchIndex(dbname, key, value string) (interface{}, error) {

	index, err := jdb.getIndex(dbname, key)
	if err != nil {
		return nil, err
	}
	result, ok := index.stringIndex[value]
	if !ok {
		return nil, ErrKeyValueNotFound
	}
	return result, nil
}
