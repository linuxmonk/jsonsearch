package db

import (
	"errors"
	"fmt"
	"log"
	"strconv"
)

var (
	ErrInvalidDatabase  = errors.New("unknown database name")
	ErrIndexNotFound    = errors.New("index not found")
	ErrUninitializedDB  = errors.New("uninitialized database")
	ErrKeyValueNotFound = errors.New("search key with value not found")
	ErrKeyNotFound      = errors.New("search key not found")
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
			}
		}
	// JSON child object is a key-value map
	case map[string]interface{}:
		v, ok := obj[key]
		if !ok {
			return false, nil
		}
		switch mobj := v.(type) {
		case string:
			if mobj == value {
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

func (jdb *JsonDB) kquery(dbname, key string) (*IndexBackend, error) {
	return jdb.kvquery(dbname, key, "", false, true)
}

// Check if the list contains values that are of basic data types. If
// so compare the value sent as arg with the basic values. If they are
// equal return true else return false.
func valueInList(value string, l []interface{}) bool {
	if l == nil || len(l) == 0 {
		return false
	}
	for _, v := range l {
		switch vv := v.(type) {
		case string:
			if vv == value {
				return true
			}
		case int:
			sval := strconv.Itoa(vv)
			if sval == value {
				return true
			}
		case float64:
			sval := strconv.Itoa(int(vv))
			if sval == value {
				return true
			}
		}
	}
	return false
}

// Perform key only or key value based lookup/search from the root/top of the JSON
// object specified by "dbname" argument.
func (jdb *JsonDB) kvquery(dbname, key, value string, valueCheck, indexing bool) (*IndexBackend, error) {

	var result IndexBackend
	var val interface{}
	var found bool

	jsonType, ok := jdb.dbMap[dbname]
	if !ok {
		return nil, ErrInvalidDatabase
	}

	result.stringIndex = make(map[string]interface{})
	result.resultSet = make([]interface{}, 0)

	toResult := func(valueFound string, enclObj interface{}) {
		if !valueCheck {
			if indexing {
				result.stringIndex[valueFound] = enclObj
			} else {
				result.resultSet = append(result.resultSet, enclObj)
			}
			return
		}
		if valueCheck && valueFound == value {
			if indexing {
				result.stringIndex[valueFound] = enclObj
			} else {
				result.resultSet = append(result.resultSet, enclObj)
			}
		}
	}

	if jsonType.list != nil {
		for _, lobj := range jsonType.list {
			found, val = find(key, lobj)
			if found {
				switch indexVal := val.(type) {
				case int:
					sval := strconv.Itoa(indexVal)
					toResult(sval, lobj)
				case float64:
					sval := strconv.Itoa(int(indexVal))
					toResult(sval, lobj)
				case string:
					toResult(indexVal, lobj)
				case []interface{}:
					if valueCheck {
						found = valueInList(value, indexVal)
						if found {
							toResult(value, lobj)
						}
					}
				case map[string]interface{}:
					if valueCheck {
						found, _ = findv(key, value, indexVal)
						if found {
							toResult(value, indexVal)
						}
					} else {
						found, val = find(key, indexVal)
						if found {
							toResult(value, indexVal)
						}
					}
				}
			}
		}
		if indexing && len(result.stringIndex) == 0 {
			return nil, ErrKeyNotFound
		}
		if !indexing && len(result.resultSet) == 0 {
			return nil, ErrKeyValueNotFound
		}
		return &result, nil
	}

	if jsonType.dict != nil {
		// This is a loop here and not a direct dict[k] access
		// because the top level values could have more complex
		// types where keys and values could be present. So
		// need to iterate all values
		for k, v := range jsonType.dict {
			if k == key {
				switch indexVal := v.(type) {
				case int:
					sval := strconv.Itoa(indexVal)
					toResult(sval, v)
				case float64:
					sval := strconv.Itoa(int(indexVal))
					toResult(sval, v)
				case string:
					toResult(indexVal, v)
				case []interface{}:
					if valueCheck {
						found = valueInList(value, indexVal)
						if found {
							toResult(value, v)
						}
					}
				case map[string]interface{}:
					if valueCheck {
						found, _ = findv(key, value, indexVal)
						if found {
							toResult(value, v)
						}
					} else {
						found, val = find(key, v)
						if found {
							toResult(value, v)
						}
					}
					// cannot index on values that are not of these basic
					// searchable types
				}
				continue
			}

			switch mobj := v.(type) {
			case []interface{}:
				found, val = find(key, mobj)
				if found == true {
					switch indexVal := v.(type) {
					case int:
						sval := strconv.Itoa(indexVal)
						toResult(sval, v)
					case float64:
						sval := strconv.Itoa(int(indexVal))
						toResult(sval, v)
					case string:
						result.stringIndex[indexVal] = v
						toResult(indexVal, v)
						// cannot index on values that are not of these basic
						// searchable types
					}
				}
			case map[string]interface{}:
				found, val = find(key, mobj)
				if found == true {
					switch indexVal := v.(type) {
					case int:
						sval := strconv.Itoa(indexVal)
						toResult(sval, v)
					case float64:
						sval := strconv.Itoa(int(indexVal))
						toResult(sval, v)
					case string:
						toResult(indexVal, v)
						// cannot index on values that are not of these basic
						// searchable types
					}
				}
			}
		}
		if indexing && len(result.stringIndex) == 0 {
			return nil, ErrKeyNotFound
		}
		if !indexing && len(result.resultSet) == 0 {
			return nil, ErrKeyValueNotFound
		}
		return &result, nil
	}
	return nil, ErrKeyNotFound
}

// query the top/first level objects for keys. A non recursive implementation.
// this method does not search for keys within objects of objects where the depth
// is not known
func (jdb *JsonDB) skimmedQuery(dbname, key string) ([]map[string]interface{}, error) {

	var retval []map[string]interface{}

	jsonType, ok := jdb.dbMap[dbname]
	if !ok {
		return nil, fmt.Errorf("invalid database name %s", dbname)
	}
	if jsonType.list != nil {
		for _, v := range jsonType.list {
			switch vv := v.(type) {
			case map[string]interface{}:
				_, ok := vv[key]
				if ok {
					retval = append(retval, vv)
				}
			default:
				return nil, errors.New("searching at multiple depths not supported")
			}
		}
		return retval, nil
	}
	panic("uninitialized database map")
}

func (jdb *JsonDB) getIndex(dbname, key string) (*IndexBackend, error) {

	if jdb == nil || jdb.dbMap == nil || len(jdb.dbMap) == 0 {
		return nil, ErrUninitializedDB
	}
	// index wasn't created
	if jdb.dbIndex == nil || len(jdb.dbIndex) == 0 {
		return nil, ErrIndexNotFound
	}
	// database with dbname isn't indexed
	keyIndex, ok := jdb.dbIndex[dbname]
	if !ok {
		return nil, ErrInvalidDatabase
	}
	// key not indexed
	indexBackend, ok := keyIndex[key]
	if !ok {
		return nil, ErrIndexNotFound
	}
	return indexBackend, nil
}

func (jdb *JsonDB) isIndexed(dbname, key string) (bool, error) {
	if _, err := jdb.getIndex(dbname, key); err != nil {
		return false, err
	}
	return true, nil
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
