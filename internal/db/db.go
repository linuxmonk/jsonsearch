package db

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"strconv"
)

var (
	ErrKeyValueNotFound     = errors.New("search key with value not found")
	ErrKeyNotFound          = errors.New("search key not found")
	ErrUnsupportedIndexType = errors.New("cannot index on type")
	ErrInvalidJson          = errors.New("invalid JSON")
)

func LoadJson(reader io.Reader) (interface{}, error) {

	var genericEntry interface{}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(b, &genericEntry); err != nil {
		return nil, err
	}
	switch vv := genericEntry.(type) {
	case map[string]interface{}:
		return vv, nil
	case []interface{}:
		return vv, nil
	}
	return nil, ErrInvalidJson
}

// Create a map index with the key's value for quick access.
// The function returns a IndexBackend with stringIndex if
// the value is of basic indexable type. If the type is
// of complex type (array / map) an error is returned.
//
func CreateIndex(unmarshalledJson interface{}, dbname, key string) (map[string]interface{}, error) {

	var result map[string]interface{}
	var val interface{}
	var found bool

	result = make(map[string]interface{})
	toResult := func(valueFound interface{}, enclObj interface{}) bool {
		switch indexVal := valueFound.(type) {
		case int:
			sval := strconv.Itoa(indexVal)
			result[sval] = enclObj
			return true
		case float64:
			sval := strconv.Itoa(int(indexVal))
			result[sval] = enclObj
			return true
		case string:
			result[indexVal] = enclObj
			return true
		}
		return false
	}

	switch jsonType := unmarshalledJson.(type) {
	case []interface{}:
		for _, lobj := range jsonType {
			if found, val = find(key, lobj); found {
				saved := toResult(val, lobj)
				if !saved {
					return nil, ErrUnsupportedIndexType
				}
			}
		}
		if len(result) == 0 {
			return nil, ErrKeyNotFound
		}
		return result, nil
	case map[string]interface{}:
		v, ok := jsonType[key]
		if ok {
			saved := toResult(v, jsonType)
			if !saved {
				return nil, ErrUnsupportedIndexType
			}
			if len(result) == 0 {
				return nil, ErrKeyNotFound
			}
			return result, nil
		}
		for _, v := range jsonType {
			switch mobj := v.(type) {
			case []interface{}:
				found, val = find(key, mobj)
				if found == true {
					saved := toResult(val, mobj)
					if !saved {
						return nil, ErrUnsupportedIndexType
					}
				}
			case map[string]interface{}:
				found, val = find(key, mobj)
				if found == true {
					saved := toResult(val, mobj)
					if !saved {
						return nil, ErrUnsupportedIndexType
					}
				}
			}
		}
		if len(result) == 0 {
			return nil, ErrKeyNotFound
		}
		return result, nil
	}
	return nil, ErrKeyNotFound
}

// Perform a search on the entire JSON object and look for the key with the
// corresponding value. The search is not indexed. If the key and value
// match one or more results are returned in IndexBackend.resultSet. If
// no values are found then an error is returned.
//
func Search(root interface{}, dbname, key, value string) ([]interface{}, error) {

	var result []interface{}
	var found bool

	toResult := func(valueFound string, enclObj interface{}) {
		result = append(result, enclObj)
	}

	result = make([]interface{}, 0)
	switch jsonType := root.(type) {
	case []interface{}:
		for _, lobj := range jsonType {
			found, _ = findv(key, value, lobj)
			if found {
				toResult(value, lobj)
			}
		}
		if len(result) == 0 {
			return nil, ErrKeyValueNotFound
		}
		return result, nil
	case map[string]interface{}:
		v, ok := jsonType[key]
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
				return result, nil
			}
			return nil, ErrKeyValueNotFound
		}
		for _, v := range jsonType {
			found, _ = findv(key, value, v)
			if found == true {
				toResult(value, v)
			}
		}
		if len(result) == 0 {
			return nil, ErrKeyValueNotFound
		}
		return result, nil
	}
	return nil, ErrKeyValueNotFound
}
