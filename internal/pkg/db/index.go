package db

import (
	"errors"
	"log"
	"strconv"
)

type JsonType uint8

const (
	JsonTypeString  JsonType = iota
	JsonTypeInteger          // falls under JSON's number type
	JsonTypeFloat            // falls under JSON's number type
	JsonTypeObject
	JsonTypeArray
	JsonTypeBoolean
	JsonTypeNull
)

type IndexBackend struct {
	stringIndex map[string]interface{}
	resultSet   []interface{}
}

type SearchResults []map[string]interface{}

// Index is a mapping of index key name to the real backend index
type KeyIndex map[string]*IndexBackend

// DBIndex is a mapping of database name it's indexes
type DBIndex map[string]KeyIndex

func (jdb *JsonDB) BuildIndex(dbname, keyname string, recursive bool) error {

	if jdb == nil || jdb.dbMap == nil || len(jdb.dbMap) == 0 {
		return errors.New("nil/empty json db")
	}

	result, err := jdb.createIndex(dbname, keyname)
	if err != nil {
		log.Printf("Error cannot recursive index on database %v key %v", dbname, keyname)
		return err
	}
	keyIndex := make(KeyIndex)
	keyIndex[keyname] = result
	jdb.dbIndex[dbname] = keyIndex
	return nil
}

// Create a map index with the key's value for quick access.
// The function returns a IndexBackend with stringIndex if
// the value is of basic indexable type. If the type is
// of complex type (array / map) an error is returned.
//
func (jdb *JsonDB) createIndex(dbname, key string) (*IndexBackend, error) {

	var result IndexBackend
	var val interface{}
	var found bool

	jsonType, ok := jdb.dbMap[dbname]
	if !ok {
		return nil, ErrInvalidDatabase
	}

	result.stringIndex = make(map[string]interface{})
	toResult := func(valueFound interface{}, enclObj interface{}) bool {
		switch indexVal := valueFound.(type) {
		case int:
			sval := strconv.Itoa(indexVal)
			result.stringIndex[sval] = enclObj
			return true
		case float64:
			sval := strconv.Itoa(int(indexVal))
			result.stringIndex[sval] = enclObj
			return true
		case string:
			result.stringIndex[indexVal] = enclObj
			return true
		}
		return false
	}

	if jsonType.list != nil {
		for _, lobj := range jsonType.list {
			if found, val = find(key, lobj); found {
				saved := toResult(val, lobj)
				if !saved {
					return nil, ErrUnsupportedIndexType
				}
			}
		}
		if len(result.stringIndex) == 0 {
			return nil, ErrKeyNotFound
		}
		return &result, nil
	}

	if jsonType.dict != nil {
		v, ok := jsonType.dict[key]
		if ok {
			saved := toResult(v, jsonType.dict)
			if !saved {
				return nil, ErrUnsupportedIndexType
			}
			if len(result.stringIndex) == 0 {
				return nil, ErrKeyNotFound
			}
			return &result, nil
		}

		for _, v := range jsonType.dict {
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
		if len(result.stringIndex) == 0 {
			return nil, ErrKeyNotFound
		}
		return &result, nil
	}
	return nil, ErrKeyNotFound
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
