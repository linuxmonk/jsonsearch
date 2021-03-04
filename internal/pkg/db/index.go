package db

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
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
}

// Index is a mapping of index key name to the real backend index
type KeyIndex map[string]*IndexBackend

// DBIndex is a mapping of database name it's indexes
type DBIndex map[string]KeyIndex

func (jdb *JsonDB) Index(dbname, keyname string, recursive bool) error {

	if jdb == nil || jdb.dbMap == nil || len(jdb.dbMap) == 0 {
		return errors.New("nil/empty json db")
	}

	if recursive == true {
		result, err := jdb.rquery(dbname, keyname)
		if err != nil {
			log.Printf("Error cannot recursive index on database %v key %v", dbname, keyname)
			return err
		}
		keyIndex := make(KeyIndex)
		keyIndex[keyname] = result
		jdb.dbIndex[dbname] = keyIndex
		return nil
	}

	result, err := jdb.query(dbname, keyname)
	if err != nil {
		log.Printf("Error cannot index on database %v key %v", dbname, keyname)
		return fmt.Errorf("cannot index on db %s key %s", dbname, keyname)
	}
	jdb.dbIndex[dbname] = make(KeyIndex)
	return doIndex(result, keyname, jdb.dbIndex[dbname])
}

func doIndex(result []map[string]interface{}, key string, keyIndex KeyIndex) error {

	var strval string

	if len(result) == 0 {
		return errors.New("empty result set")
	}
	if strings.TrimSpace(key) == "" {
		return errors.New("cannot index on an empty key name")
	}
	indexBackend := &IndexBackend{
		stringIndex: make(map[string]interface{}),
	}
	keyIndex[key] = indexBackend
	for _, r := range result {
		// query already returned the values for the key. All the values in
		// result have key present
		val := r[key]
		switch x := val.(type) {
		case int:
			strval = strconv.Itoa(x)
		case float64:
			i := int(x)
			strval = strconv.Itoa(i)
		case string:
			strval = x
		default:
			log.Println("Found unexpected data type (Supported types: int, string). Skipping indexing")
			continue
		}
		keyIndex[key].stringIndex[strval] = r
	}
	return nil
}
