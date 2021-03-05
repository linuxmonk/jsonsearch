package db

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var ErrMissingJson = errors.New("nil/missing JSON input")
var ErrNameMismatch = errors.New("input readers and names must match")

type JSONType struct {
	list []interface{}
	dict map[string]interface{}
}

type DBMap map[string]*JSONType

type JsonDB struct {
	dbMap   DBMap
	dbIndex DBIndex
}

func Load(filenames []string) (JsonDB, error) {

	var jsonDB JsonDB

	if len(filenames) == 0 {
		return jsonDB, errors.New("input files missing")
	}

	jsonDB.dbMap = make(DBMap)
	jsonDB.dbIndex = make(DBIndex)

	for _, fname := range filenames {
		file, err := os.Open(fname)
		if err != nil {
			log.Println("Error opening file", err)
			return jsonDB, err
		}
		if err := loadJson(fname, file, jsonDB.dbMap); err != nil {
			log.Println("Error loading JSON files to the database", err)
			return jsonDB, err
		}
	}
	return jsonDB, nil
}

func (jdb *JsonDB) Search(dbname, key, value string) ([]interface{}, error) {

	results := make([]interface{}, 0)
	isIndexed, err := jdb.isIndexed(dbname, key)
	if isIndexed == true {
		result, err := jdb.searchIndex(dbname, key, value)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
		return results, nil
	}

	indexBackend, err := jdb.kvquery(dbname, key, value, true, false)
	if err != nil {
		return nil, err
	}
	if len(indexBackend.resultSet) == 0 {
		return nil, ErrKeyValueNotFound
	}
	return indexBackend.resultSet, nil
}

func loadJson(fname string, reader io.Reader, db DBMap) error {

	var dbEntry JSONType
	var genericEntry interface{}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &genericEntry); err != nil {
		return err
	}
	base := filepath.Base(fname)
	ext := filepath.Ext(base)
	name := base[0:strings.Index(base, ext)]
	switch vv := genericEntry.(type) {
	case map[string]interface{}:
		dbEntry.dict = vv
	case []interface{}:
		dbEntry.list = vv
	default:
		return errors.New("Invalid JSON")
	}
	db[name] = &dbEntry
	return nil
}
