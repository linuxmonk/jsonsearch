package jsondb

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/gusaki/jsonsearch/internal/db"
)

var (
	ErrInvalidDatabase      = errors.New("unknown database name")
	ErrIndexNotFound        = errors.New("index not found")
	ErrUninitializedDB      = errors.New("uninitialized database")
	ErrKeyValueNotFound     = errors.New("search key with value not found")
	ErrKeyNotFound          = errors.New("search key not found")
	ErrUnsupportedIndexType = errors.New("cannot index on type")
	ErrMissingJson          = errors.New("nil/missing JSON input")
	ErrNameMismatch         = errors.New("input readers and names must match")
)

type JSONType struct {
	list []interface{}
	dict map[string]interface{}
}

type DBMap map[string]*JSONType

type JsonDB struct {
	dbMap   DBMap
	dbIndex DBIndex
}

func Load(filenames []string) (*JsonDB, error) {

	var jsonDB JsonDB

	if len(filenames) == 0 {
		return nil, ErrMissingJson
	}

	jsonDB.dbMap = make(DBMap)
	for _, fname := range filenames {
		file, err := os.Open(fname)
		base := filepath.Base(fname)
		ext := filepath.Ext(base)
		dbname := base[0:strings.Index(base, ext)]
		if err != nil {
			log.Println("Error opening file", err)
			return nil, err
		}
		v, err := db.LoadJson(file)
		if err != nil {
			log.Println("Error loading JSON files to the database", err)
			return nil, err
		}
		jsonType := &JSONType{}
		switch jtype := v.(type) {
		case map[string]interface{}:
			jsonType.dict = jtype
		case []interface{}:
			jsonType.list = jtype
		default:
			return nil, ErrInvalidDatabase
		}
		jsonDB.dbMap[dbname] = jsonType
	}
	return &jsonDB, nil
}

func (jdb *JsonDB) Search(dbname, key, value string) ([]interface{}, error) {

	if jdb == nil || jdb.dbMap == nil {
		return nil, ErrInvalidDatabase
	}

	// Look in the index
	if jdb.dbIndex != nil {
		kIndex, kIndexOk := jdb.dbIndex[dbname]
		if kIndexOk {
			vIndex, vIndexOk := kIndex[key]
			if vIndexOk {
				v, vOk := vIndex[value]
				if vOk {
					result := make([]interface{}, 0)
					result = append(result, v)
					return result, nil
				}
			}
		} else {
			return nil, ErrInvalidDatabase
		}
	}

	root := jdb.getDB(dbname)
	r, err := db.Search(root, dbname, key, value)
	if err != nil {
		return nil, err
	}
	return r, nil
}
