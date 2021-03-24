package jsondb

import (
	"errors"
	"fmt"
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

	errNotRelated = errors.New("db not related")
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

func getRelatedDB(dbname, key, relationship string) (string, string, error) {

	found := false
	r := strings.Split(relationship, ":")
	for _, idx := range r {
		li := strings.LastIndex(idx, ".")
		rdb := idx[0:li]
		rkey := idx[li+1:]
		if rdb == dbname && rkey == key {
			found = true
			break
		}
	}
	if found == true {
		li := strings.LastIndex(r[1], ".")
		return r[1][0:li], r[1][li+1:], nil
	}
	return "", "", errNotRelated
}

func (jdb *JsonDB) searchIndex(dbname, key, value string) ([]interface{}, error) {
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
	return nil, ErrIndexNotFound
}

func (jdb *JsonDB) Search(dbname, key, value string, relations []string) ([]interface{}, error) {

	var results []interface{}
	// dbname:key pairs
	var found []string
	var nfIndex []string

	if jdb == nil || jdb.dbMap == nil {
		return nil, ErrInvalidDatabase
	}

	notFoundInIndex := func() []string {
		var notFound []string
		related := 0
		relatedFound := false
		for _, r := range relations {
			relatedFound = false
			rdb, rkey, err := getRelatedDB(dbname, key, r)
			if err == nil {
				related++
				for _, f := range found {
					if f == fmt.Sprintf("%s:%s", rdb, rkey) {
						relatedFound = true
					}
				}
				if !relatedFound {
					notFound = append(notFound, fmt.Sprintf("%s:%s", rdb, rkey))
				}
			}
		}
		return notFound
	}

	// search index for the given dbname, key and value
	res, err := jdb.searchIndex(dbname, key, value)
	if err == ErrInvalidDatabase {
		return nil, ErrInvalidDatabase
	}
	if err == nil {
		results = append(results, res...)
		found = append(found, fmt.Sprintf("%s:%s", dbname, key))
		// check if the dbname has any related dbnames and
		// look for the related values in the index
		for _, reln := range relations {
			relDb, relKey, err := getRelatedDB(dbname, key, reln)
			if err != nil {
				continue
			}
			rres, err := jdb.searchIndex(relDb, relKey, value)
			if err == nil {
				results = append(results, rres...)
				found = append(found, fmt.Sprintf("%s:%s", relDb, relKey))
			}
		}
		nfIndex = notFoundInIndex()
		if len(nfIndex) == 0 {
			return results, nil
		}
	}

	if len(nfIndex) > 0 {
		// perform full search for the ones not found on index
		for _, v := range nfIndex {
			li := strings.LastIndex(v, ":")
			nDb := v[0:li]
			nKey := v[li+1:]
			root := jdb.getDB(nDb)
			r, err := db.Search(root, nDb, nKey, value)
			if err != nil {
				continue
			}
			results = append(results, r...)
		}
		return results, nil
	}

	// perform full search for everything
	root := jdb.getDB(dbname)
	r, err := db.Search(root, dbname, key, value)
	if err != nil {
		return nil, err
	}
	results = append(results, r...)
	for _, reln := range relations {
		relDb, relKey, err := getRelatedDB(dbname, key, reln)
		if err != nil {
			continue
		}
		root := jdb.getDB(relDb)
		r, err := db.Search(root, relDb, relKey, value)
		if err != nil {
			continue
		}
		results = append(results, r...)
	}
	if len(results) == 0 {
		return nil, ErrKeyValueNotFound
	}
	return results, nil
}
