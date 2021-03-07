package jsondb

import (
	"log"

	"github.com/gusaki/jsonsearch/internal/db"
)

type SearchResults []map[string]interface{}

// keyIndex is a map of key name to its value indexes.
// map[keyname] -> map[value]interface{}
type keyIndex map[string]map[string]interface{}

// DBIndex is a mapping of database name it's indexes
type DBIndex map[string]keyIndex

func (jdb *JsonDB) getDB(name string) interface{} {
	jsonType := jdb.dbMap[name]
	if jsonType.list != nil {
		return jsonType.list
	}
	return jsonType.dict
}

func (jdb *JsonDB) BuildIndex(dbname, keyname string) error {

	if jdb == nil || jdb.dbMap == nil || len(jdb.dbMap) == 0 {
		return ErrInvalidDatabase
	}

	root := jdb.getDB(dbname)
	if jdb.dbIndex == nil {
		jdb.dbIndex = make(DBIndex)
	}
	result, err := db.CreateIndex(root, dbname, keyname)
	if err != nil {
		log.Printf("Error %v, cannot create index on database %v key %v", err, dbname, keyname)
		return err
	}
	kIndex := make(keyIndex)
	kIndex[keyname] = result
	jdb.dbIndex[dbname] = kIndex
	return nil
}
