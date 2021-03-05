package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gusaki/jsonsearch/internal/pkg/db"
)

func main() {

	var dbfiles DBFiles
	var indexKeys IndexBy
	var keyRelns KeyRelations

	flag.Var(&dbfiles, "dbfiles", "Comma separated list of filenames/filepaths")
	flag.Var(
		&indexKeys, "indexby", "Comma separated list of index keys."+
			" In the form of <filename.json_key>."+
			"\nExample: organizations._id,tickets.id")
	keyRelns = make(KeyRelations)
	flag.Var(&keyRelns, "relationships", "Comma separated list of relationships\n"+
		"with each relationship delimited with a colon."+
		"\nExample: organizations._id:tickets.organization_id,users.organization_id:organizations._id")
	flag.Parse()
	if dbfiles == nil || len(dbfiles) == 0 {
		fmt.Println("Missing required argument: -dbfiles")
		flag.PrintDefaults()
		os.Exit(1)
	}
	jsonDb, err := db.Load(dbfiles)
	if err != nil {
		log.Println("Program terminated with an error")
		os.Exit(1)
	}
	for _, key := range indexKeys {
		li := strings.LastIndex(key, ".")
		dbname := key[0:li]
		jsonkey := key[li+1:]
		err = jsonDb.Index(dbname, jsonkey, true)
		if err != nil {
			log.Println("Indexing has failed. This will make searches slow")
		}
	}
}
