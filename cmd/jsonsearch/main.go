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
	var dbname string
	var keyPath string
	var value string

	flag.Var(&dbfiles, "dbfiles", "Comma separated list of filenames/filepaths")
	flag.Var(
		&indexKeys, "indexby", "Comma separated list of index keys."+
			" In the form of <filename.json_key>."+
			"\nExample: organizations._id,tickets.id")
	keyRelns = make(KeyRelations)
	flag.Var(&keyRelns, "relationships", "Comma separated list of relationships\n"+
		"with each relationship delimited with a colon."+
		"\nExample: organizations._id:tickets.organization_id,users.organization_id:organizations._id")
	flag.StringVar(&keyPath, "keypath", "", "Dot separated path to the JSON key")
	flag.StringVar(&dbname, "searchdb", "", "Name of database to search")
	flag.StringVar(&value, "searchvalue", "", "Search value")
	flag.Parse()
	if dbfiles == nil || len(dbfiles) == 0 {
		fmt.Println("Missing required argument: -dbfiles")
		flag.PrintDefaults()
		os.Exit(1)
	}
	if strings.TrimSpace(dbname) == "" || strings.TrimSpace(keyPath) == "" || strings.TrimSpace(value) == "" {
		fmt.Println("Missing required argument(s): -keypath / -searchvalue")
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
		if li == -1 {
			fmt.Println("Invalid format -indexby")
			flag.PrintDefaults()
			os.Exit(1)
		}
		dbname := key[0:li]
		jsonkey := key[li+1:]
		err = jsonDb.BuildIndex(dbname, jsonkey, true)
		if err != nil {
			log.Println("Indexing has failed. This will make searches slow")
		}
	}
	fmt.Println("================ Test Data Search Array/List values =================")
	results, err := jsonDb.Search(dbname, keyPath, value)
	if err != nil {
		fmt.Println(err)
	}
	PrintResults(results)
}

func PrintResults(results []interface{}) {
	if len(results) == 0 {
		fmt.Println("Empty results")
	}
	fmt.Println("----------------------------------------------------")
	for i, r := range results {
		fmt.Println("----")
		dict, ok := r.(map[string]interface{})
		if !ok {
			fmt.Printf("Unrecognized format for result at index %d\n", i)
		}
		for k, v := range dict {
			switch val := v.(type) {
			case float64:
				fmt.Printf("%s: %v\n", k, val)
			case int:
				fmt.Printf("%s: %v\n", k, val)
			case bool:
				fmt.Printf("%s: %v\n", k, val)
			case string:
				fmt.Printf("%s: %s\n", k, val)
			case []interface{}:
				fmt.Printf("%s: %v\n", k, val)
			default:
				fmt.Printf("Unknown Type: %T. Key = %s, Val = %v\n", val, k, val)
			}
		}
	}
}
