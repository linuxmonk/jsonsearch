package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gusaki/jsonsearch/pkg/jsondb"
)

func main() {

	var dbfiles DBFiles
	var indexKeys IndexBy
	var keyRelns KeyRelations
	var dbname string
	var keyPath string
	var value string
	var interactive bool

	flag.Var(&dbfiles, "dbfiles", "Comma separated list of filenames/filepaths")
	flag.Var(
		&indexKeys, "indexby", "Comma separated list of index keys."+
			" In the form of <filename.json_key>."+
			"\nExample: organizations._id,tickets.id")
	keyRelns = make(KeyRelations, 0)
	flag.Var(&keyRelns, "relationships", "Comma separated list of relationships\n"+
		"with each relationship delimited with a colon."+
		"\nExample: organizations._id:tickets.organization_id,users.organization_id:organizations._id")
	flag.StringVar(&keyPath, "keypath", "", "Dot separated path to the JSON key")
	flag.StringVar(&dbname, "searchdb", "", "Name of database to search")
	flag.StringVar(&value, "searchvalue", "", "Search value")
	flag.BoolVar(&interactive, "interactive", true, "Run in interactive mode")
	flag.Usage = func() {
		fmt.Println()
		fmt.Println("-dbfiles value")
		fmt.Println("\tComma separated list of filenames/filepaths")
		fmt.Println("-indexby value")
		fmt.Println("\tComma separated list of index keys. In the form of <filename.json_key>.")
		fmt.Println("\t\tExample: organizations._id,tickets.id")
		fmt.Println("-interactive")
		fmt.Println("\tRun in interactive mode")
		fmt.Println("-keypath string")
		fmt.Println("\tDot separated path to the JSON key")
		fmt.Println("-relationships value")
		fmt.Println("\tComma separated list of relationships")
		fmt.Println("\twith each relationship delimited with a colon.")
		fmt.Println("\t\tExample: organizations._id:tickets.organization_id,users.organization_id:organizations._id")
		fmt.Println("-searchdb string")
		fmt.Println("\tName of database to search")
		fmt.Println("-searchvalue string")
		fmt.Println("\tSearch value")
		fmt.Println()
		fmt.Println("Usage examples:")
		fmt.Println("Command line mode:")
		fmt.Println("\tjsonsearch -dbfiles /home/u/org.json,/home/u/tickets.json,/home/u/users.json \\")
		fmt.Println("\t-indexby org._id:tickets.id -relationships org._id:users.org_id \\")
		fmt.Println("\t-searchdb org -keypath _id -value 101")
		fmt.Println()
		fmt.Println("Interactive Mode (also requires some initialization parameters)")
		fmt.Println("\tjsonsearch -dbfiles /home/u/org.json,/home/u/tickets.json,/home/u/users.json \\")
		fmt.Println("\t-indexby org._id:tickets.id -relationships org._id:users.org_id \\")
		fmt.Println("\t-interactive")
	}
	flag.CommandLine.Usage = flag.Usage
	flag.Parse()

	if dbfiles == nil || len(dbfiles) == 0 {
		fmt.Println("Missing required argument: -dbfiles")
		flag.Usage()
		os.Exit(1)
	}

	if !interactive {
		if strings.TrimSpace(dbname) == "" || strings.TrimSpace(keyPath) == "" {
			fmt.Println("Missing required argument(s): -keypath / -searchvalue")
			flag.Usage()
			os.Exit(1)
		}
	}
	// process -dbfiles and load the database
	jsonDb, err := jsondb.Load(dbfiles)
	if err != nil {
		log.Println("Program terminated with an error")
		os.Exit(1)
	}

	// process -indexby and create indexes
	for _, key := range indexKeys {
		li := strings.LastIndex(key, ".")
		if li == -1 {
			fmt.Println("Invalid format -indexby")
			flag.Usage()
			os.Exit(1)
		}
		dbname := key[0:li]
		jsonkey := key[li+1:]
		err = jsonDb.BuildIndex(dbname, jsonkey)
		if err != nil {
			log.Println("Indexing has failed. This will make searches slow")
		}
	}

	// process -relationships and create indexes
	for _, reln := range keyRelns {
		r := strings.Split(reln, ":")
		for _, index := range r {
			li := strings.LastIndex(index, ".")
			if li == -1 {
				fmt.Println("Invalid format -relationship")
				flag.PrintDefaults()
				os.Exit(1)
			}
			dbname := index[0:li]
			jsonkey := index[li+1:]
			err = jsonDb.BuildIndex(dbname, jsonkey)
			if err != nil {
				log.Printf("Indexing has failed for %s", index)
			}
		}
	}

	if interactive {
		runInteractive(jsonDb, keyRelns)
		os.Exit(0)
	}

	results, err := jsonDb.Search(dbname, keyPath, value, keyRelns)
	if err != nil {
		fmt.Println(err)
	}
	PrintResults(results)
}
