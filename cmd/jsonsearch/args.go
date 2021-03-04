package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
)

type DBFiles []string
type IndexBy []string
type KeyRelations map[string]string

func (f *DBFiles) String() string {
	return fmt.Sprint(*f)
}

func (f *DBFiles) Set(value string) error {
	if len(*f) > 0 {
		return errors.New("dbfiles flag already set")
	}
	for _, fname := range strings.Split(value, ",") {
		tFname := strings.TrimSpace(fname)
		if tFname == "" {
			log.Println("Error empty dbfiles parameter")
			continue
		}
		if _, err := os.Stat(tFname); err != nil {
			return err
		}
		// Append only if the value was not already present
		found := false
		for _, v := range *f {
			if tFname == v {
				found = true
				break
			}
		}
		if !found {
			*f = append(*f, tFname)
		}
	}
	return nil
}

func (i *IndexBy) String() string {
	return fmt.Sprint(*i)
}

func (i *IndexBy) Set(value string) error {
	for _, idx := range strings.Split(value, ",") {
		tIdx := strings.TrimSpace(idx)
		if tIdx == "" {
			log.Println("Error empty indexby parameter")
			continue
		}
		found := false
		for _, v := range *i {
			if tIdx == v {
				found = true
				break
			}
		}
		if !found {
			*i = append(*i, tIdx)
		}
	}
	return nil
}

func (kr KeyRelations) String() string {
	var s string
	for k, v := range kr {
		s += fmt.Sprintf("%s: %s ", k, v)
	}
	return s
}

func (k KeyRelations) Set(value string) error {

	for n, reln := range strings.Split(value, ",") {
		tReln := strings.TrimSpace(reln)
		if tReln == "" {
			log.Println("Error empty indexby parameter found at pos", n)
			continue
		}
		if strings.Index(tReln, ":") == -1 {
			return errors.New("invalid relationship format")
		}
		newkeyidx := strings.Index(tReln, ":")
		newkey := tReln[0:newkeyidx]
		newval := tReln[newkeyidx+1:]
		found := false
		for key := range k {
			if newkey == key {
				found = true
				break
			}
		}
		if !found {
			k[newkey] = newval
		}
	}
	return nil
}
