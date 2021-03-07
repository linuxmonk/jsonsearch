package jsondb

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadSuccess(t *testing.T) {

	files := []string{
		"./testdata/organizations.json",
		"./testdata/tickets.json",
		"./testdata/users.json",
	}
	jsonDb, err := Load(files)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(jsonDb.dbMap), len(files))
}

func TestLoadLargeDatabase(t *testing.T) {
	files := []string{
		"./testdata/24mb.json",
	}
	jsonDb, err := Load(files)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(jsonDb.dbMap), len(files))
}

func TestLoadFail(t *testing.T) {
	files := []string{
		"./testdata/doesnotexist.json",
		"./testdata/tickets.json",
		"./testdata/users.json",
	}
	_, err := Load(files)
	log.Println(err)
	assert.NotEqual(t, err, nil)
}

func TestIndexDBSuccess(t *testing.T) {
	files := []string{
		"./testdata/organizations.json",
		"./testdata/tickets.json",
		"./testdata/users.json",
	}
	jsonDb, err := Load(files)
	assert.Equal(t, err, nil)
	err = jsonDb.BuildIndex("organizations", "_id")
	assert.Equal(t, err, nil)
}

func TestIndexLargeDB(t *testing.T) {
	files := []string{
		"./testdata/24mb.json",
	}
	jsonDb, err := Load(files)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(jsonDb.dbMap), len(files))
	err = jsonDb.BuildIndex("24mb", "id")
	assert.Equal(t, err, nil)
}

func TestIndexDBFailWrongKey(t *testing.T) {
	files := []string{
		"./testdata/organizations.json",
		"./testdata/tickets.json",
		"./testdata/users.json",
	}
	jsonDb, err := Load(files)
	assert.Equal(t, err, nil)
	err = jsonDb.BuildIndex("organizations", "nokey")
	assert.Equal(t, err, ErrKeyNotFound)
}

func TestLargeDBTest(t *testing.T) {
	files := []string{
		"./testdata/24mb.json",
	}
	jsonDb, err := Load(files)
	assert.Equal(t, err, nil)
	err = jsonDb.BuildIndex("24mb", "id")
	assert.Equal(t, err, nil)

	tests := []struct {
		name           string
		dbname         string
		key            string
		value          string
		err            error
		returnValCount int
	}{
		{
			"Search for a value that exists in the data set",
			"24mb",
			"id",
			"2489651045",
			nil,
			1,
		},
		{
			"Search for a value that does not exist in the data set",
			"24mb",
			"id",
			"1",
			ErrKeyValueNotFound,
			0,
		},
		{
			"Search for with a non existent key",
			"24mb",
			"nokey",
			"101",
			ErrKeyValueNotFound,
			0,
		},
		{
			"Search with a non existent dbname",
			"wrongobject",
			"_id",
			"101",
			ErrInvalidDatabase,
			0,
		},
		{
			"Search for values embedded in lists",
			"24mb",
			"sha",
			"6b089eb4a43f728f0a594388092f480f2ecacfcd",
			nil,
			1,
		},
	}
	for _, test := range tests {
		log.Println("Test: ", test.name)
		_, err := jsonDb.Search(test.dbname, test.key, test.value, nil)
		assert.Equal(t, err, test.err)
	}

}

func TestIndexedSearch(t *testing.T) {
	files := []string{
		"./testdata/organizations.json",
		"./testdata/tickets.json",
		"./testdata/users.json",
	}
	jsonDb, err := Load(files)
	assert.Equal(t, err, nil)
	err = jsonDb.BuildIndex("organizations", "_id")
	assert.Equal(t, err, nil)

	tests := []struct {
		name           string
		dbname         string
		key            string
		value          string
		err            error
		returnValCount int
	}{
		{
			"Search for a value that exists in the data set",
			"organizations",
			"_id",
			"105",
			nil,
			1,
		},
		{
			"Search for a value that does not exist in the data set",
			"organizations",
			"_id",
			"1993434",
			ErrKeyValueNotFound,
			0,
		},
		{
			"Search for with a non existent key",
			"organizations",
			"nokey",
			"101",
			ErrKeyValueNotFound,
			0,
		},
		{
			"Search with a non existent dbname",
			"wrongobject",
			"_id",
			"101",
			ErrInvalidDatabase,
			0,
		},
		{
			"Search for values embedded in lists",
			"organizations",
			"tags",
			"Cherry",
			nil,
			1,
		},
	}
	for _, test := range tests {
		log.Println("Test: ", test.name)
		_, err := jsonDb.Search(test.dbname, test.key, test.value, nil)
		assert.Equal(t, err, test.err)
	}
}

// initialize the JsonDB into package level variables to eliminate the
// loading from the search operations during the benchmark tests.

var benchlargeDb *JsonDB
var benchJsonDb *JsonDB
var benchResult []interface{}
var dbInitDone bool
var largeDbInitDone bool

func dbinit() {
	if dbInitDone {
		return
	}
	files := []string{
		"./testdata/organizations.json",
		"./testdata/tickets.json",
		"./testdata/users.json",
	}
	jdb, _ := Load(files)
	jdb.BuildIndex("organizations", "_id")
	benchJsonDb = jdb
	dbInitDone = true
}

func largeDbInit() {
	if largeDbInitDone {
		return
	}
	files := []string{
		"./testdata/24mb.json",
	}
	jdb, _ := Load(files)
	jdb.BuildIndex("24mb", "id")
	benchlargeDb = jdb
	largeDbInitDone = true
}

func TestMain(m *testing.M) {
	log.Println("Setup for benchmark tests")
	dbinit()
	largeDbInit()
	m.Run()
}

func benchmarkSearch(jsonDb *JsonDB, dbname, key, value string, b *testing.B) {
	for n := 0; n < b.N; n++ {
		benchResult, _ = jsonDb.Search(dbname, key, value, nil)
	}
}

func BenchmarkIndexedSearchByKey(b *testing.B) {
	benchmarkSearch(benchJsonDb, "organizations", "_id", "105", b)
}
func BenchmarkIndexedSearchValueNotFound(b *testing.B) {
	benchmarkSearch(benchJsonDb, "organizations", "_id", "999999999", b)
}
func BenchmarkSearchByKey(b *testing.B) {
	benchmarkSearch(benchJsonDb, "organizations", "name", "Enthaze", b)
}
func BenchmarkSearchByKeyNoValue(b *testing.B) {
	benchmarkSearch(benchJsonDb, "organizations", "name", "zzz", b)
}
func BenchmarkSearchListValues(b *testing.B) {
	benchmarkSearch(benchJsonDb, "organizations", "tags", "Frank", b)
}

// Large DB benchmark tests
func BenchmarkLargeDBIndexedSearchByKey(b *testing.B) {
	benchmarkSearch(benchlargeDb, "24mb", "id", "2489651051", b)
}
func BenchmarkLargeDBIndexedSearchValueNotFound(b *testing.B) {
	benchmarkSearch(benchlargeDb, "24mb", "id", "999999999", b)
}
func BenchmarkLargeDBSearchByKey(b *testing.B) {
	benchmarkSearch(benchlargeDb, "24mb", "login", "rspt", b)
}
func BenchmarkLargeDBSearchByKeyNoValue(b *testing.B) {
	benchmarkSearch(benchlargeDb, "24mb", "login", "zzz", b)
}
