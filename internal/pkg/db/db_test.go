package db

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
	err = jsonDb.Index("organizations", "_id", true)
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
	err = jsonDb.Index("organizations", "nokey", true)
	assert.Equal(t, err, ErrKeyNotFound)
}

func TestIndexedSearch(t *testing.T) {
	files := []string{
		"./testdata/organizations.json",
		"./testdata/tickets.json",
		"./testdata/users.json",
	}
	jsonDb, err := Load(files)
	assert.Equal(t, err, nil)
	err = jsonDb.Index("organizations", "_id", true)
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
		_, err := jsonDb.Search(test.dbname, test.key, test.value)
		assert.Equal(t, err, test.err)
	}
}
