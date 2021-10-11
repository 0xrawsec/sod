package main

import (
	"fmt"
	"os"

	"github.com/0xrawsec/sod"
)

type UniquePerson struct {
	sod.Item
	FirstName string `sod:"index"`
	LastName  string `sod:"unique"`
	Age       int    `sod:"index"`
}

func printSearchResult(s *sod.Search) {
	if sr, err := s.Collect(); err != nil {
		panic(err)
	} else {
		fmt.Printf("Search brought %d results\n", len(sr))
		for _, obj := range sr {
			fmt.Println(obj.(*UniquePerson))
		}
		fmt.Println()
	}
}

func main() {
	dbpath := "./data/database"

	os.RemoveAll(dbpath)

	db := sod.Open(dbpath)
	// We need to create a directory and a schema to store Person structures
	if err := db.Create(&UniquePerson{}, sod.DefaultSchema); err != nil {
		panic(err)
	}

	john := UniquePerson{FirstName: "John", LastName: "Doe", Age: 42}
	// insert person in the db
	if err := db.InsertOrUpdate(&john); err != nil {
		panic(err)
	}

	lennon := UniquePerson{FirstName: "John", LastName: "Lennon", Age: 40}
	// insert person in the db
	if err := db.InsertOrUpdate(&lennon); err != nil {
		panic(err)

	}
	alfred := UniquePerson{FirstName: "Alfred", LastName: "Lennon", Age: 10}
	// insert person in the db
	if err := db.InsertOrUpdate(&alfred); sod.IsUnique(err) {
		fmt.Printf("person %v cannot be added as it does not meet uniqueness constraints\n", alfred)
	}

	printSearchResult(db.Search(&UniquePerson{}, "Age", ">=", 40))
	printSearchResult(db.Search(&UniquePerson{}, "FirstName", "=", "John").And("Age", "<", 42))
	printSearchResult(db.Search(&UniquePerson{}, "LastName", "=", "Connor").Or("Age", "<", 128))
	printSearchResult(db.Search(&UniquePerson{}, "LastName", "=", "Connor").Or("LastName", "=", "Doe"))
}
