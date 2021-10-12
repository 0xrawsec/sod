package main

import (
	"fmt"
	"os"

	"github.com/0xrawsec/sod"
)

type Person struct {
	sod.Item
	FirstName string `sod:"index"`
	LastName  string `sod:"index"`
	Age       int    `sod:"index"`
}

func printSearchResult(s *sod.Search) {
	if sr, err := s.Collect(); err != nil {
		panic(err)
	} else {
		fmt.Printf("Search brought %d results\n", len(sr))
		for _, obj := range sr {
			fmt.Println(obj.(*Person))
		}
		fmt.Println()
	}
}

func main() {
	dbpath := "./data/database"

	os.RemoveAll(dbpath)

	db := sod.Open(dbpath)
	// We need to create a directory and a schema to store Person structures
	if err := db.Create(&Person{}, sod.DefaultSchema); err != nil {
		panic(err)
	}

	john := Person{FirstName: "John", LastName: "Doe", Age: 42}
	// insert person in the db
	if err := db.InsertOrUpdate(&john); err != nil {
		panic(err)
	}

	connor := Person{FirstName: "John", LastName: "Connor", Age: 10}
	// insert person in the db
	if err := db.InsertOrUpdate(&connor); err != nil {
		panic(err)
	}

	lennon := Person{FirstName: "John", LastName: "Lennon", Age: 40}
	// insert person in the db
	if err := db.InsertOrUpdate(&lennon); err != nil {
		panic(err)
	}

	printSearchResult(db.Search(&Person{}, "Age", ">=", 40))
	printSearchResult(db.Search(&Person{}, "FirstName", "=", "John").And("Age", "<", 42))
	printSearchResult(db.Search(&Person{}, "LastName", "=", "Connor").Or("Age", "<", 128))
	printSearchResult(db.Search(&Person{}, "LastName", "=", "Connor").Or("LastName", "=", "Doe"))
}
