package main

import (
	"fmt"
	"os"

	"github.com/0xrawsec/sod"
)

type Person struct {
	sod.Item
	FirstName string
	LastName  string
	Age       int
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

	// we create an index on LastName and Age
	index := sod.NewIndex("LastName", "Age")

	s := &sod.Schema{Extension: ".json", ObjectsIndex: index}

	db := sod.Open(dbpath)
	// We need to create a directory and a schema to store Person structures
	if err := db.Create(&Person{}, s); err != nil {
		panic(err)
	}

	john := Person{FirstName: "John", LastName: "Doe", Age: 42}
	// insert person in the db
	if err := db.InsertOrUpdate(&john); err != nil {
		panic(err)
	}

	printSearchResult(db.Search(&Person{}, "Age", ">=", 40))
}
