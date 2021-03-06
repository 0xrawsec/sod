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
	var persons []*Person
	if err := s.Assign(&persons); err != nil {
		panic(err)
	} else {
		fmt.Printf("Search brought %d results\n", len(persons))
		for _, person := range persons {
			fmt.Println(person)
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

	printSearchResult(db.Search(&Person{}, "Age", ">=", 40))
}
