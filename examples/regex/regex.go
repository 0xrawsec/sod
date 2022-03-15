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

func insertOrPanic(db *sod.DB, p *Person) {
	if err := db.InsertOrUpdate(p); err != nil {
		panic(err)
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

	insertOrPanic(db, &Person{FirstName: "John", LastName: "Lennon", Age: 42})
	insertOrPanic(db, &Person{FirstName: "Johnny", LastName: "Cash", Age: 71})
	insertOrPanic(db, &Person{FirstName: "Joe", LastName: "Dassin", Age: 41})

	fmt.Println("Printing Persons with a first name starting matching regex")
	printSearchResult(db.Search(&Person{}, "FirstName", "~=", "^(?i:john.*)"))

	fmt.Println("Printing Persons with a first name starting with J")
	printSearchResult(db.Search(&Person{}, "FirstName", "~=", "^J"))
}
