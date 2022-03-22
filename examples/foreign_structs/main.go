package main

import (
	"fmt"
	"os"

	"github.com/0xrawsec/sod"
)

// We make the assumption ForeignStruct is
// defined in an imported package. In this case
// we cannot edit the struct to make it a sod.Item
// and set the approriate sod tags
type ForeignStruct struct {
	FirstName string
	LastName  string
	Age       int
}

// An embedded structure needs to be defined in the
// package you are actually writting
type Person struct {
	sod.Item
	ForeignStruct
	// you can even add new fields if you like
	Country string `sod:"index,upper"`
}

func NewLocalPerson(first, last string, age int, country string) Person {
	return Person{ForeignStruct: ForeignStruct{FirstName: first, LastName: last, Age: age}, Country: country}
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
	defer db.Close()

	// getting FieldDescritpors from Object we defined
	descriptors := sod.FieldDescriptors(&Person{})

	// we make a field of the foreign structure an indexed field
	if err := descriptors.Constraint("ForeignStruct.FirstName", sod.Constraints{Index: true}); err != nil {
		panic(err)
	}

	customSchema := sod.NewCustomSchema(descriptors, sod.DefaultExtension)
	// We need to create a directory and a schema to store Person structures
	if err := db.Create(&Person{}, customSchema); err != nil {
		panic(err)
	}

	john := NewLocalPerson("John", "Doe", 42, "us")
	// insert person in the db
	if err := db.InsertOrUpdate(&john); err != nil {
		panic(err)
	}

	connor := NewLocalPerson("John", "Connor", 10, "us")
	// insert person in the db
	if err := db.InsertOrUpdate(&connor); err != nil {
		panic(err)
	}

	lennon := NewLocalPerson("John", "Lennon", 40, "uk")
	// insert person in the db
	if err := db.InsertOrUpdate(&lennon); err != nil {
		panic(err)
	}

	fmt.Println("Showing indexed fields")
	s, err := db.Schema(&Person{})
	if err != nil {
		panic(err)
	}
	for _, fd := range s.Indexed() {
		fmt.Println(fd)
	}
	fmt.Println()

	printSearchResult(db.Search(&Person{}, "Age", ">=", 40))
	printSearchResult(db.Search(&Person{}, "FirstName", "=", "John").And("Age", "<", 42))
	printSearchResult(db.Search(&Person{}, "LastName", "=", "Connor").Or("Age", "<", 128))
	printSearchResult(db.Search(&Person{}, "LastName", "=", "Connor").Or("LastName", "=", "Doe"))
	printSearchResult(db.Search(&Person{}, "Country", "=", "us"))
}
