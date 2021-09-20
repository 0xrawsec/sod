![build](https://github.com/0xrawsec/sod/actions/workflows/go.yml/badge.svg)
![coverage](https://raw.githubusercontent.com/0xrawsec/sod/master/.github/coverage/badge.svg)

# Go Simple Object Database

A simple database model to store Go structure (on disk) and search across them.
It has features close to what an ORM framework can provide but has the advantage of being:
 * in pure Go (and thus portable)
 * does not depend on any DB engine (SQL, SQLite, Mongo ...) to do its job
 * everything is kept simple (one file per structure and eventually an index)
It supports structure fields indexing to speed up searches on important fields.

What you should use this project for:
 * you want to implement Go struct persistency in a simple way
 * you want to be able to DB engine like operations on those structures (Update, Delete, Search ...)
 * you don't want to deploy an ORM framework

What you should not use this project for:
 * even though performances are not so bad, I don't think you can rely on it for high troughput DB operations

# Performances



# Examples

```go
type Person struct {
		Item
		FirstName string
		LastName  string
		Age       int
	}

printSearchResult := func(s *Search) {
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

// we create an index on LastName and Age
index := NewIndex("LastName", "Age")

s := &Schema{Extension: ".json", ObjectsIndex: index}
db := Open(dbpath)
// We need to create a directory and a schema to store Person structures
if err := db.Create(&Person{}, s); err != nil {
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

if err := db.Commit(&Person{}); err != nil {
    panic(err)
}

printSearchResult(db.Search(&Person{}, "Age", ">=", 40))
printSearchResult(db.Search(&Person{}, "FirstName", "=", "John").And("Age", "<", 42))
printSearchResult(db.Search(&Person{}, "LastName", "=", "Connor").Or("Age", "<", 128))
printSearchResult(db.Search(&Person{}, "LastName", "=", "Connor").Or("LastName", "=", "Doe"))
```
