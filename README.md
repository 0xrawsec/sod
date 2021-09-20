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



# Example
