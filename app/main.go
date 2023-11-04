package main

import (
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/odit-bit/linkstore"
	"github.com/odit-bit/linkstore/linkpostgre"
)

func main() {
	dsn := os.Getenv("DSN")
	if dsn == "" {
		log.Println("DSN var is nil")
		return
	}
	dbConn, err := connectPG(dsn)
	if err != nil {
		log.Fatal(err)
	}
	db := linkpostgre.New(dbConn)

	srv := linkstore.Server{
		Port:    8181,
		Handler: db,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

}

func connectPG(dsn string) (*sqlx.DB, error) {
	//IMPORT !!
	// _ "github.com/jackc/pgx/v5/stdlib"

	// DSN format
	// "host= dbname= password= user="

	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}
