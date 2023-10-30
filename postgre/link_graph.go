package postgregraph

import (
	"context"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/odit-bit/linkstore/linkgraph"
)

var _ linkgraph.Graph = (*postgre)(nil)

type postgre struct {
	db *sqlx.DB
}

func New(db *sqlx.DB) *postgre {
	p := postgre{
		db: db,
	}
	if err := p.Migrate(); err != nil {
		log.Fatal(err)
	}
	return &p
}

// ==============

// const dropLinksTable = `
// DROP TABLE IF EXISTS links;
// `

// const dropEdgeTable = `
// DROP TABLE IF EXISTS edges;
// `

const createLinkTableQuery = `
		CREATE TABLE IF NOT EXISTS links(
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			url text UNIQUE,
			retrieved_at TIMESTAMP 
		);
`

const createEdgeTableQuery = `
		CREATE TABLE IF NOT EXISTS edges(
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			src UUID NOT NULL REFERENCES links(id) ON DELETE CASCADE,
			dst UUID NOT NULL REFERENCES links(id) ON DELETE CASCADE,
			update_at TIMESTAMP,
			CONSTRAINT edge_links UNIQUE(src,dst)
		);
`

func (p *postgre) Migrate() error {
	//link table
	_, err := p.db.ExecContext(context.TODO(), createLinkTableQuery)
	if err != nil {
		return fmt.Errorf("create table: %v", err)
	}

	//edge table
	_, err = p.db.ExecContext(context.TODO(), createEdgeTableQuery)
	if err != nil {
		return fmt.Errorf("create table: %v", err)
	}

	return nil
}
