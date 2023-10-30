package postgregraph

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/odit-bit/linkstore/linkgraph"
)

const linkUpsertQuery = `
	INSERT INTO links (url, retrieved_at) 
	VALUES ($1, $2)
	ON CONFLICT (url) DO UPDATE SET retrieved_at=GREATEST(links.retrieved_at, $2)
	RETURNING id,retrieved_at
`

// UpsertLink implements graph.Graph.
// TODO: make fix time standar so no need to call UTC() every time
func (p *postgre) UpsertLink(link *linkgraph.Link) error {
	link.RetrievedAt = link.RetrievedAt.UTC()
	err := p.db.QueryRowxContext(context.TODO(), linkUpsertQuery, link.URL, link.RetrievedAt).Scan(
		&link.ID,
		&link.RetrievedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert link: %v ", err)
	}

	return nil
}

const edgeUpsertQuery = `
	INSERT INTO edges (src, dst, update_at) 
	VALUES ($1, $2, NOW())
	ON CONFLICT (src,dst) DO UPDATE SET update_at=NOW()
	RETURNING id,update_at
`

// UpsertEdge implements graph.Graph.
// TODO: make fix time standar so no need to call UTC() every time
func (p *postgre) UpsertEdge(edge *linkgraph.Edge) error {
	edge.UpdateAt = edge.UpdateAt.UTC()

	err := p.db.QueryRowxContext(context.TODO(), edgeUpsertQuery, edge.Src, edge.Dst).Scan(&edge.ID, &edge.UpdateAt)
	if err != nil {
		pgErr, ok := err.(*pgconn.PgError)
		if ok {
			switch pgErr.Code {
			case "23503":
				return linkgraph.ErrUnknownEdgeLinks
			}
		}

		return fmt.Errorf("edge upsert: %v", err)

	}
	return nil
}
