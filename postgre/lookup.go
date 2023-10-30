package postgregraph

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"

	"github.com/odit-bit/linkstore/linkgraph"
)

const lookupLinkQuery = `
	SELECT id, url, retrieved_at
	FROM links
	WHERE id = $1
`

// LookupLink implements graph.Graph.
func (p *postgre) LookupLink(id uuid.UUID) (*linkgraph.Link, error) {
	var link linkgraph.Link

	err := p.db.QueryRowxContext(context.TODO(), lookupLinkQuery, id).Scan(&link.ID, &link.URL, &link.RetrievedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, linkgraph.ErrNotFound
		}
		return nil, fmt.Errorf("lookup link: %v", err)
	}

	return &link, nil
}
