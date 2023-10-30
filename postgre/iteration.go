package postgregraph

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/odit-bit/linkstore/linkgraph"
)

//==========

const linksIterationQuery = `
	SELECT id, url, retrieved_at 
	FROM links 
	WHERE id >= $1 AND id < $2 AND retrieved_at < $3
	`

// Links implements graph.Graph.
func (p *postgre) Links(fromID uuid.UUID, toID uuid.UUID, accessBefore time.Time) (linkgraph.LinkIterator, error) {

	// rows := p.db.QueryRowxContext(context.Background(), linksQuery, fromID, toID, retrieveBefore.UTC())
	rows, err := p.db.QueryxContext(context.TODO(), linksIterationQuery, fromID, toID, accessBefore.UTC())
	if err != nil {
		return nil, err
	}

	linkIterator := iterator{
		rows:    rows,
		lastErr: nil,
	}

	return &linkIterator, nil
}

//==========

const edgesIterationQuery = `
	SELECT id, src, dst, update_at 
	FROM edges 
	WHERE src >= $1 AND src < $2 AND update_at < $3
`

// Edges implements graph.Graph.
func (p *postgre) Edges(fromID uuid.UUID, toID uuid.UUID, updateBefore time.Time) (linkgraph.EdgeIterator, error) {
	//find edges row
	rows, err := p.db.QueryxContext(context.TODO(), edgesIterationQuery, fromID, toID, updateBefore.UTC())
	if err != nil {
		return nil, fmt.Errorf("edge iterator: %v", err)
	}

	edgeIterator := iterator{
		rows:    rows,
		lastErr: err,
	}

	return &edgeIterator, nil
}

//==========

var _ linkgraph.LinkIterator = (*iterator)(nil)
var _ linkgraph.EdgeIterator = (*iterator)(nil)

// linkedge iterator
type iterator struct {
	rows    *sqlx.Rows
	lastErr error
}

// Edge implements graph.EdgeIterator.
func (it *iterator) Edge() *linkgraph.Edge {
	var edge linkgraph.Edge
	it.lastErr = it.rows.Scan(&edge.ID, &edge.Src, &edge.Dst, &edge.UpdateAt)
	if it.lastErr != nil {
		return nil
	}

	return &edge
}

// Close implements graph.LinkIterator.
func (it *iterator) Close() error {
	return it.rows.Close()
}

// Error implements graph.LinkIterator.
func (it *iterator) Error() error {
	return it.lastErr
}

// Link implements graph.LinkIterator.
func (it *iterator) Link() *linkgraph.Link {
	var link linkgraph.Link
	it.lastErr = it.rows.Scan(&link.ID, &link.URL, &link.RetrievedAt) //Scan(&link)
	if it.lastErr != nil {
		return nil
	}

	return &link
}

// Next implements graph.LinkIterator.
func (it *iterator) Next() bool {
	ok := it.rows.Next()
	if !ok {
		err := it.rows.Err()
		if err != nil {
			it.lastErr = err
		}
	}
	return ok
}
