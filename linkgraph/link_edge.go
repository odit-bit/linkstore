package linkgraph

import (
	"time"

	"github.com/google/uuid"
)

// Link represent the set of web pages that have been processed or discovered
// by the crawler component
type Link struct {
	// unique identifier for link
	ID uuid.UUID

	// link target
	URL string `db:"url"`

	// timestamp when link retrieved after processed
	RetrievedAt time.Time `db:"retrieved_at"`
}

// Edge represents a uni-directional connection between two links in the graph.
// describe graph edge that originate from src and endup at dst,
type Edge struct {
	//unique identifier
	ID uuid.UUID

	Src uuid.UUID //Link ID

	Dst uuid.UUID // Link ID

	// timestamp when link is update
	UpdateAt time.Time `db:"update_at"`
}
