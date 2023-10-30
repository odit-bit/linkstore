package postgregraph

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

//================

const edgeRemoveStaleQuery = `
	DELETE FROM edges 
	WHERE src=$1 and update_at < $2
`

// RemoveStaleEdges implements graph.Graph.
func (p *postgre) RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error {
	_, err := p.db.ExecContext(context.TODO(), edgeRemoveStaleQuery, fromID, updatedBefore.UTC())
	if err != nil {
		return fmt.Errorf("remove stale edge: %v", err)
	}

	return nil
}
