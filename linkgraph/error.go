package linkgraph

import "fmt"

var ErrNotFound = fmt.Errorf("not found")
var ErrUnknownEdgeLinks = fmt.Errorf("unknown edges's link src or dst")
