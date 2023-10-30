package linkstore

import (
	"context"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/odit-bit/linkstore/api"
	"github.com/odit-bit/linkstore/linkgraph"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ linkgraph.Graph = (*apiClient)(nil)

type apiClient struct {
	ctx context.Context
	lgc api.LinkGraphClient
}

// New returns a new client instance that implements a subset
// of the linkgraph.Graph interface by delegating methods to a graph instance
// exposed by a remote gRPC sever.
func NewClient(ctx context.Context, clientConn grpc.ClientConnInterface) (*apiClient, error) {
	lgClient := api.NewLinkGraphClient(clientConn)

	linkCli := apiClient{
		ctx: ctx,
		lgc: lgClient,
	}

	return &linkCli, nil
}

// Edges implements linkgraph.Graph.
func (cli *apiClient) Edges(fromID uuid.UUID, toID uuid.UUID, updateBefore time.Time) (linkgraph.EdgeIterator, error) {
	r := api.Range{
		FromUuid: fromID[:],
		ToUuid:   toID[:],
		Filter:   timestamppb.New(updateBefore),
	}

	ctx, cancel := context.WithCancel(cli.ctx)
	stream, err := cli.lgc.Edges(ctx, &r)
	if err != nil {
		cancel()
		return nil, err
	}
	return &edgeIterator{
		stream:   stream,
		edge:     nil,
		err:      nil,
		cancelFn: cancel,
	}, nil
}

// Links implements linkgraph.Graph.
func (cli *apiClient) Links(fromID uuid.UUID, toID uuid.UUID, retrieveBefore time.Time) (linkgraph.LinkIterator, error) {
	ctx, cancel := context.WithCancel(cli.ctx)
	r := api.Range{
		FromUuid: fromID[:],
		ToUuid:   toID[:],
		Filter:   timestamppb.New(retrieveBefore),
	}
	stream, err := cli.lgc.Links(ctx, &r)
	if err != nil {
		cancel()
		return nil, err
	}

	//make linkIterator instance
	return &linkIterator{
		stream:   stream,
		link:     nil,
		err:      nil,
		cancelFn: cancel,
	}, nil
}

// RemoveStaleEdges implements linkgraph.Graph.
func (cli *apiClient) RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error {
	ctx, cancel := context.WithCancel(cli.ctx)
	defer cancel()
	_, err := cli.lgc.RemoveStaleEdges(ctx, &api.RemoveStaleEdgesQuery{
		FromUuid:      fromID[:],
		UpdatedBefore: timestamppb.New(updatedBefore),
	})
	if err != nil {
		return err
	}
	return nil
}

// UpsertEdge implements linkgraph.Graph.
func (cli *apiClient) UpsertEdge(edge *linkgraph.Edge) error {

	rpcEdge, err := cli.lgc.UpsertEdge(cli.ctx, &api.Edge{
		Uuid:      edge.ID[:],
		SrcUuid:   edge.Src[:],
		DstUuid:   edge.Dst[:],
		UpdatedAt: timestamppb.New(edge.UpdateAt),
	})

	if err != nil {
		return err
	}

	edge.UpdateAt = rpcEdge.UpdatedAt.AsTime()
	return nil
}

// UpsertLink implements linkgraph.Graph.
func (cli *apiClient) UpsertLink(link *linkgraph.Link) error {

	rpcLink, err := cli.lgc.UpsertLink(cli.ctx, &api.Link{
		Uuid:        link.ID[:],
		Url:         link.URL,
		RetrievedAt: timestamppb.New(link.RetrievedAt),
	})

	if err != nil {
		return err
	}
	link.ID = uuid.UUID(rpcLink.Uuid)
	link.RetrievedAt = rpcLink.RetrievedAt.AsTime()
	return nil
}

//======== link iterator

var _ linkgraph.LinkIterator = (*linkIterator)(nil)

type linkIterator struct {
	stream api.LinkGraph_LinksClient

	// current retreived link
	link *linkgraph.Link

	// current error
	err error

	// A function to cancel the context used to perform the streaming RPC. It
	// allows us to abort server-streaming calls from the client side.
	cancelFn func() // context.CancelFunc
}

// Close implements linkgraph.LinkIterator.
func (it *linkIterator) Close() error {
	return it.stream.CloseSend()
}

// Error implements linkgraph.LinkIterator.
func (it *linkIterator) Error() error {
	return it.err
}

// Link implements linkgraph.LinkIterator.
func (it *linkIterator) Link() *linkgraph.Link {
	return it.link
}

// Next implements linkgraph.LinkIterator.
func (it *linkIterator) Next() bool {
	rpcLink, err := it.stream.Recv()
	if err != nil {

		//stream will return EOF if no more data to received
		if err != io.EOF {
			it.err = err
		}
		it.cancelFn()
		return false
	}
	it.link = &linkgraph.Link{
		ID:          uuidFromBytes(rpcLink.Uuid),
		URL:         rpcLink.Url,
		RetrievedAt: rpcLink.RetrievedAt.AsTime(),
	}

	return true
}

//======== edge iterator

var _ linkgraph.EdgeIterator = (*edgeIterator)(nil)

type edgeIterator struct {
	stream api.LinkGraph_EdgesClient

	// current retreived link
	edge *linkgraph.Edge

	// current error
	err error

	// A function to cancel the context used to perform the streaming RPC. It
	// allows us to abort server-streaming calls from the client side.
	cancelFn func() // context.CancelFunc
}

// Close implements linkgraph.EdgeIterator.
func (it *edgeIterator) Close() error {
	return it.stream.CloseSend()
}

// Edge implements linkgraph.EdgeIterator.
func (it *edgeIterator) Edge() *linkgraph.Edge {
	return it.edge
}

// Error implements linkgraph.EdgeIterator.
func (it *edgeIterator) Error() error {
	return it.err
}

// Next implements linkgraph.EdgeIterator.
func (it *edgeIterator) Next() bool {
	rpcEdge, err := it.stream.Recv()
	if err != nil {

		//stream will return EOF if no more data to received
		if err != io.EOF {
			it.err = err
		}
		it.cancelFn()
		return false
	}
	it.edge = &linkgraph.Edge{
		ID:       uuid.UUID(rpcEdge.Uuid),
		Src:      uuid.UUID(rpcEdge.SrcUuid),
		Dst:      uuid.UUID(rpcEdge.DstUuid),
		UpdateAt: rpcEdge.UpdatedAt.AsTime(),
	}

	return true
}

func uuidFromBytes(b []byte) uuid.UUID {
	if len(b) != 16 {
		return uuid.Nil
	}

	var dst uuid.UUID
	copy(dst[:], b)
	return dst
}
