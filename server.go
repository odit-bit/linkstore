package linkstore

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"github.com/odit-bit/linkstore/api"
	"github.com/odit-bit/linkstore/linkgraph"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ api.LinkGraphServer = (*ApiServer)(nil)

type ApiServer struct {
	api.UnimplementedLinkGraphServer
	g linkgraph.Graph
}

func NewServer(graph linkgraph.Graph) *ApiServer {
	srv := ApiServer{
		UnimplementedLinkGraphServer: api.UnimplementedLinkGraphServer{},
		g:                            graph,
	}
	return &srv
}

// Edges implements api.LinkGraphServer.
func (srv *ApiServer) Edges(idRange *api.Range, w api.LinkGraph_EdgesServer) error {
	updateBefore := idRange.Filter.AsTime()

	from, err := uuid.FromBytes(idRange.FromUuid)
	if err != nil {
		return err
	}
	to, err := uuid.FromBytes(idRange.ToUuid)
	if err != nil {
		return err
	}

	it, err := srv.g.Edges(from, to, updateBefore)
	if err != nil {
		_ = it.Close()
		return err
	}
	defer func() { _ = it.Close() }()

	for it.Next() {
		edge := it.Edge()
		msg := &api.Edge{
			Uuid:      edge.ID[:],
			SrcUuid:   edge.Src[:],
			DstUuid:   edge.Dst[:],
			UpdatedAt: timestamppb.New(edge.UpdateAt),
		}

		if err := w.Send(msg); err != nil {
			_ = it.Close()
			return err
		}

	}

	if err := it.Error(); err != nil {
		return err
	}

	return it.Close()
}

// RemoveStaleEdges implements api.LinkGraphServer.
func (srv *ApiServer) RemoveStaleEdges(ctx context.Context, req *api.RemoveStaleEdgesQuery) (*emptypb.Empty, error) {
	updatedBefore := req.UpdatedBefore.AsTime() //ptypes.Timestamp(req.UpdatedBefore)

	err := srv.g.RemoveStaleEdges(
		uuidFromBytes(req.FromUuid),
		updatedBefore,
	)

	return new(empty.Empty), err
}

// UpsertEdge implements api.LinkGraphServer.
func (srv *ApiServer) UpsertEdge(ctx context.Context, req *api.Edge) (*api.Edge, error) {
	edge := linkgraph.Edge{
		ID:  uuidFromBytes(req.Uuid),
		Src: uuidFromBytes(req.SrcUuid),
		Dst: uuidFromBytes(req.DstUuid),
	}

	if err := srv.g.UpsertEdge(&edge); err != nil {
		return nil, err
	}

	req.Uuid = edge.ID[:]
	req.SrcUuid = edge.Src[:]
	req.DstUuid = edge.Dst[:]
	req.UpdatedAt = timestamppb.New(edge.UpdateAt)
	return req, nil
}

// UpsertLink implements api.LinkGraphServer.
func (srv *ApiServer) UpsertLink(ctx context.Context, req *api.Link) (*api.Link, error) {
	var (
		err  error
		link = linkgraph.Link{
			ID:  uuidFromBytes(req.Uuid),
			URL: req.Url,
		}
	)

	link.RetrievedAt = req.RetrievedAt.AsTime()
	if err = srv.g.UpsertLink(&link); err != nil {
		return nil, err
	}

	req.RetrievedAt = timestamppb.New(link.RetrievedAt) //timeToProto(link.RetrievedAt)
	req.Url = link.URL
	req.Uuid = link.ID[:]

	return req, nil
}

func (srv *ApiServer) Links(idRange *api.Range, w api.LinkGraph_LinksServer) error {
	accessedBefore := idRange.Filter.AsTime()

	from, err := uuid.FromBytes(idRange.FromUuid)
	if err != nil {
		return err
	}
	to, err := uuid.FromBytes(idRange.ToUuid)
	if err != nil {
		return err
	}

	it, err := srv.g.Links(from, to, accessedBefore)
	if err != nil {
		return err
	}

	defer func() { _ = it.Close() }()

	for it.Next() {
		link := it.Link()
		msg := api.Link{
			Uuid:        link.ID[:],
			Url:         link.URL,
			RetrievedAt: timestamppb.New(link.RetrievedAt),
		}

		if err := w.Send(&msg); err != nil {
			_ = it.Close()
			return err
		}
	}

	if err := it.Error(); err != nil {
		return err
	}

	return it.Close()
}
