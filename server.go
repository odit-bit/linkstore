package linkstore

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"github.com/odit-bit/linkstore/api"
	"github.com/odit-bit/linkstore/linkgraph"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	Port    int
	Handler linkgraph.Graph
}

func (srv *Server) ListenAndServe() error {
	linkServer := NewServer(srv.Handler)

	grpcServer := grpc.NewServer()
	api.RegisterLinkGraphServer(grpcServer, linkServer)

	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", srv.Port))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("listen on :", listen.Addr().String())

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	//server setup
	wg.Add(1)
	go func() {
		defer wg.Done()
		grpcServer.Serve(listen)

	}()

	select {
	case <-ctx.Done():
	case <-sigC:
		cancel()
	}

	grpcServer.GracefulStop()

	wg.Wait()
	return nil
}

//================

var _ api.LinkGraphServer = (*GraphServer)(nil)

type GraphServer struct {
	api.UnimplementedLinkGraphServer
	g linkgraph.Graph
}

func NewServer(graph linkgraph.Graph) *GraphServer {

	srv := GraphServer{
		UnimplementedLinkGraphServer: api.UnimplementedLinkGraphServer{},
		g:                            graph,
	}
	return &srv
}

// Edges implements api.LinkGraphServer.
func (srv *GraphServer) Edges(idRange *api.Range, w api.LinkGraph_EdgesServer) error {
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
func (srv *GraphServer) RemoveStaleEdges(ctx context.Context, req *api.RemoveStaleEdgesQuery) (*emptypb.Empty, error) {
	updatedBefore := req.UpdatedBefore.AsTime() //ptypes.Timestamp(req.UpdatedBefore)

	err := srv.g.RemoveStaleEdges(
		uuidFromBytes(req.FromUuid),
		updatedBefore,
	)

	return new(empty.Empty), err
}

// UpsertEdge implements api.LinkGraphServer.
func (srv *GraphServer) UpsertEdge(ctx context.Context, req *api.Edge) (*api.Edge, error) {
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
func (srv *GraphServer) UpsertLink(ctx context.Context, req *api.Link) (*api.Link, error) {
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

func (srv *GraphServer) Links(idRange *api.Range, w api.LinkGraph_LinksServer) error {
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
