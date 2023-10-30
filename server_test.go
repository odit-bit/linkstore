package linkstore

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/odit-bit/linkstore/api"
	"github.com/odit-bit/linkstore/linkgraph"
	postgregraph "github.com/odit-bit/linkstore/postgre"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Test_server(t *testing.T) {

	db := postgregraph.New(connectPG())
	linkServer := NewServer(db)

	grpcServer := grpc.NewServer()
	api.RegisterLinkGraphServer(grpcServer, linkServer)

	listen, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 8181))
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	var wg sync.WaitGroup

	//server setup
	wg.Add(1)
	go func() {
		defer wg.Done()
		go grpcServer.Serve(listen)
		<-ctx.Done()
		grpcServer.GracefulStop()

	}()

	//client setup
	conn, err := grpc.Dial("localhost:8181", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		t.Fatal(err)
	}

	cli, err := NewClient(ctx, conn)
	if err != nil {
		cancel()
		t.Error(err)
	}

	// test upsert
	l := &linkgraph.Link{
		// ID:          uuid.New(),
		URL: "www.example1.com",
		// RetrievedAt: time.Now(),
	}

	if err := cli.UpsertLink(l); err != nil {
		cancel()
		t.Fatal(err)
	}

	cancel()
	wg.Wait()
}

func connectPG() *sqlx.DB {
	db := sqlx.MustOpen("pgx", "host=localhost dbname=postgres password=test user=postgres")
	err := db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	return db
}
