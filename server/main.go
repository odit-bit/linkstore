package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/odit-bit/linkstore"
	"github.com/odit-bit/linkstore/api"
	postgregraph "github.com/odit-bit/linkstore/postgre"
	"google.golang.org/grpc"
)

func main() {
	dsn := os.Getenv("DSN")
	if dsn == "" {
		log.Println("DSN var is nil")
		return
	}
	db := postgregraph.New(connectPG(dsn))
	linkServer := linkstore.NewServer(db)

	grpcServer := grpc.NewServer()
	api.RegisterLinkGraphServer(grpcServer, linkServer)

	listen, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 8989))
	if err != nil {
		log.Fatal(err)
	}

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
	fmt.Println("rpc server shutdown")
}

func connectPG(dsn string) *sqlx.DB {
	// "host=localhost dbname=postgres password=test user=postgres"
	db := sqlx.MustOpen("pgx", dsn)
	err := db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	return db
}
