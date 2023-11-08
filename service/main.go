package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/odit-bit/linkstore"
	"github.com/odit-bit/linkstore/linkpostgre"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	mainCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn, ok := os.LookupEnv("DSN")
	if !ok {
		if dsn == "" {
			slog.Error("DSN var is nil")
			os.Exit(2)
		}
	}
	exporterHost, ok := os.LookupEnv("OTEL_EXPORTER_HOST")
	if !ok {
		if exporterHost == "" {
			slog.Error("otel exporter var not set")
			os.Exit(2)
		}
	}

	dbConn, err := connectPGWithOTEL(dsn)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(2)
	}
	db := linkpostgre.New(dbConn)

	//setup exporter connection

	exporter, err := newGrpcExporter(mainCtx, exporterHost)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(2)
	}

	shutdownFunc, err := setupOTelSDK(mainCtx, "graph-db", "0.0.1", exporter)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(2)
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, shutdownFunc(mainCtx))
	}()

	var wg sync.WaitGroup
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-mainCtx.Done():
		case <-sig:
			cancel()
		}
	}()

	// setup service server
	srv := linkstore.Server{
		Port:    8181,
		Handler: db,
	}

	err = srv.ListenAndServe()
	if err != nil {
		slog.Error(err.Error())
	}
	wg.Wait()
	slog.Info("exit graph server")
}

func connectPG(dsn string) (*sqlx.DB, error) {
	//IMPORT !!
	// _ "github.com/jackc/pgx/v5/stdlib"

	// DSN format
	// "host= dbname= password= user="

	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func connectPGWithOTEL(dsn string) (*sqlx.DB, error) {

	db, err := otelsqlx.Connect("pgx", dsn)

	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func newGrpcExporter(ctx context.Context, host string) (sdktrace.SpanExporter, error) {

	nCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var opts []otlptracegrpc.Option

	opts = append(opts, otlptracegrpc.WithInsecure())
	opts = append(opts, otlptracegrpc.WithEndpoint(host))

	exp, err := otlptracegrpc.New(nCtx, opts...)
	if err != nil {
		return nil, err
	}
	return exp, nil

}
