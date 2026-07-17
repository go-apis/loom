// Throwaway: serves the orders service console for a screenshot check.
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
)

func main() {
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, "postgres://postgres:mysecret@localhost:5432/postgres")
	if err != nil {
		log.Fatal(err)
	}
	_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS loom_topo_demo WITH (FORCE)")
	if _, err := admin.Exec(ctx, "CREATE DATABASE loom_topo_demo"); err != nil {
		log.Fatal(err)
	}
	admin.Close()

	pool, err := pgxpool.New(ctx, "postgres://postgres:mysecret@localhost:5432/loom_topo_demo")
	if err != nil {
		log.Fatal(err)
	}
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore("/tmp/topodemo-blobs", "http://localhost:8098/files")})
	if err != nil {
		log.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	if err := cli.Start(ctx, time.Second); err != nil {
		log.Fatal(err)
	}
	log.Println("console on http://localhost:8098/console")
	log.Fatal(http.ListenAndServe("localhost:8098", cli.HTTPHandler()))
}
