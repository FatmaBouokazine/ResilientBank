// Command migrate applies, rolls back, and reports on the PostgreSQL schema.
//
// WHY THIS EXISTS:
// Schema changes previously lived in docker-entrypoint-initdb.d, which the Postgres
// image only executes when the data volume is empty. A developer who ran the stack
// during Phase 1 already had a populated `payments` table with no `outbox` table, and
// pulling a branch containing a new migration gave them a service that failed at
// runtime against a database nobody could repair without deleting their data.
//
// A versioned runner fixes that: migrations are embedded in this binary, applied in
// order, and recorded in goose's goose_db_version table so each runs exactly once.
//
// Usage:
//
//	go run ./cmd/migrate up       apply all pending migrations
//	go run ./cmd/migrate down     roll back the most recent migration
//	go run ./cmd/migrate status   list applied and pending migrations
//	go run ./cmd/migrate version  print the current database version
//
// Configuration is read from DATABASE_URL.
package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/pressly/goose/v3"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver used by database/sql

	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/migrations"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := run(os.Args[1:]); err != nil {
		logger.Error("migration failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing command: expected one of up, down, status, version")
	}

	command := args[0]

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://resilient:resilient@localhost:5432/payments?sslmode=disable"
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("connect to %s: %w (is the database running? try: make up)", redact(databaseURL), err)
	}

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}

	switch command {
	case "up":
		// goose's own progress output is suppressed; the summary line below reports the
		// outcome instead, and `migrate status` shows the individual migrations.
		goose.SetLogger(goose.NopLogger())

		before, err := goose.GetDBVersion(db)
		if err != nil {
			return fmt.Errorf("read version: %w", err)
		}

		if err := goose.Up(db, migrations.Dir); err != nil {
			return fmt.Errorf("apply migrations: %w", err)
		}

		after, err := goose.GetDBVersion(db)
		if err != nil {
			return fmt.Errorf("read version: %w", err)
		}

		switch {
		case after == before:
			fmt.Printf("schema already up to date at version %d\n", after)
		case after > before:
			fmt.Printf("applied %d migration(s): version %d -> %d\n", after-before, before, after)
		default:
			// Unreachable via goose.Up, which only ever moves forward. Reported rather
			// than silently swallowed so an unexpected future change is visible.
			fmt.Printf("WARNING: version moved backwards, %d -> %d\n", before, after)
		}
		return nil

	case "down":
		goose.SetLogger(goose.NopLogger())
		// Rolls back exactly one migration. "goose reset" is intentionally not exposed
		// here: it would drop and recreate the schema, which on a developer's machine
		// means silently deleting real payment history.
		if err := goose.Down(db, migrations.Dir); err != nil {
			return fmt.Errorf("roll back migration: %w", err)
		}
		return nil

	case "status":
		// The default logger is required here: it is what actually prints the table of
		// applied and pending migrations.
		if err := goose.Status(db, migrations.Dir); err != nil {
			return fmt.Errorf("migration status: %w", err)
		}
		return nil

	case "version":
		version, err := goose.GetDBVersion(db)
		if err != nil {
			return fmt.Errorf("read version: %w", err)
		}
		fmt.Printf("database version: %d\n", version)
		return nil

	default:
		return fmt.Errorf("unknown command %q: expected one of up, down, status, version", command)
	}
}

// redact removes any password from a connection string before it reaches an error
// message or a log line.
func redact(databaseURL string) string {
	at := strings.LastIndex(databaseURL, "@")
	scheme := strings.Index(databaseURL, "://")
	if scheme < 0 || at < 0 || at < scheme {
		return databaseURL
	}
	credentials := databaseURL[scheme+3 : at]
	if colon := strings.Index(credentials, ":"); colon >= 0 {
		return databaseURL[:scheme+3] + credentials[:colon] + ":***" + databaseURL[at:]
	}
	return databaseURL
}
