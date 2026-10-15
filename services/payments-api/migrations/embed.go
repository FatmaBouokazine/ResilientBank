// Package migrations embeds the versioned SQL schema so the migrator is a single
// self-contained binary that cannot be run against a schema different from the one
// compiled into it.
//
// This package exists because go:embed patterns are relative to the package directory
// and cannot reference parent directories. Keeping the embed alongside the .sql files
// means adding a migration never requires touching Go code.
package migrations

import "embed"

// FS contains every migration in filename order. goose derives the ordering from the
// numeric filename prefix, so 00002_ always applies after 00001_.
//
//go:embed *.sql
var FS embed.FS

// Dir is the directory within FS that goose should treat as the migration root. The
// embedded files sit at the root, so this is ".".
const Dir = "."
