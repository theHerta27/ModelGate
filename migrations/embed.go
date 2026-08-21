package migrations

import "embed"

// FS contains the ordered SQL migrations applied when PostgreSQL is enabled.
//
//go:embed *.sql
var FS embed.FS
