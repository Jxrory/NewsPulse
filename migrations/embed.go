package migrations

import "embed"

// FS holds the numbered SQL migrations applied to every annual shard.
//
//go:embed *.sql
var FS embed.FS
