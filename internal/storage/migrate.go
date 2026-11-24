package storage

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jxrory/newspulse/migrations"
)

type migration struct {
	version  int
	name     string
	sql      string
	checksum string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, err
	}
	var out []migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		parts := strings.SplitN(e.Name(), "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("migration %s must start with a numeric prefix", e.Name())
		}
		v, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("migration %s has invalid version: %w", e.Name(), err)
		}
		b, err := migrations.FS.ReadFile(e.Name())
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(b)
		out = append(out, migration{version: v, name: e.Name(), sql: string(b), checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	for i := 1; i < len(out); i++ {
		if out[i].version == out[i-1].version {
			return nil, fmt.Errorf("duplicate migration version %d", out[i].version)
		}
	}
	return out, nil
}

// ApplyMigrations applies each migration exactly once per annual shard and
// reports checksum drift instead of silently rewriting applied SQL.
func ApplyMigrations(s *Shard) error {
	ms, err := loadMigrations()
	if err != nil {
		return err
	}
	applied := map[int]string{}
	var check string
	if err := s.r.QueryRow(`SELECT sqlite_master.type FROM sqlite_master WHERE type='table' AND name='schema_migrations'`).Scan(&check); err == nil {
		rows, err := s.r.Query(`SELECT version, checksum FROM schema_migrations`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var v int
			var sum string
			if err := rows.Scan(&v, &sum); err != nil {
				return err
			}
			applied[v] = sum
		}
		if err := rows.Err(); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	for _, m := range ms {
		if sum, ok := applied[m.version]; ok {
			if sum != m.checksum {
				return fmt.Errorf("migration %d checksum changed (applied %s, embedded %s); shard %d requires an explicit migration plan", m.version, sum, m.checksum, s.Year)
			}
			continue
		}
		tx, err := s.w.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(m.sql); err != nil {
			tx.Rollback()
			return fmt.Errorf("apply %s: %w", m.name, err)
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO schema_migrations(version,checksum,applied_at) VALUES(?,?,?)`,
			m.version, m.checksum, time.Now().Unix()); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit %s: %w", m.name, err)
		}
	}
	return nil
}
