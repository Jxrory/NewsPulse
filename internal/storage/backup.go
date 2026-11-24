package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jxrory/newspulse/internal/config"
)

type BackupFile struct {
	Name          string `json:"name"`
	Year          int    `json:"year"`
	Size          int64  `json:"size"`
	SHA256        string `json:"sha256"`
	SchemaVersion int64  `json:"schema_version"`
}

type BackupManifest struct {
	CreatedAt      string       `json:"created_at"`
	Unix           int64        `json:"unix"`
	AppVersion     string       `json:"app_version"`
	AnnualTimezone string       `json:"annual_timezone"`
	Files          []BackupFile `json:"files"`
	Dir            string       `json:"dir"`
}

// CreateBackup pauses application writes, snapshots every annual file with
// VACUUM INTO (including WAL content), verifies checksums and atomically
// publishes the backup set. It never copies a live .sqlite file directly.
func (m *ShardManager) CreateBackup(ctx context.Context, appVersion string, now time.Time, retention config.BackupConfig) (*BackupManifest, error) {
	m.writeGate.Lock()
	defer m.writeGate.Unlock()

	shards, err := m.AllShards()
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, errors.New("no annual shards to back up")
	}
	backupsDir := filepath.Join(m.dataDir, "backups")
	if err := os.MkdirAll(backupsDir, 0o700); err != nil {
		return nil, err
	}
	name := "backup-" + now.UTC().Format("20060102T150405Z")
	tmpDir := filepath.Join(backupsDir, ".tmp-"+name)
	if err := os.MkdirAll(tmpDir, 0o700); err != nil {
		return nil, err
	}
	published := false
	defer func() {
		if !published {
			os.RemoveAll(tmpDir)
		}
	}()

	manifest := &BackupManifest{
		CreatedAt:      now.UTC().Format(time.RFC3339),
		Unix:           now.Unix(),
		AppVersion:     appVersion,
		AnnualTimezone: m.cfg.App.AnnualTimezone,
	}
	for _, s := range shards {
		target := filepath.Join(tmpDir, filepath.Base(s.Path))
		quoted := strings.ReplaceAll(target, "'", "''")
		if _, err := s.w.ExecContext(ctx, "VACUUM INTO '"+quoted+"'"); err != nil {
			return nil, fmt.Errorf("snapshot shard %d: %w", s.Year, err)
		}
		info, err := os.Stat(target)
		if err != nil {
			return nil, err
		}
		sum, err := fileSHA256(target)
		if err != nil {
			return nil, err
		}
		var schemaVersion int64
		_ = s.r.QueryRowContext(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&schemaVersion)
		manifest.Files = append(manifest.Files, BackupFile{
			Name: filepath.Base(target), Year: s.Year, Size: info.Size(),
			SHA256: sum, SchemaVersion: schemaVersion,
		})
	}
	if len(manifest.Files) == 0 {
		return nil, errors.New("backup produced no files")
	}
	mb, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(tmpDir, "manifest.json")
	if err := os.WriteFile(manifestPath, mb, 0o600); err != nil {
		return nil, err
	}
	finalDir := filepath.Join(backupsDir, name)
	if err := os.Rename(tmpDir, finalDir); err != nil {
		return nil, err
	}
	published = true
	manifest.Dir = finalDir
	if err := pruneBackups(backupsDir, retention); err != nil {
		return manifest, fmt.Errorf("backup created but retention pruning failed: %w", err)
	}
	return manifest, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func pruneBackups(backupsDir string, r config.BackupConfig) error {
	entries, err := os.ReadDir(backupsDir)
	if err != nil {
		return err
	}
	type backupInfo struct {
		path string
		t    time.Time
	}
	var infos []backupInfo
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "backup-") {
			continue
		}
		t, err := time.Parse("backup-20060102T150405Z", e.Name())
		if err != nil {
			continue
		}
		infos = append(infos, backupInfo{filepath.Join(backupsDir, e.Name()), t})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].t.After(infos[j].t) })
	keep := map[string]bool{}
	daily, weekly, monthly := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, info := range infos {
		d := info.t.UTC().Format("2006-01-02")
		y, w := info.t.UTC().ISOWeek()
		wk := fmt.Sprintf("%04d-%02d", y, w)
		mo := info.t.UTC().Format("2006-01")
		okDaily := r.KeepDaily > 0 && len(daily) < r.KeepDaily && !daily[d]
		okWeekly := r.KeepWeekly > 0 && len(weekly) < r.KeepWeekly && !weekly[wk]
		okMonthly := r.KeepMonthly > 0 && len(monthly) < r.KeepMonthly && !monthly[mo]
		if okDaily {
			daily[d] = true
		}
		if okWeekly {
			weekly[wk] = true
		}
		if okMonthly {
			monthly[mo] = true
		}
		if okDaily || okWeekly || okMonthly || (r.KeepDaily == 0 && r.KeepWeekly == 0 && r.KeepMonthly == 0 && len(keep) < 5) {
			keep[info.path] = true
		}
	}
	for _, info := range infos {
		if !keep[info.path] {
			if err := os.RemoveAll(info.path); err != nil {
				return err
			}
		}
	}
	return nil
}
