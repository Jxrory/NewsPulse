package storage

import (
	"context"
	"database/sql"
	"errors"
)

type LLMCallReserve struct {
	ID                   string
	JobID                string
	RequestedAt          int64
	Provider             string
	Model                string
	InputHash            string
	Status               string
	ReservedInputTokens  int
	ReservedOutputTokens int
}

func (s *Shard) ReserveLLMCall(ctx context.Context, c *LLMCallReserve) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	id, err := NewID()
	if err != nil {
		return err
	}
	c.ID = id
	_, err = s.w.ExecContext(ctx, `INSERT INTO llm_calls(id, shard_year, job_id, requested_at,
		provider, model, input_hash, status, reserved_input_tokens, reserved_output_tokens)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		c.ID, s.Year, nullable(c.JobID), c.RequestedAt, c.Provider, c.Model,
		c.InputHash, c.Status, c.ReservedInputTokens, c.ReservedOutputTokens)
	return err
}

func (s *Shard) FinishLLMCall(ctx context.Context, id, status string, usageJSON string,
	inputTokens, outputTokens int, costMicros *int64, currency, errCode string) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	_, err := s.w.ExecContext(ctx, `UPDATE llm_calls SET finished_at=?, status=?, usage_json=?,
		reserved_input_tokens=MAX(reserved_input_tokens, ?),
		reserved_output_tokens=MAX(reserved_output_tokens, ?),
		cost_estimate_micros=?, currency=?, error_code=? WHERE id=?`,
		nowUnix(), status, usageJSON, inputTokens, outputTokens,
		sqlNullInt64(costMicros), nullable(currency), nullable(errCode), id)
	return err
}

// DailyLLMSpend aggregates reservation rows across every shard because a
// budget must not reset when a new annual file appears.
func (m *ShardManager) DailyLLMSpend(ctx context.Context, dayStart, dayEnd int64) (LLMUsageSummary, error) {
	shards, err := m.AllShards()
	if err != nil {
		return LLMUsageSummary{}, err
	}
	var total LLMUsageSummary
	for _, s := range shards {
		var reqs, in, out, cost int64
		err := s.r.QueryRowContext(ctx, `SELECT count(*),
			COALESCE(SUM(reserved_input_tokens),0), COALESCE(SUM(reserved_output_tokens),0),
			COALESCE(SUM(cost_estimate_micros),0)
			FROM llm_calls WHERE requested_at >= ? AND requested_at < ?`, dayStart, dayEnd).
			Scan(&reqs, &in, &out, &cost)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return total, err
		}
		total.Requests += reqs
		total.InputTokens += in
		total.OutputTokens += out
		total.CostMicros += cost
	}
	return total, nil
}

type CachedAnalysis struct {
	CacheKey   string
	Provider   string
	Model      string
	OutputJSON string
	UsageJSON  string
	CostMicros *int64
}

func (s *Shard) GetAnalysisCache(ctx context.Context, key string) (*CachedAnalysis, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	var c CachedAnalysis
	var cost sql.NullInt64
	err := s.r.QueryRowContext(ctx, `SELECT cache_key, provider, model, output_json, usage_json, cost_estimate_micros
		FROM analysis_cache WHERE cache_key=?`, key).Scan(&c.CacheKey, &c.Provider, &c.Model, &c.OutputJSON, &c.UsageJSON, &cost)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.CostMicros = int64Ptr(cost)
	return &c, nil
}

func (s *Shard) PutAnalysisCache(ctx context.Context, c CachedAnalysis, inputHash, refsJSON, provider,
	model, promptVersion string, schemaVersion int, currency string, now int64) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	_, err := s.w.ExecContext(ctx, `INSERT OR REPLACE INTO analysis_cache(
		cache_key, input_hash, input_refs_json, provider, model, prompt_version,
		output_schema_version, output_json, usage_json, cost_estimate_micros,
		currency, created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		c.CacheKey, inputHash, refsJSON, provider, model, promptVersion,
		schemaVersion, c.OutputJSON, c.UsageJSON, sqlNullInt64(c.CostMicros),
		nullable(currency), now)
	return err
}

var _ = errors.Is
