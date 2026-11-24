-- NewsPulse 年度库 schema 草案；尚不是应用迁移器。
-- 归属/跨库查询/事务边界见 IMPLEMENTATION_PLAN.md 第5节。
-- 每个年度文件独立执行；所有 *_at INTEGER 均为 UTC Unix 秒。
-- JSON 字段写入前由应用校验；未在这里依赖可选的 JSON/FTS 扩展。
PRAGMA foreign_keys = ON;

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);

CREATE TABLE sources (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    publisher_id TEXT NOT NULL,
    publisher_group TEXT NOT NULL,
    language TEXT NOT NULL,
    section_hint TEXT NOT NULL,
    timezone_hint TEXT,
    config_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE source_state (
    source_id TEXT PRIMARY KEY REFERENCES sources(id),
    etag TEXT,
    last_modified TEXT,
    last_checked_at INTEGER,
    last_success_at INTEGER,
    latest_published_at INTEGER,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    next_fetch_at INTEGER NOT NULL,
    health TEXT NOT NULL DEFAULT 'unknown',
    last_error_code TEXT,
    updated_at INTEGER NOT NULL
);
CREATE INDEX idx_source_due ON source_state(next_fetch_at);

CREATE TABLE fetch_runs (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id),
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    status TEXT NOT NULL,
    http_status INTEGER,
    response_bytes INTEGER,
    parsed_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    rejected_items_json TEXT NOT NULL DEFAULT '[]',
    error_code TEXT,
    error_message_redacted TEXT
);
CREATE INDEX idx_fetch_source_time ON fetch_runs(source_id, started_at DESC);

CREATE TABLE articles (
    id TEXT PRIMARY KEY,
    identity_version INTEGER NOT NULL DEFAULT 1,
    canonical_url TEXT,
    title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    summary_text TEXT NOT NULL DEFAULT '',
    content_text TEXT NOT NULL DEFAULT '',
    content_hash TEXT NOT NULL,
    language TEXT NOT NULL,
    section TEXT NOT NULL DEFAULT 'unknown',
    publisher_id TEXT NOT NULL,
    publisher_group TEXT NOT NULL,
    published_at_raw TEXT,
    published_at INTEGER,
    updated_at INTEGER,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    date_quality TEXT NOT NULL,
    quality_flags_json TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX idx_articles_published ON articles(published_at DESC, id);
CREATE INDEX idx_articles_seen ON articles(first_seen_at DESC, id);
CREATE INDEX idx_articles_section_date ON articles(section, published_at DESC, id);
CREATE INDEX idx_articles_last_seen ON articles(last_seen_at DESC);

CREATE TABLE article_aliases (
    alias_hash TEXT PRIMARY KEY,
    alias_kind TEXT NOT NULL,
    alias_value TEXT NOT NULL,
    article_id TEXT NOT NULL REFERENCES articles(id),
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_alias_article ON article_aliases(article_id);
-- alias_hash 由应用对完整前缀身份计算 SHA-256；查询时也比较原值防止意外冲突。

CREATE TABLE article_observations (
    id TEXT PRIMARY KEY,
    article_id TEXT NOT NULL REFERENCES articles(id),
    source_id TEXT NOT NULL REFERENCES sources(id),
    source_item_key TEXT NOT NULL,
    guid_raw TEXT,
    url_raw TEXT,
    title_raw TEXT NOT NULL,
    description_raw TEXT,
    content_raw TEXT,
    published_at_raw TEXT,
    publisher_claim_raw TEXT,
    raw_item_json TEXT NOT NULL,
    raw_item_hash TEXT NOT NULL,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    UNIQUE(source_id, source_item_key)
);
CREATE INDEX idx_observation_article ON article_observations(article_id);

CREATE TABLE article_revisions (
    id TEXT PRIMARY KEY,
    article_id TEXT NOT NULL REFERENCES articles(id),
    observed_at INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    snapshot_json TEXT NOT NULL
);
CREATE INDEX idx_revision_article_time ON article_revisions(article_id, observed_at);
-- 正文内容回退到旧版本也可以产生新revision，因此不对content_hash作唯一约束。

CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    job_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    schedule_id TEXT,
    scheduled_at INTEGER NOT NULL,
    window_start INTEGER,
    window_end INTEGER,
    cutoff_at INTEGER,
    generation_deadline INTEGER,
    expires_at INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER,
    lease_until INTEGER,
    heartbeat_at INTEGER,
    config_snapshot_json TEXT NOT NULL,
    candidate_snapshot_json TEXT,
    last_error_code TEXT,
    last_error_redacted TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX idx_jobs_due ON jobs(status, next_attempt_at);
CREATE INDEX idx_jobs_schedule ON jobs(schedule_id, scheduled_at DESC);

CREATE TABLE analysis_cache (
    cache_key TEXT PRIMARY KEY,
    input_hash TEXT NOT NULL,
    input_refs_json TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    output_schema_version INTEGER NOT NULL,
    output_json TEXT NOT NULL,
    usage_json TEXT NOT NULL DEFAULT '{}',
    cost_estimate_micros INTEGER,
    currency TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER
);
CREATE INDEX idx_analysis_created ON analysis_cache(created_at);
-- 只有验证成功的输出可进入缓存；预算还要计算失败/重试调用，见下一表。

CREATE TABLE llm_calls (
    id TEXT PRIMARY KEY,
    job_id TEXT REFERENCES jobs(id),
    requested_at INTEGER NOT NULL,
    finished_at INTEGER,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    reserved_input_tokens INTEGER NOT NULL DEFAULT 0,
    reserved_output_tokens INTEGER NOT NULL DEFAULT 0,
    usage_json TEXT NOT NULL DEFAULT '{}',
    cost_estimate_micros INTEGER,
    currency TEXT,
    error_code TEXT
);
CREATE INDEX idx_llm_calls_time ON llm_calls(requested_at);
-- 发起请求前先持久化预算预留；超时且无usage时按预留计，不能视为免费。
-- job关联的调用保存在job所属库；每日预算按requested_at跨全部年度库汇总。

CREATE TABLE digests (
    id TEXT PRIMARY KEY,
    job_id TEXT REFERENCES jobs(id),
    slot_key TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    schedule_id TEXT,
    scheduled_at INTEGER NOT NULL,
    window_start INTEGER NOT NULL,
    window_end INTEGER NOT NULL,
    cutoff_at INTEGER NOT NULL,
    title TEXT NOT NULL,
    markdown TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    generation_mode TEXT NOT NULL,
    model_metadata_json TEXT NOT NULL DEFAULT '{}',
    source_health_json TEXT NOT NULL,
    quality_flags_json TEXT NOT NULL DEFAULT '[]',
    created_at INTEGER NOT NULL,
    UNIQUE(slot_key, revision)
);
CREATE INDEX idx_digest_schedule ON digests(scheduled_at DESC, id);

CREATE TABLE digest_items (
    digest_id TEXT NOT NULL REFERENCES digests(id),
    ordinal INTEGER NOT NULL,
    section TEXT NOT NULL CHECK(section IN ('international', 'domestic', 'status')),
    event_key TEXT NOT NULL,
    title_zh TEXT NOT NULL,
    facts_json TEXT NOT NULL,
    background TEXT NOT NULL DEFAULT '',
    impact TEXT NOT NULL DEFAULT '',
    importance INTEGER CHECK(importance BETWEEN 0 AND 100),
    coverage TEXT NOT NULL,
    article_refs_json TEXT NOT NULL,
    evidence_snapshot_json TEXT NOT NULL,
    quality_flags_json TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY(digest_id, ordinal)
);
-- article_refs_json = [{"year":2026,"id":"..."}, ...]；不能添加跨文件外键。

CREATE TABLE notification_outbox (
    id TEXT PRIMARY KEY,
    digest_id TEXT NOT NULL REFERENCES digests(id),
    dedup_key TEXT NOT NULL UNIQUE,
    provider TEXT NOT NULL,
    recipient_alias TEXT NOT NULL,
    part_no INTEGER NOT NULL DEFAULT 1,
    resend_revision INTEGER NOT NULL DEFAULT 0,
    resend_reason TEXT,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    send_attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER,
    lease_until INTEGER,
    expires_at INTEGER NOT NULL,
    provider_response_redacted_json TEXT,
    sent_at INTEGER,
    last_error_code TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(digest_id, provider, recipient_alias, part_no, resend_revision)
);
CREATE INDEX idx_outbox_due ON notification_outbox(status, next_attempt_at);
-- payload_json只存稳定业务消息，不含Webhook URL、签名密钥、timestamp/sign。
-- 发送时重新签名并检查最终请求体大小；sent仅指API报告成功，不代表群成员已读。
-- resend_revision=0为自动发送；显式重发时dedup_key也包含resend_revision。

CREATE TABLE notification_attempts (
    id TEXT PRIMARY KEY,
    outbox_id TEXT NOT NULL REFERENCES notification_outbox(id),
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    http_status INTEGER,
    provider_code TEXT,
    result TEXT NOT NULL,
    error_code TEXT,
    response_redacted_json TEXT
);
CREATE INDEX idx_attempt_outbox ON notification_attempts(outbox_id, started_at);
