-- Processed transcript stream: idempotent by event_id; separate alert facts.
CREATE TABLE IF NOT EXISTS ops.call_center_processed (
 event_id text PRIMARY KEY, call_id text NOT NULL, sequence bigint NOT NULL,
 event_time timestamptz NOT NULL, transcript text NOT NULL,
 has_alert boolean NOT NULL, matched_pattern text,
 processed_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(call_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_calls_event_time ON ops.call_center_processed(event_time);
CREATE TABLE IF NOT EXISTS ops.call_center_alert (
 event_id text PRIMARY KEY REFERENCES ops.call_center_processed(event_id),
 call_id text NOT NULL,
 event_time timestamptz NOT NULL,
 transcript text NOT NULL,
 matched_pattern text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_call_alerts_time ON ops.call_center_alert(event_time);
