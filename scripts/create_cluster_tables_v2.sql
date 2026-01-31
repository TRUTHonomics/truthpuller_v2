-- ============================================================================
-- INSTRUCTIONS:
-- Dit script recreëert de cluster tabellen met nieuwe structuur.
-- 
-- Voer uit via SSH tunnel:
--   1. Start tunnel: ssh -i "F:\Containers\.ssh\id_rsa" -N -L 15432:localhost:5432 bart@10.10.10.1
--   2. Voer uit: psql -h localhost -p 15432 -U postgres -d KFLhyper -f scripts/create_cluster_tables_v2.sql
--
-- OF via pgAdmin/DBeaver op localhost:15432
-- ============================================================================

-- Drop oude tabellen (cascade verwijdert ook cluster_posts door FK)
DROP TABLE IF EXISTS truth.cluster_posts CASCADE;
DROP TABLE IF EXISTS truth.post_clusters CASCADE;

-- ============================================================================
-- truth.post_clusters - Hoofdtabel voor clusters
-- ============================================================================
CREATE TABLE truth.post_clusters (
    -- Identifiers
    cluster_id SERIAL PRIMARY KEY,
    cluster_group VARCHAR(50),              -- Cross-interval groupering (bijv. "CG_20250120_1400")
    interval VARCHAR(10) NOT NULL,          -- Kline interval: '1m', '5m', '15m', '1h', '4h', '12h'
    
    -- Triggerende kline (die dit cluster triggerde, >2σ)
    trigger_kline_time TIMESTAMPTZ NOT NULL,    -- Open time van triggerende kline
    trigger_kline_close TIMESTAMPTZ NOT NULL,   -- Close time van triggerende kline
    trigger_sigma NUMERIC(8,4) NOT NULL,        -- Sigma score van triggerende kline
    trigger_return_pct NUMERIC(8,4) NOT NULL,   -- Return % van triggerende kline
    
    -- Hoogste sigma kline (in cluster_group, cross-interval)
    max_sigma_interval VARCHAR(10),             -- Interval met hoogste sigma
    max_sigma_kline_time TIMESTAMPTZ,           -- Open time van kline met hoogste sigma
    max_sigma_value NUMERIC(8,4),               -- Hoogste sigma waarde in group
    max_sigma_return_pct NUMERIC(8,4),          -- Return % van kline met hoogste sigma
    
    -- Post metadata
    first_post_time TIMESTAMPTZ,                -- Eerste post in lookback window
    last_post_time TIMESTAMPTZ,                 -- Laatste post in lookback window
    post_count INTEGER NOT NULL DEFAULT 0,      -- Aantal posts in cluster
    
    -- LLM analyse
    llm_suggested_post_id VARCHAR(255),         -- Post ID gesuggereerd door LLM
    llm_confidence NUMERIC(3,2),                -- LLM confidence (0.00-1.00)
    llm_reasoning TEXT,                         -- LLM uitleg
    llm_mechanism VARCHAR(1),                   -- 'A' (publiek), 'B' (signaal), 'C' (toeval)
    llm_causation_likelihood NUMERIC(3,2),      -- Kans dat post de beweging veroorzaakte
    llm_model VARCHAR(100),                     -- Gebruikt LLM model
    llm_analyzed_at TIMESTAMPTZ,                -- Wanneer LLM analyse gedaan
    
    -- Menselijke review
    validated_post_id VARCHAR(255),             -- Gevalideerde trigger post
    validated_by VARCHAR(100),                  -- Wie heeft gevalideerd
    validated_at TIMESTAMPTZ,                   -- Wanneer gevalideerd
    review_notes TEXT,                          -- Notities bij review
    
    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending, llm_analyzed, validated, skipped
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes voor efficiënte queries
CREATE INDEX idx_post_clusters_cluster_group ON truth.post_clusters(cluster_group);
CREATE INDEX idx_post_clusters_interval ON truth.post_clusters(interval);
CREATE INDEX idx_post_clusters_trigger_time ON truth.post_clusters(trigger_kline_time DESC);
CREATE INDEX idx_post_clusters_trigger_sigma ON truth.post_clusters(trigger_sigma DESC);
CREATE INDEX idx_post_clusters_status ON truth.post_clusters(status);
CREATE INDEX idx_post_clusters_post_count ON truth.post_clusters(post_count DESC);
CREATE INDEX idx_post_clusters_llm_suggested ON truth.post_clusters(llm_suggested_post_id);
CREATE INDEX idx_post_clusters_validated ON truth.post_clusters(validated_post_id);

-- ============================================================================
-- truth.cluster_posts - Join tabel voor posts in clusters
-- ============================================================================
CREATE TABLE truth.cluster_posts (
    cluster_id INTEGER NOT NULL REFERENCES truth.post_clusters(cluster_id) ON DELETE CASCADE,
    post_id VARCHAR(255) NOT NULL REFERENCES truth.posts(post_id) ON DELETE CASCADE,
    position_in_cluster INTEGER NOT NULL,       -- Positie (1 = eerste, chronologisch)
    PRIMARY KEY (cluster_id, post_id)
);

-- Index voor lookups op post_id
CREATE INDEX idx_cluster_posts_post_id ON truth.cluster_posts(post_id);

-- ============================================================================
-- Comments voor documentatie
-- ============================================================================
COMMENT ON TABLE truth.post_clusters IS 'Clusters van posts rondom significante koersbewegingen (>2σ)';
COMMENT ON COLUMN truth.post_clusters.cluster_group IS 'Groepering van clusters uit verschillende intervallen die overlappen in tijd';
COMMENT ON COLUMN truth.post_clusters.interval IS 'Kline interval waaruit dit cluster is gedetecteerd';
COMMENT ON COLUMN truth.post_clusters.trigger_kline_time IS 'Open timestamp van de kline die het event triggerde';
COMMENT ON COLUMN truth.post_clusters.trigger_kline_close IS 'Close timestamp van de kline die het event triggerde';
COMMENT ON COLUMN truth.post_clusters.trigger_sigma IS 'Sigma score (|return|/std) van de triggerende kline';
COMMENT ON COLUMN truth.post_clusters.max_sigma_interval IS 'Interval met de hoogste sigma in de cluster_group';
COMMENT ON COLUMN truth.post_clusters.llm_mechanism IS 'A=publieke reactie, B=gecoördineerd signaal, C=toeval';
COMMENT ON COLUMN truth.post_clusters.llm_causation_likelihood IS 'Geschatte kans dat de post de koersbeweging veroorzaakte';

COMMENT ON TABLE truth.cluster_posts IS 'Join tabel die posts koppelt aan clusters';

-- Bevestig aanmaak
SELECT 'Tabellen succesvol aangemaakt' AS status;
SELECT COUNT(*) AS post_clusters_count FROM truth.post_clusters;

