-- Create truth.post_impact table for storing impact analysis results
-- Database: KFLhyper
-- Schema: truth
--
-- INSTRUCTIONS:
-- Run this script via psql or pgAdmin to create the table:
--   psql -h 10.10.10.1 -U postgres -d KFLhyper -f create_post_impact_table.sql
--
-- Or via SSH tunnel:
--   ssh -L 15432:localhost:5432 bart@10.10.10.1
--   psql -h localhost -p 15432 -U postgres -d KFLhyper -f create_post_impact_table.sql

-- Drop existing table if it exists (for clean reinstall)
-- DROP TABLE IF EXISTS truth.post_impact;

-- Create the post_impact table
CREATE TABLE IF NOT EXISTS truth.post_impact (
    -- Primary key with foreign key to posts
    post_id VARCHAR(255) PRIMARY KEY REFERENCES truth.posts(post_id) ON DELETE CASCADE,
    analyzed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Global classification (best impact across all measurements)
    impact_label VARCHAR(15) NOT NULL,          -- STRONG_IMPACT/MODERATE_IMPACT/WEAK_IMPACT/NO_IMPACT
    signal VARCHAR(4) NOT NULL,                 -- BUY/SELL/HOLD
    confidence NUMERIC(3,2) NOT NULL,           -- 0.00-1.00
    impact_horizon VARCHAR(6),                  -- SHORT/MEDIUM/LONG (NULL for NO_IMPACT)
    expected_move_pct NUMERIC(5,2),             -- -99.99 to +99.99%
    best_interval VARCHAR(5),                   -- 1m/5m/15m/1h/4h/12h
    best_window VARCHAR(5),                     -- 15m/60m/240m/720m
    
    -- Volatility context
    sigma_pct NUMERIC(8,4),                     -- σ (stddev returns) in %
    baseline_volatility NUMERIC(8,4),           -- Baseline vol for comparison
    threshold_method VARCHAR(10) NOT NULL,      -- fixed/sigma
    
    -- Per-horizon labels (for multi-task learning)
    impact_15m VARCHAR(15),
    signal_15m VARCHAR(4),
    confidence_15m NUMERIC(3,2),
    
    impact_60m VARCHAR(15),
    signal_60m VARCHAR(4),
    confidence_60m NUMERIC(3,2),
    
    impact_240m VARCHAR(15),
    signal_240m VARCHAR(4),
    confidence_240m NUMERIC(3,2),
    
    impact_720m VARCHAR(15),
    signal_720m VARCHAR(4),
    confidence_720m NUMERIC(3,2),
    
    -- Raw price changes as JSONB (flexible, queryable, compressible)
    -- Contains all interval/window combinations with full metrics
    price_changes JSONB,
    
    -- Classification reasoning
    reasoning TEXT,
    
    -- Constraints
    CONSTRAINT chk_impact_label CHECK (impact_label IN ('STRONG_IMPACT', 'MODERATE_IMPACT', 'WEAK_IMPACT', 'NO_IMPACT', 'UNKNOWN')),
    CONSTRAINT chk_signal CHECK (signal IN ('BUY', 'SELL', 'HOLD')),
    CONSTRAINT chk_confidence CHECK (confidence >= 0 AND confidence <= 1),
    CONSTRAINT chk_horizon CHECK (impact_horizon IS NULL OR impact_horizon IN ('SHORT', 'MEDIUM', 'LONG')),
    CONSTRAINT chk_threshold_method CHECK (threshold_method IN ('fixed', 'sigma'))
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_post_impact_label ON truth.post_impact(impact_label);
CREATE INDEX IF NOT EXISTS idx_post_impact_signal ON truth.post_impact(signal);
CREATE INDEX IF NOT EXISTS idx_post_impact_analyzed ON truth.post_impact(analyzed_at DESC);
CREATE INDEX IF NOT EXISTS idx_post_impact_horizon ON truth.post_impact(impact_horizon) WHERE impact_horizon IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_post_impact_confidence ON truth.post_impact(confidence DESC);

-- GIN index for JSONB queries on price_changes
CREATE INDEX IF NOT EXISTS idx_post_impact_price_changes ON truth.post_impact USING GIN (price_changes);

-- Comments for documentation
COMMENT ON TABLE truth.post_impact IS 'Impact analysis of Truth Social posts on TRUMP coin price';
COMMENT ON COLUMN truth.post_impact.post_id IS 'Foreign key to truth.posts';
COMMENT ON COLUMN truth.post_impact.impact_label IS 'Global impact classification: STRONG/MODERATE/WEAK/NO_IMPACT';
COMMENT ON COLUMN truth.post_impact.signal IS 'Trading signal: BUY/SELL/HOLD';
COMMENT ON COLUMN truth.post_impact.confidence IS 'Confidence score 0.00-1.00';
COMMENT ON COLUMN truth.post_impact.impact_horizon IS 'Time horizon of impact: SHORT (<60m), MEDIUM (60-240m), LONG (>240m)';
COMMENT ON COLUMN truth.post_impact.expected_move_pct IS 'Actual price movement percentage';
COMMENT ON COLUMN truth.post_impact.sigma_pct IS 'Stddev of returns (σ) used for sigma-based thresholds';
COMMENT ON COLUMN truth.post_impact.price_changes IS 'JSONB with all interval/window price change metrics';
COMMENT ON COLUMN truth.post_impact.impact_15m IS 'Impact classification for 15-minute horizon';
COMMENT ON COLUMN truth.post_impact.impact_60m IS 'Impact classification for 60-minute horizon';
COMMENT ON COLUMN truth.post_impact.impact_240m IS 'Impact classification for 4-hour horizon';
COMMENT ON COLUMN truth.post_impact.impact_720m IS 'Impact classification for 12-hour horizon';

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON truth.post_impact TO truthpuller;

-- Verify creation
SELECT 
    column_name, 
    data_type, 
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'truth' AND table_name = 'post_impact'
ORDER BY ordinal_position;

