-- =============================================================================
-- CREATE truth.post_analysis TABLE
-- =============================================================================
-- Dit script maakt de tabel voor Fase 2: Per-Post LLM Analyse.
-- Bevat scores en metadata van individuele post analyse door het 72B labeling model.
--
-- Uitvoeren via SSH tunnel:
--   psql -h localhost -p 15432 -U postgres -d KFLhyper -f scripts/create_post_analysis_table.sql
-- =============================================================================

-- Maak tabel aan
CREATE TABLE IF NOT EXISTS truth.post_analysis (
    -- Primary key
    post_id VARCHAR(255) PRIMARY KEY REFERENCES truth.posts(post_id) ON DELETE CASCADE,
    
    -- Fase 2 scores (0.000 - 1.000)
    impact_likelihood NUMERIC(4,3),       -- Kans dat post koers beweegt
    suspicion_score NUMERIC(4,3),         -- Hoe "gecodeerd" de post lijkt
    sentiment_intensity NUMERIC(4,3),     -- Emotionele intensiteit
    news_value NUMERIC(4,3),              -- Nieuwswaarde/urgentie
    
    -- Mechanism scores (moeten optellen tot ~1.0)
    mechanism_a_score NUMERIC(4,3),       -- Public market reaction
    mechanism_b_score NUMERIC(4,3),       -- Coordinated signals (insider)
    mechanism_c_score NUMERIC(4,3),       -- Coincidence/noise
    likely_mechanisms VARCHAR(10),        -- 'A', 'B', 'C', 'AB', etc.
    
    -- Verdachte elementen (voor Mechanism B detectie)
    suspicious_elements TEXT,             -- Beschrijving van verdachte patterns
    detected_numbers TEXT,                -- Gevonden getallen/datums in post
    unusual_phrases TEXT,                 -- Ongebruikelijke woordkeuzes
    
    -- LLM reasoning
    reasoning TEXT,                       -- LLM uitleg van analyse
    
    -- Metadata
    model VARCHAR(100),                   -- LLM model naam (bijv. qwen2.5-72b)
    analyzed_at TIMESTAMPTZ,              -- Tijdstip van analyse
    analysis_version INTEGER DEFAULT 1,   -- Versie voor re-analyse tracking
    
    -- Self-reported confidence
    confidence_calibration NUMERIC(4,3),  -- LLM's eigen confidence in analyse
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes voor efficiente queries
CREATE INDEX IF NOT EXISTS idx_post_analysis_impact 
    ON truth.post_analysis(impact_likelihood DESC);

CREATE INDEX IF NOT EXISTS idx_post_analysis_suspicion 
    ON truth.post_analysis(suspicion_score DESC);

CREATE INDEX IF NOT EXISTS idx_post_analysis_mechanism 
    ON truth.post_analysis(likely_mechanisms);

CREATE INDEX IF NOT EXISTS idx_post_analysis_analyzed 
    ON truth.post_analysis(analyzed_at DESC);

-- Partial index voor niet-geanalyseerde posts
CREATE INDEX IF NOT EXISTS idx_post_analysis_pending 
    ON truth.post_analysis(post_id) 
    WHERE analyzed_at IS NULL;

-- Comments
COMMENT ON TABLE truth.post_analysis IS 'Per-post LLM analyse resultaten (Fase 2 van training pipeline)';
COMMENT ON COLUMN truth.post_analysis.impact_likelihood IS 'Kans dat post koers beweegt (0.0-1.0)';
COMMENT ON COLUMN truth.post_analysis.suspicion_score IS 'Hoe gecodeerd/verdacht de post lijkt (0.0-1.0)';
COMMENT ON COLUMN truth.post_analysis.mechanism_a_score IS 'Kans op Public Market Reaction';
COMMENT ON COLUMN truth.post_analysis.mechanism_b_score IS 'Kans op Coordinated Signal (insider)';
COMMENT ON COLUMN truth.post_analysis.mechanism_c_score IS 'Kans op Coincidence/noise';
COMMENT ON COLUMN truth.post_analysis.likely_mechanisms IS 'Meest waarschijnlijke mechanism(s): A, B, C, AB';

