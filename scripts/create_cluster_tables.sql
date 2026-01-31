-- Create cluster tables for semi-supervised labeling
-- Database: KFLhyper
-- Schema: truth
--
-- INSTRUCTIONS:
-- Run this script via psql or pgAdmin to create the tables:
--   psql -h localhost -p 15432 -U postgres -d KFLhyper -f scripts/create_cluster_tables.sql

-- ============================================================================
-- Table: truth.post_clusters
-- ============================================================================
-- Stores detected post clusters and their review status

CREATE TABLE IF NOT EXISTS truth.post_clusters (
    cluster_id SERIAL PRIMARY KEY,
    
    -- Cluster timing
    first_post_time TIMESTAMPTZ NOT NULL,
    last_post_time TIMESTAMPTZ NOT NULL,
    post_count INTEGER NOT NULL,
    
    -- Impact metrics (from analyze_price_impact.py)
    total_impact_pct NUMERIC(5,2),
    
    -- LLM analysis results
    llm_suggested_post_id VARCHAR(255) REFERENCES truth.posts(post_id),
    llm_confidence NUMERIC(3,2) CHECK (llm_confidence >= 0 AND llm_confidence <= 1),
    llm_reasoning TEXT,
    llm_model VARCHAR(100),
    llm_analyzed_at TIMESTAMPTZ,
    
    -- Human validation
    validated_post_id VARCHAR(255) REFERENCES truth.posts(post_id),
    validated_by VARCHAR(100),
    validated_at TIMESTAMPTZ,
    review_notes TEXT,
    
    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_cluster_status CHECK (status IN ('pending', 'llm_analyzed', 'validated', 'skipped'))
);

-- ============================================================================
-- Table: truth.cluster_posts
-- ============================================================================
-- Join table linking clusters to their posts

CREATE TABLE IF NOT EXISTS truth.cluster_posts (
    cluster_id INTEGER NOT NULL REFERENCES truth.post_clusters(cluster_id) ON DELETE CASCADE,
    post_id VARCHAR(255) NOT NULL REFERENCES truth.posts(post_id) ON DELETE CASCADE,
    position_in_cluster INTEGER NOT NULL,
    
    PRIMARY KEY (cluster_id, post_id)
);

-- ============================================================================
-- Indexes
-- ============================================================================

-- Cluster queries
CREATE INDEX IF NOT EXISTS idx_post_clusters_status ON truth.post_clusters(status);
CREATE INDEX IF NOT EXISTS idx_post_clusters_first_time ON truth.post_clusters(first_post_time DESC);
CREATE INDEX IF NOT EXISTS idx_post_clusters_impact ON truth.post_clusters(total_impact_pct DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_post_clusters_post_count ON truth.post_clusters(post_count) WHERE post_count > 1;

-- Cluster posts queries
CREATE INDEX IF NOT EXISTS idx_cluster_posts_post_id ON truth.cluster_posts(post_id);
CREATE INDEX IF NOT EXISTS idx_cluster_posts_position ON truth.cluster_posts(cluster_id, position_in_cluster);

-- ============================================================================
-- Comments
-- ============================================================================

COMMENT ON TABLE truth.post_clusters IS 'Detected clusters of posts that occurred within a short time window';
COMMENT ON COLUMN truth.post_clusters.cluster_id IS 'Auto-generated cluster ID';
COMMENT ON COLUMN truth.post_clusters.first_post_time IS 'Timestamp of first post in cluster';
COMMENT ON COLUMN truth.post_clusters.last_post_time IS 'Timestamp of last post in cluster';
COMMENT ON COLUMN truth.post_clusters.post_count IS 'Number of posts in this cluster';
COMMENT ON COLUMN truth.post_clusters.total_impact_pct IS 'Maximum impact percentage observed across cluster posts';
COMMENT ON COLUMN truth.post_clusters.llm_suggested_post_id IS 'Post ID suggested by LLM as the trigger post';
COMMENT ON COLUMN truth.post_clusters.llm_confidence IS 'LLM confidence in its suggestion (0.0-1.0)';
COMMENT ON COLUMN truth.post_clusters.llm_reasoning IS 'LLM explanation for why this post was chosen';
COMMENT ON COLUMN truth.post_clusters.llm_model IS 'Name of LLM model used for analysis';
COMMENT ON COLUMN truth.post_clusters.validated_post_id IS 'Post ID validated by human reviewer';
COMMENT ON COLUMN truth.post_clusters.validated_by IS 'Username of human reviewer';
COMMENT ON COLUMN truth.post_clusters.validated_at IS 'Timestamp of human validation';
COMMENT ON COLUMN truth.post_clusters.review_notes IS 'Optional notes from reviewer';
COMMENT ON COLUMN truth.post_clusters.status IS 'Workflow status: pending, llm_analyzed, validated, skipped';

COMMENT ON TABLE truth.cluster_posts IS 'Join table linking clusters to their constituent posts';
COMMENT ON COLUMN truth.cluster_posts.position_in_cluster IS 'Chronological position of post within cluster (1 = first)';

-- ============================================================================
-- Useful views
-- ============================================================================

-- View: Clusters that need review (multi-post clusters with impact)
CREATE OR REPLACE VIEW truth.clusters_needing_review AS
SELECT 
    pc.cluster_id,
    pc.first_post_time,
    pc.last_post_time,
    pc.post_count,
    pc.total_impact_pct,
    pc.status,
    pc.llm_suggested_post_id,
    pc.llm_confidence,
    pc.validated_post_id
FROM truth.post_clusters pc
WHERE pc.post_count > 1
  AND pc.status IN ('pending', 'llm_analyzed')
  AND ABS(COALESCE(pc.total_impact_pct, 0)) >= 1.0  -- At least 1% impact
ORDER BY ABS(pc.total_impact_pct) DESC NULLS LAST;

COMMENT ON VIEW truth.clusters_needing_review IS 'Multi-post clusters with impact that need human review';

-- View: Review progress statistics
CREATE OR REPLACE VIEW truth.cluster_review_stats AS
SELECT 
    COUNT(*) FILTER (WHERE post_count = 1) AS single_post_clusters,
    COUNT(*) FILTER (WHERE post_count > 1) AS multi_post_clusters,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
    COUNT(*) FILTER (WHERE status = 'llm_analyzed') AS llm_analyzed,
    COUNT(*) FILTER (WHERE status = 'validated') AS validated,
    COUNT(*) FILTER (WHERE status = 'skipped') AS skipped,
    COUNT(*) FILTER (WHERE post_count > 1 AND ABS(COALESCE(total_impact_pct, 0)) >= 1.0) AS need_review
FROM truth.post_clusters;

COMMENT ON VIEW truth.cluster_review_stats IS 'Statistics on cluster review progress';

-- ============================================================================
-- Verification
-- ============================================================================

SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_schema = 'truth' 
  AND table_name IN ('post_clusters', 'cluster_posts')
ORDER BY table_name, ordinal_position;

