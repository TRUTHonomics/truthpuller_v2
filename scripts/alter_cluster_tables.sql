-- =============================================================================
-- ALTER CLUSTER TABLES FOR PHASE 3
-- =============================================================================
-- Dit script voegt kolommen toe aan truth.cluster_posts en truth.post_clusters
-- voor Fase 3: Cluster Vergelijking en Review Prioriteit.
--
-- Uitvoeren via SSH tunnel:
--   psql -h localhost -p 15432 -U postgres -d KFLhyper -f scripts/alter_cluster_tables.sql
-- =============================================================================

-- ==== truth.cluster_posts ====

-- is_trigger: Geeft aan of deze post de trigger is in het cluster
ALTER TABLE truth.cluster_posts 
ADD COLUMN IF NOT EXISTS is_trigger BOOLEAN DEFAULT FALSE;

-- rank_in_cluster: Ranking van posts binnen cluster op basis van analyse scores
ALTER TABLE truth.cluster_posts 
ADD COLUMN IF NOT EXISTS rank_in_cluster INTEGER;

-- cluster_score: Gecombineerde score voor deze post in dit specifieke cluster
ALTER TABLE truth.cluster_posts 
ADD COLUMN IF NOT EXISTS cluster_score NUMERIC(4,3);


-- ==== truth.post_clusters ====

-- review_priority: Prioriteit voor menselijke review (1 = hoogste)
ALTER TABLE truth.post_clusters 
ADD COLUMN IF NOT EXISTS review_priority INTEGER;

-- review_reason: Uitleg waarom dit cluster hoge prioriteit heeft
ALTER TABLE truth.post_clusters 
ADD COLUMN IF NOT EXISTS review_reason TEXT;

-- score_spread: Verschil tussen #1 en #2 kandidaat (close race indicator)
ALTER TABLE truth.post_clusters 
ADD COLUMN IF NOT EXISTS score_spread NUMERIC(4,3);

-- runner_up_post_id: Tweede beste kandidaat post
ALTER TABLE truth.post_clusters 
ADD COLUMN IF NOT EXISTS runner_up_post_id VARCHAR(255);

-- runner_up_confidence: Confidence voor runner-up
ALTER TABLE truth.post_clusters 
ADD COLUMN IF NOT EXISTS runner_up_confidence NUMERIC(3,2);

-- combined_effect: Geeft aan of meerdere posts samen effect hadden
ALTER TABLE truth.post_clusters 
ADD COLUMN IF NOT EXISTS combined_effect BOOLEAN DEFAULT FALSE;


-- ==== Indexes ====

-- Index voor trigger posts
CREATE INDEX IF NOT EXISTS idx_cluster_posts_trigger 
    ON truth.cluster_posts(cluster_id) 
    WHERE is_trigger = TRUE;

-- Index voor review prioriteit
CREATE INDEX IF NOT EXISTS idx_post_clusters_priority 
    ON truth.post_clusters(review_priority ASC NULLS LAST)
    WHERE status IN ('pending', 'llm_analyzed', 'needs_review');

-- Index voor ranking
CREATE INDEX IF NOT EXISTS idx_cluster_posts_rank 
    ON truth.cluster_posts(cluster_id, rank_in_cluster);


-- ==== Comments ====

COMMENT ON COLUMN truth.cluster_posts.is_trigger IS 'Post is de geselecteerde trigger in dit cluster';
COMMENT ON COLUMN truth.cluster_posts.rank_in_cluster IS 'Ranking binnen cluster (1=hoogst)';
COMMENT ON COLUMN truth.cluster_posts.cluster_score IS 'Gecombineerde score (impact + timing) voor dit cluster';

COMMENT ON COLUMN truth.post_clusters.review_priority IS 'Review prioriteit (1=hoogst), gebaseerd op uncertainty';
COMMENT ON COLUMN truth.post_clusters.review_reason IS 'Reden voor review prioriteit (low confidence, mechanism B, etc.)';
COMMENT ON COLUMN truth.post_clusters.score_spread IS 'Verschil tussen #1 en #2 kandidaat';
COMMENT ON COLUMN truth.post_clusters.runner_up_post_id IS 'Tweede beste kandidaat post ID';
COMMENT ON COLUMN truth.post_clusters.runner_up_confidence IS 'Confidence score voor runner-up';
COMMENT ON COLUMN truth.post_clusters.combined_effect IS 'Meerdere posts werkten samen';


-- ==== View voor review queue ====

CREATE OR REPLACE VIEW truth.review_queue AS
SELECT 
    pc.cluster_id,
    pc.cluster_group,
    pc.interval,
    pc.trigger_sigma,
    pc.trigger_return_pct,
    pc.post_count,
    pc.llm_suggested_post_id,
    pc.llm_confidence,
    pc.llm_mechanism,
    pc.llm_causation_likelihood,
    pc.review_priority,
    pc.review_reason,
    pc.score_spread,
    pc.runner_up_post_id,
    pc.status,
    pc.first_post_time,
    pc.trigger_kline_close,
    -- Top 3 posts by impact_likelihood
    (
        SELECT json_agg(sub ORDER BY sub.impact_likelihood DESC)
        FROM (
            SELECT 
                cp.post_id,
                cp.rank_in_cluster,
                cp.is_trigger,
                pa.impact_likelihood,
                pa.suspicion_score,
                pa.likely_mechanisms
            FROM truth.cluster_posts cp
            LEFT JOIN truth.post_analysis pa ON cp.post_id = pa.post_id
            WHERE cp.cluster_id = pc.cluster_id
            ORDER BY COALESCE(pa.impact_likelihood, 0) DESC
            LIMIT 3
        ) sub
    ) as top_candidates
FROM truth.post_clusters pc
WHERE pc.status IN ('llm_analyzed', 'needs_review', 'pending')
ORDER BY pc.review_priority ASC NULLS LAST, pc.trigger_sigma DESC;

COMMENT ON VIEW truth.review_queue IS 'Review queue gesorteerd op prioriteit met top kandidaten';

