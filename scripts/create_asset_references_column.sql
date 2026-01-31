-- Add asset_references column to truth.posts
-- This column stores detected asset references as JSONB array
-- Run this script to add the column to an existing table

-- Add the column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'truth' 
        AND table_name = 'posts' 
        AND column_name = 'asset_references'
    ) THEN
        ALTER TABLE truth.posts 
        ADD COLUMN asset_references JSONB DEFAULT '[]'::jsonb;
        
        RAISE NOTICE 'Column asset_references added to truth.posts';
    ELSE
        RAISE NOTICE 'Column asset_references already exists';
    END IF;
END $$;

-- Add comment describing the column structure
COMMENT ON COLUMN truth.posts.asset_references IS 
'Array van asset referenties gevonden in post. Structuur: [{"asset_id": 123, "symbol": "BTCUSDT", "base": "BTC", "match_type": "ticker|name|full_symbol", "match_text": "BTC", "asset_type": "crypto|non_crypto"}]';

-- Create GIN index for efficient JSONB queries
CREATE INDEX IF NOT EXISTS idx_posts_asset_references 
ON truth.posts USING GIN (asset_references);

-- Example queries after this column is populated:
-- 
-- Alle posts die BTC refereren:
-- SELECT * FROM truth.posts 
-- WHERE asset_references @> '[{"base": "BTC"}]'::jsonb;
--
-- Posts met meerdere assets:
-- SELECT post_id, jsonb_array_length(asset_references) as asset_count
-- FROM truth.posts 
-- WHERE jsonb_array_length(asset_references) > 1;
--
-- Posts met specifieke asset_id:
-- SELECT * FROM truth.posts 
-- WHERE asset_references @> '[{"asset_id": 123}]'::jsonb;

