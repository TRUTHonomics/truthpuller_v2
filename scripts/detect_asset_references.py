"""
# INSTRUCTIONS:
# Dit script detecteert directe asset referenties (crypto namen, tickers) in posts.
# Het scant post content op matches met een TOML-geconfigureerde asset lijst.
#
# Vereist: 
#   - SSH tunnel naar database (ssh -L 15432:localhost:5432 bart@10.10.10.1)
#   - config/assets/crypto_assets.toml (genereer met generate_asset_toml.py)
#   - config/assets/non_crypto_assets.toml
#
# Draai vanuit de truthpuller_v2 directory met actieve venv:
#   python scripts/detect_asset_references.py
#   python scripts/detect_asset_references.py --limit 100     # Max aantal posts
#   python scripts/detect_asset_references.py --test          # Test met sample post
#   python scripts/detect_asset_references.py --post-id <id>  # Specifieke post
#   python scripts/detect_asset_references.py --reprocess     # Herverwerk alle posts
#
# Output: asset_references JSONB kolom in truth.posts
"""

import os
import sys
import re
import json
import logging
import warnings
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple, Any
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor, Json

# Suppress warnings
warnings.filterwarnings('ignore')

# Use tomllib (Python 3.11+)
import tomllib

def load_toml(path: Path) -> Dict:
    """Load TOML file using tomllib."""
    with open(path, 'rb') as f:
        return tomllib.load(f)


def setup_logging() -> logging.Logger:
    """Setup logging met file output en archivering."""
    script_dir = Path(__file__).parent.parent
    log_dir = script_dir / '_log'
    archive_dir = log_dir / 'archive'
    
    log_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / 'detect_asset_references.log'
    
    if log_file.exists():
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_path = archive_dir / f'detect_asset_references_{timestamp}.log'
        shutil.move(str(log_file), str(archive_path))
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


logger = setup_logging()


class AssetReferenceDetector:
    """
    Detecteert asset referenties in tekst.
    
    Laadt asset configuratie uit TOML bestanden en matcht deze
    tegen post content met word boundary regex matching.
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize detector.
        
        Args:
            config_path: Path naar truth_config.yaml
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config' / 'truth_config.yaml'
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.db_config = {
            'host': self.config['database']['host'],
            'port': self.config['database']['port'],
            'dbname': self.config['database']['dbname'],
            'user': self.config['database']['user'],
            'password': self.config['database']['password']
        }
        
        # Load asset configurations
        assets_dir = Path(__file__).parent.parent / 'config' / 'assets'
        
        self.crypto_assets = self._load_asset_config(assets_dir / 'crypto_assets.toml')
        self.non_crypto_assets = self._load_asset_config(assets_dir / 'non_crypto_assets.toml')
        
        # Build search patterns
        self._build_search_patterns()
        
        logger.info(f"Loaded {len(self.crypto_assets)} crypto assets")
        logger.info(f"Loaded {len(self.non_crypto_assets)} non-crypto assets")
    
    def _load_asset_config(self, path: Path) -> Dict[str, Dict]:
        """Load asset configuration from TOML file."""
        if not path.exists():
            logger.warning(f"Asset config not found: {path}")
            return {}
        
        try:
            data = load_toml(path)
            return data.get('assets', {})
        except Exception as e:
            logger.error(f"Failed to load {path}: {e}")
            return {}
    
    def _build_search_patterns(self):
        """
        Build regex patterns for efficient text searching.
        
        Creates a mapping of patterns to asset info for quick lookup.
        Patterns use word boundaries to avoid false positives.
        """
        self.patterns: List[Tuple[re.Pattern, Dict]] = []
        
        # Process crypto assets
        for key, asset in self.crypto_assets.items():
            asset_info = {
                'asset_id': asset.get('asset_id'),
                'symbol': asset.get('symbol', key),
                'base': asset.get('base', ''),
                'asset_type': 'crypto'
            }
            
            for ticker in asset.get('tickers', []):
                # Skip very short tickers (1-2 chars) - too many false positives
                if len(ticker) < 3:
                    continue
                
                pattern = self._create_word_boundary_pattern(ticker)
                self.patterns.append((pattern, asset_info, ticker, 'ticker'))
        
        # Process non-crypto assets
        for key, asset in self.non_crypto_assets.items():
            asset_info = {
                'asset_id': asset.get('asset_id'),  # May be None
                'symbol': key,
                'base': key,
                'asset_type': asset.get('asset_type', 'non_crypto')
            }
            
            for ticker in asset.get('tickers', []):
                # Skip very short tickers
                if len(ticker) < 2:
                    continue
                
                pattern = self._create_word_boundary_pattern(ticker)
                self.patterns.append((pattern, asset_info, ticker, 
                                    'name' if ' ' in ticker else 'ticker'))
        
        logger.info(f"Built {len(self.patterns)} search patterns")
    
    def _create_word_boundary_pattern(self, text: str) -> re.Pattern:
        """
        Create regex pattern with word boundaries.
        
        Args:
            text: Text to match
            
        Returns:
            Compiled regex pattern (case-insensitive)
        """
        # Escape special regex characters
        escaped = re.escape(text)
        # Use word boundaries to avoid partial matches
        pattern = rf'\b{escaped}\b'
        return re.compile(pattern, re.IGNORECASE)
    
    def detect_in_text(self, text: str) -> List[Dict]:
        """
        Detect asset references in text.
        
        Args:
            text: Text content to search
            
        Returns:
            List of detected asset references
        """
        if not text:
            return []
        
        detected: Dict[str, Dict] = {}  # Key by (asset_type, symbol) to dedupe
        
        for pattern, asset_info, original_ticker, match_type in self.patterns:
            if pattern.search(text):
                # Create unique key for deduplication
                key = f"{asset_info['asset_type']}:{asset_info['symbol']}"
                
                if key not in detected:
                    detected[key] = {
                        'asset_id': asset_info['asset_id'],
                        'symbol': asset_info['symbol'],
                        'base': asset_info['base'],
                        'match_type': match_type,
                        'match_text': original_ticker,
                        'asset_type': asset_info['asset_type']
                    }
                else:
                    # Already detected, but update if we found a more specific match
                    # Prefer 'name' over 'ticker' as it's more explicit
                    if match_type == 'name' and detected[key]['match_type'] != 'name':
                        detected[key]['match_type'] = match_type
                        detected[key]['match_text'] = original_ticker
        
        return list(detected.values())
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """Get database connection."""
        return psycopg2.connect(**self.db_config)
    
    def get_posts_to_process(self, limit: Optional[int] = None, reprocess: bool = False) -> List[Dict]:
        """
        Get posts that need asset reference detection.
        
        Args:
            limit: Maximum number of posts
            reprocess: If True, process all posts regardless of existing references
            
        Returns:
            List of post dicts
        """
        if reprocess:
            where_clause = "WHERE p.content_plain IS NOT NULL AND p.content_plain != ''"
        else:
            where_clause = """
                WHERE (p.asset_references IS NULL OR p.asset_references = '[]'::jsonb)
                  AND p.content_plain IS NOT NULL 
                  AND p.content_plain != ''
            """
        
        query = f"""
            SELECT 
                p.post_id,
                p.content_plain,
                p.created_at
            FROM truth.posts p
            {where_clause}
            ORDER BY p.created_at DESC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            posts = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(posts)} posts to process")
            return posts
        except Exception as e:
            logger.error(f"Failed to get posts: {e}")
            return []
    
    def get_single_post(self, post_id: str) -> Optional[Dict]:
        """Get a specific post."""
        query = """
            SELECT 
                p.post_id,
                p.content_plain,
                p.created_at
            FROM truth.posts p
            WHERE p.post_id = %s
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, (post_id,))
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Failed to get post {post_id}: {e}")
            return None
    
    def save_asset_references(self, post_id: str, references: List[Dict]) -> bool:
        """
        Save detected asset references to database.
        
        Args:
            post_id: Post ID
            references: List of detected references
            
        Returns:
            True if successful
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Convert to JSON
            references_json = Json(references) if references else Json([])
            
            cursor.execute("""
                UPDATE truth.posts 
                SET asset_references = %s
                WHERE post_id = %s
            """, (references_json, post_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
        except Exception as e:
            logger.error(f"Failed to save references for {post_id}: {e}")
            return False
    
    def process_post(self, post: Dict) -> Tuple[int, List[Dict]]:
        """
        Process a single post for asset detection.
        
        Args:
            post: Post dict
            
        Returns:
            Tuple of (num_detected, list of references)
        """
        post_id = post['post_id']
        content = post.get('content_plain', '') or ''
        
        # Detect references
        references = self.detect_in_text(content)
        
        # Save to database
        if self.save_asset_references(post_id, references):
            return len(references), references
        else:
            return 0, []
    
    def run_test(self) -> Dict:
        """
        Test mode: show detection on sample texts.
        
        Returns:
            Dict with test results
        """
        logger.info("=== TEST MODE: Asset Reference Detection ===")
        
        # Sample test texts
        test_texts = [
            "Just bought some BTC and ETH! Bitcoin to the moon!",
            "NVIDIA stock is up 10% after earnings",
            "Gold and Silver are safe haven assets",
            "TRUMP coin and DOGE are pumping today",
            "Tesla stock (TSLA) is looking bullish",
            "No crypto mentioned here, just politics",
            "The S&P 500 is at all-time highs",
            "Ethereum Classic (ETC) vs Ethereum (ETH)",
            "Check out $BTC and $SOL prices",
        ]
        
        results = []
        for text in test_texts:
            refs = self.detect_in_text(text)
            results.append({
                'text': text[:80] + ('...' if len(text) > 80 else ''),
                'detected': [r['match_text'] for r in refs],
                'count': len(refs)
            })
            
            logger.info(f"\nText: {text}")
            if refs:
                for ref in refs:
                    logger.info(f"  -> {ref['symbol']} ({ref['asset_type']}) matched on '{ref['match_text']}'")
            else:
                logger.info("  -> No assets detected")
        
        return {'test_results': results}
    
    def run(self, limit: Optional[int] = None, reprocess: bool = False) -> Dict:
        """
        Process all pending posts.
        
        Args:
            limit: Maximum number of posts
            reprocess: Reprocess all posts
            
        Returns:
            Dict with statistics
        """
        logger.info("Starting Asset Reference Detection...")
        
        posts = self.get_posts_to_process(limit, reprocess)
        
        if not posts:
            logger.info("No posts to process")
            return {'processed': 0, 'with_references': 0, 'total_references': 0}
        
        processed = 0
        with_references = 0
        total_references = 0
        start_time = datetime.now()
        
        for i, post in enumerate(posts):
            post_id = post['post_id']
            
            # Progress
            if processed > 0 and processed % 100 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = processed / elapsed
                remaining = len(posts) - i
                eta = remaining / rate if rate > 0 else 0
                logger.info(f"Progress: {i+1}/{len(posts)} ({rate:.1f} posts/s, ETA: {eta/60:.1f}m)")
            
            num_refs, refs = self.process_post(post)
            processed += 1
            
            if num_refs > 0:
                with_references += 1
                total_references += num_refs
                logger.debug(f"Post {post_id[:20]}: {num_refs} refs - {[r['symbol'] for r in refs]}")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        stats = {
            'processed': processed,
            'with_references': with_references,
            'total_references': total_references,
            'elapsed_seconds': elapsed,
            'rate_per_second': processed / elapsed if elapsed > 0 else 0
        }
        
        logger.info(f"\n=== Asset Reference Detection Complete ===")
        logger.info(f"Processed: {processed} posts")
        logger.info(f"With references: {with_references} ({100*with_references/processed:.1f}%)")
        logger.info(f"Total references: {total_references}")
        logger.info(f"Time: {elapsed:.1f}s ({stats['rate_per_second']:.1f} posts/s)")
        
        return stats


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Detect asset references in posts')
    parser.add_argument('--limit', type=int, help='Maximum number of posts to process')
    parser.add_argument('--test', action='store_true', help='Run test mode with sample texts')
    parser.add_argument('--post-id', type=str, help='Process specific post ID')
    parser.add_argument('--reprocess', action='store_true', help='Reprocess all posts')
    args = parser.parse_args()
    
    detector = AssetReferenceDetector()
    
    if args.test:
        result = detector.run_test()
        sys.exit(0)
    
    if args.post_id:
        post = detector.get_single_post(args.post_id)
        if not post:
            logger.error(f"Post not found: {args.post_id}")
            sys.exit(1)
        
        logger.info(f"Processing post: {args.post_id}")
        logger.info(f"Content: {(post.get('content_plain', '') or '')[:200]}...")
        
        num_refs, refs = detector.process_post(post)
        
        logger.info(f"\nDetected {num_refs} asset references:")
        for ref in refs:
            logger.info(f"  - {ref['symbol']} ({ref['asset_type']}): matched '{ref['match_text']}'")
        
        sys.exit(0)
    
    stats = detector.run(limit=args.limit, reprocess=args.reprocess)
    
    if stats.get('processed', 0) == 0:
        logger.info("No posts processed. Run generate_asset_toml.py first if crypto_assets.toml is missing.")
    
    logger.info("\nDone!")


if __name__ == "__main__":
    main()

