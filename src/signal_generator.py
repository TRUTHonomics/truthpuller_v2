"""
# INSTRUCTIONS:
# Signal Generator voor real-time verwerking van Truth Social posts.
#
# Draai vanuit de truthpuller_v2 directory met actieve venv:
#   python src/signal_generator.py
#
# Of voor continue monitoring:
#   python src/signal_generator.py --watch
#
# Vereist: LM Studio draaiend met geladen model op localhost:1234
"""

import os
import sys
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import yaml

# Voeg project root toe aan path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import TruthDatabase
from src.llm_client import LLMClient
from src.kfl_logging import setup_kfl_logging

logger = setup_kfl_logging("signal_generator")


class SignalGenerator:
    """
    Real-time signal generator voor Truth Social posts.
    
    Polls database voor nieuwe posts, analyseert met LLM,
    en slaat signals op voor de trade engine.
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize signal generator.
        
        Args:
            config_path: Path naar config YAML
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config' / 'truth_config.yaml'
        
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        # Initialize components
        self.db = TruthDatabase(self.config['database'])
        self.llm = LLMClient(self.config['llm'])
        
        # Config
        sig_config = self.config.get('signal_generator', {})
        self.poll_interval = sig_config.get('poll_interval', 30)
        self.accounts = sig_config.get('accounts', ['realDonaldTrump'])
        self.always_include_trump = sig_config.get('always_include_trump', True)
        
        # TRUMP asset ID (cached)
        self._trump_asset_id = None
        
        # Stats
        self.stats = {
            'posts_processed': 0,
            'signals_generated': 0,
            'errors': 0,
            'started_at': datetime.now(timezone.utc)
        }
    
    @property
    def trump_asset_id(self) -> Optional[int]:
        """Get cached TRUMP asset ID."""
        if self._trump_asset_id is None:
            symbol = self.config.get('signal_generator', {}).get('trump_asset_symbol', 'TRUMPUSDT')
            self._trump_asset_id = self.db.get_trump_asset_id(symbol)
        return self._trump_asset_id
    
    def check_services(self) -> bool:
        """
        Check of alle services beschikbaar zijn.
        
        Returns:
            True als alles OK
        """
        logger.info("Checking services...")
        
        # Database
        if not self.db.test_connection():
            logger.error("Database connection failed!")
            return False
        logger.info("✓ Database connected")
        
        # LLM
        if not self.llm.health_check():
            logger.error("LLM service not available!")
            logger.error(f"Check of LM Studio draait op {self.config['llm']['base_url']}")
            return False
        logger.info("✓ LLM service available")
        
        # TRUMP asset
        if self.trump_asset_id is None:
            logger.warning("⚠ TRUMP asset not found in database")
            logger.warning("Signals worden niet gekoppeld aan een asset_id")
        else:
            logger.info(f"✓ TRUMP asset_id: {self.trump_asset_id}")
        
        return True
    
    def process_post(self, post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Verwerk een enkele post en genereer signal.
        
        Args:
            post: Post dict met post_id, content_plain, etc.
            
        Returns:
            Signal dict of None bij fout
        """
        post_id = post['post_id']
        content = post.get('content_plain', '')
        
        if not content:
            logger.warning(f"Post {post_id} has no content")
            return None
        
        logger.debug(f"Processing post {post_id}: {content[:50]}...")
        
        # Analyseer met LLM
        analysis = self.llm.analyze_post(content)
        
        if not analysis.get('success'):
            logger.warning(f"LLM analysis failed for {post_id}: {analysis.get('error')}")
            self.stats['errors'] += 1
            return None
        
        # Bouw signal
        signal = {
            'post_id': post_id,
            'asset_id': self.trump_asset_id,
            'signal': analysis['signal'],
            'confidence': analysis['confidence'],
            'reasoning': analysis['reasoning'],
            'analyzed_at': analysis['analyzed_at']
        }
        
        self.stats['posts_processed'] += 1
        
        # Log significant signals
        if signal['signal'] != 'HOLD' and signal['confidence'] >= 0.5:
            logger.info(f"📊 Signal: {signal['signal']} (conf: {signal['confidence']:.2f}) "
                       f"- {content[:60]}...")
        
        return signal
    
    def process_batch(self, limit: int = 50) -> int:
        """
        Verwerk een batch onverwerkte posts.
        
        Args:
            limit: Maximum aantal posts te verwerken
            
        Returns:
            Aantal verwerkte posts
        """
        # Haal onverwerkte posts op
        posts = self.db.get_unprocessed_posts(limit=limit, accounts=self.accounts)
        
        if not posts:
            logger.debug("No unprocessed posts found")
            return 0
        
        logger.info(f"Processing {len(posts)} posts...")
        
        signals = []
        for post in posts:
            signal = self.process_post(post)
            if signal and signal['asset_id']:
                signals.append(signal)
        
        # Sla signals op
        if signals:
            saved = self.db.save_signals_batch(signals)
            self.stats['signals_generated'] += saved
            logger.info(f"Saved {saved} signals")
        
        return len(posts)
    
    def run_once(self) -> Dict[str, Any]:
        """
        Verwerk alle onverwerkte posts (eenmalig).
        
        Returns:
            Stats dict
        """
        logger.info("Running single batch processing...")
        
        total_processed = 0
        batch_size = 50
        
        while True:
            processed = self.process_batch(limit=batch_size)
            total_processed += processed
            
            if processed < batch_size:
                break
            
            # Rate limiting
            time.sleep(1)
        
        logger.info(f"Completed. Processed {total_processed} posts, "
                   f"generated {self.stats['signals_generated']} signals")
        
        return self.stats
    
    def run_watch(self):
        """
        Continue monitoring mode - poll voor nieuwe posts.
        """
        logger.info(f"Starting watch mode (interval: {self.poll_interval}s)")
        logger.info(f"Monitoring accounts: {', '.join(self.accounts)}")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while True:
                processed = self.process_batch(limit=10)
                
                if processed > 0:
                    logger.info(f"Processed {processed} new posts")
                
                time.sleep(self.poll_interval)
                
        except KeyboardInterrupt:
            logger.info("\nStopping watch mode...")
            self.print_stats()
    
    def print_stats(self):
        """Print processing statistieken."""
        runtime = datetime.now(timezone.utc) - self.stats['started_at']
        
        logger.info("\n" + "=" * 50)
        logger.info("Signal Generator Statistics")
        logger.info("=" * 50)
        logger.info(f"Runtime: {runtime}")
        logger.info(f"Posts processed: {self.stats['posts_processed']}")
        logger.info(f"Signals generated: {self.stats['signals_generated']}")
        logger.info(f"Errors: {self.stats['errors']}")
        
        # Database stats
        db_stats = self.db.get_signal_stats()
        if db_stats:
            logger.info("\nSignal Distribution (v2 model):")
            for sentiment, data in db_stats.get('by_sentiment', {}).items():
                logger.info(f"  {sentiment}: {data['count']} (avg conf: {data['avg_confidence']:.2f})")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Truthpuller v2 Signal Generator')
    parser.add_argument('--watch', action='store_true',
                       help='Run in continuous watch mode')
    parser.add_argument('--config', type=str,
                       help='Path to config file')
    parser.add_argument('--check', action='store_true',
                       help='Only check services, do not process')
    args = parser.parse_args()
    
    # Config path
    config_path = Path(args.config) if args.config else None
    
    # Initialize
    generator = SignalGenerator(config_path)
    
    # Check services
    if not generator.check_services():
        logger.error("Service check failed. Exiting.")
        sys.exit(1)
    
    if args.check:
        logger.info("Service check passed. Exiting.")
        sys.exit(0)
    
    # Run
    if args.watch:
        generator.run_watch()
    else:
        generator.run_once()
        generator.print_stats()


if __name__ == "__main__":
    main()

