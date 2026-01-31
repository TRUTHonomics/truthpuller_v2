"""
Database operaties voor Truthpuller v2.

Hergebruikt de KFLhyper database op 10.10.10.1 met truth schema.
"""

import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

logger = logging.getLogger(__name__)


class TruthDatabase:
    """
    Database operaties voor Truth Social posts en signals.
    
    Schema: truth
    Tabellen: posts, signals, post_assets
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize database connection.
        
        Args:
            config: Database configuratie dict
        """
        self.db_config = {
            'host': config.get('host', '10.10.10.1'),
            'port': config.get('port', 5432),
            'dbname': config.get('dbname', 'KFLhyper'),
            'user': config.get('user', 'truthpuller'),
            'password': config.get('password', '1234')
        }
        self.schema = config.get('schema', 'truth')
        
        logger.info(f"Database configured: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}")
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """Get new database connection."""
        return psycopg2.connect(**self.db_config)
    
    def test_connection(self) -> bool:
        """
        Test database connectivity.
        
        Returns:
            True als connectie succesvol
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            conn.close()
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def get_unprocessed_posts(
        self,
        limit: int = 100,
        accounts: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Haal posts op die nog niet door v2 zijn verwerkt.
        
        Args:
            limit: Maximum aantal posts
            accounts: Optioneel filter op account usernames
            
        Returns:
            List van post dicts
        """
        # REASON: We filteren op posts die nog geen v2 signal hebben
        # Dit wordt bepaald door model_version in signals tabel
        query = f"""
            SELECT 
                p.post_id,
                p.created_at,
                p.content_plain,
                p.account_username,
                p.replies_count,
                p.reblogs_count,
                p.favourites_count
            FROM {self.schema}.posts p
            WHERE p.content_plain IS NOT NULL
              AND p.content_plain != ''
              AND NOT EXISTS (
                  SELECT 1 FROM {self.schema}.signals s 
                  WHERE s.post_id = p.post_id 
                  AND s.model_version LIKE 'trump_signal%%'
              )
        """
        
        params = []
        
        if accounts:
            placeholders = ','.join(['%s'] * len(accounts))
            query += f" AND p.account_username IN ({placeholders})"
            params.extend(accounts)
        
        query += f" ORDER BY p.created_at DESC LIMIT {limit}"
        
        try:
            conn = self.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, params)
            results = [dict(row) for row in cur.fetchall()]
            cur.close()
            conn.close()
            
            logger.debug(f"Found {len(results)} unprocessed posts")
            return results
            
        except Exception as e:
            logger.error(f"Failed to get unprocessed posts: {e}")
            return []
    
    def get_latest_posts(
        self,
        limit: int = 10,
        accounts: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Haal de meest recente posts op.
        
        Args:
            limit: Maximum aantal posts
            accounts: Optioneel filter op account usernames
            
        Returns:
            List van post dicts
        """
        query = f"""
            SELECT 
                p.post_id,
                p.created_at,
                p.content_plain,
                p.account_username,
                p.replies_count,
                p.reblogs_count,
                p.favourites_count
            FROM {self.schema}.posts p
            WHERE p.content_plain IS NOT NULL
              AND p.content_plain != ''
        """
        
        params = []
        
        if accounts:
            placeholders = ','.join(['%s'] * len(accounts))
            query += f" AND p.account_username IN ({placeholders})"
            params.extend(accounts)
        
        query += f" ORDER BY p.created_at DESC LIMIT {limit}"
        
        try:
            conn = self.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, params)
            results = [dict(row) for row in cur.fetchall()]
            cur.close()
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Failed to get latest posts: {e}")
            return []
    
    def get_trump_asset_id(self, symbol: str = "TRUMPUSDT") -> Optional[int]:
        """
        Haal asset_id op voor TRUMP coin.
        
        Args:
            symbol: Symbol naam (default TRUMPUSDT)
            
        Returns:
            Asset ID of None
        """
        query = """
            SELECT id FROM symbols.symbols 
            WHERE bybit_symbol = %s 
               OR kraken_symbol ILIKE %s
               OR base ILIKE 'TRUMP'
            LIMIT 1
        """
        
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(query, (symbol, f'%{symbol}%'))
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            if result:
                return result[0]
            return None
            
        except Exception as e:
            logger.error(f"Failed to get TRUMP asset_id: {e}")
            return None
    
    def save_signal(
        self,
        post_id: str,
        asset_id: int,
        signal: str,
        confidence: float,
        reasoning: str,
        model_version: str = "trump_signal_llama3b_v1"
    ) -> bool:
        """
        Sla een trading signal op.
        
        Args:
            post_id: Post ID
            asset_id: Asset ID
            signal: BUY/SELL/HOLD → wordt vertaald naar BULLISH/BEARISH/NEUTRAL
            confidence: 0.0-1.0
            reasoning: LLM reasoning
            model_version: Model versie identifier
            
        Returns:
            True als succesvol
        """
        # Vertaal signal naar sentiment format (voor compatibiliteit met v1)
        sentiment_map = {
            'BUY': 'BULLISH',
            'SELL': 'BEARISH',
            'HOLD': 'NEUTRAL'
        }
        sentiment = sentiment_map.get(signal.upper(), 'NEUTRAL')
        
        query = f"""
            INSERT INTO {self.schema}.signals 
            (post_id, asset_id, sentiment, confidence, reasoning, model_version, processed_at_utc)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(query, (
                post_id,
                asset_id,
                sentiment,
                confidence,
                reasoning,
                model_version,
                datetime.now(timezone.utc)
            ))
            conn.commit()
            cur.close()
            conn.close()
            
            logger.debug(f"Saved signal for post {post_id}: {sentiment} ({confidence:.2f})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save signal: {e}")
            return False
    
    def save_signals_batch(
        self,
        signals: List[Dict[str, Any]],
        model_version: str = "trump_signal_llama3b_v1"
    ) -> int:
        """
        Sla meerdere signals op in batch.
        
        Args:
            signals: List van signal dicts met post_id, asset_id, signal, confidence, reasoning
            model_version: Model versie identifier
            
        Returns:
            Aantal opgeslagen signals
        """
        if not signals:
            return 0
        
        sentiment_map = {
            'BUY': 'BULLISH',
            'SELL': 'BEARISH',
            'HOLD': 'NEUTRAL'
        }
        
        data = []
        now = datetime.now(timezone.utc)
        
        for s in signals:
            sentiment = sentiment_map.get(s.get('signal', 'HOLD').upper(), 'NEUTRAL')
            data.append((
                s['post_id'],
                s['asset_id'],
                sentiment,
                s.get('confidence', 0.5),
                s.get('reasoning', ''),
                model_version,
                now
            ))
        
        query = f"""
            INSERT INTO {self.schema}.signals 
            (post_id, asset_id, sentiment, confidence, reasoning, model_version, processed_at_utc)
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            execute_values(cur, query, data)
            inserted = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Saved {inserted} signals in batch")
            return inserted
            
        except Exception as e:
            logger.error(f"Failed to save signals batch: {e}")
            return 0
    
    def get_signal_stats(self) -> Dict[str, Any]:
        """
        Haal statistieken op over signals.
        
        Returns:
            Dict met counts en distributie
        """
        query = f"""
            SELECT 
                COUNT(*) as total_signals,
                COUNT(DISTINCT post_id) as unique_posts,
                sentiment,
                AVG(confidence) as avg_confidence
            FROM {self.schema}.signals
            WHERE model_version LIKE 'trump_signal%%'
            GROUP BY sentiment
        """
        
        try:
            conn = self.get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query)
            results = [dict(row) for row in cur.fetchall()]
            cur.close()
            conn.close()
            
            stats = {
                'by_sentiment': {},
                'total': 0,
                'unique_posts': 0
            }
            
            for row in results:
                sentiment = row['sentiment']
                stats['by_sentiment'][sentiment] = {
                    'count': row['total_signals'],
                    'avg_confidence': float(row['avg_confidence']) if row['avg_confidence'] else 0
                }
                stats['total'] += row['total_signals']
                stats['unique_posts'] = max(stats['unique_posts'], row['unique_posts'])
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get signal stats: {e}")
            return {}

