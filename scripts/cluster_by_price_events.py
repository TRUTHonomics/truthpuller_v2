"""
# INSTRUCTIONS:
# Dit script detecteert post clusters op basis van koersbewegingen (event-driven).
#
# Logica:
# 1. Detecteer significante koersbewegingen uit klines (> 2sigma) per interval
# 2. Dedupliceer events BINNEN elk interval (niet cross-interval)
# 3. Wijs cluster_groups toe aan overlappende events uit verschillende intervallen
# 4. Voor elk event: haal alle posts op van de 4 uur ervoor
#
# Draai vanuit de truthpuller_v2 directory met actieve venv:
#   python scripts/cluster_by_price_events.py
#
# Output: truth.post_clusters en truth.cluster_posts tabellen
"""

import os
import sys
import logging
import warnings
import shutil
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import yaml
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor

# Onderdruk SQLAlchemy warnings van pandas
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')


def setup_logging() -> logging.Logger:
    """Setup logging met file output en archivering."""
    script_dir = Path(__file__).parent.parent
    log_dir = script_dir / '_log'
    archive_dir = log_dir / 'archive'
    
    log_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / 'cluster_by_price_events.log'
    
    if log_file.exists():
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_path = archive_dir / f'cluster_by_price_events_{timestamp}.log'
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


class PriceEventClusterDetector:
    """
    Detecteert clusters van posts op basis van significante koersbewegingen.
    
    Event-driven aanpak:
    1. Vind significante koersbewegingen (> N sigma) per kline interval
    2. Dedupliceer overlappende events BINNEN elk interval
    3. Wijs cluster_groups toe voor cross-interval overlap
    4. Voor elk event: verzamel posts van de 4 uur ervoor
    """
    
    # Beschikbare kline intervallen (gesorteerd kort naar lang)
    KLINE_INTERVALS = ['1m', '5m', '15m', '1h', '4h', '12h']
    
    # Interval naar minuten mapping
    INTERVAL_MINUTES = {
        '1m': 1,
        '5m': 5,
        '15m': 15,
        '1h': 60,
        '4h': 240,
        '12h': 720
    }
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize detector.
        
        Args:
            config_path: Path naar config YAML
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
        self.schema = self.config['database']['schema']
        
        # Event detection configuratie
        event_cfg = self.config.get('event_clustering', {})
        self.sigma_threshold = event_cfg.get('sigma_threshold', 2.0)
        self.lookback_hours = event_cfg.get('lookback_hours', 4)
        self.dedup_window_hours = event_cfg.get('dedup_window_hours', 1)
        self.baseline_hours = event_cfg.get('baseline_hours', 24)
        self.min_klines_for_sigma = event_cfg.get('min_klines_for_sigma', 20)
        
        # Minimum datum - TRUMP coin launch
        self.min_date = self.config.get('impact_analysis', {}).get(
            'min_post_date', '2025-01-17T17:00:00+00:00'
        )
        
        logger.info(f"Event detection settings:")
        logger.info(f"  Sigma threshold: {self.sigma_threshold}")
        logger.info(f"  Lookback window: {self.lookback_hours} hours")
        logger.info(f"  Dedup window (binnen interval): {self.dedup_window_hours} hours")
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """Get database connection."""
        return psycopg2.connect(**self.db_config)
    
    def get_klines(self, interval: str) -> pd.DataFrame:
        """
        Haal kline data op voor een specifiek interval.
        
        Args:
            interval: Kline interval (1m, 5m, etc.)
            
        Returns:
            DataFrame met kline data
        """
        query = """
            SELECT 
                time,
                open,
                high,
                low,
                close,
                volume
            FROM truth.klines_raw
            WHERE interval_min = %s
              AND time >= %s
            ORDER BY time ASC
        """
        
        try:
            conn = self.get_connection()
            df = pd.read_sql(query, conn, params=(interval, self.min_date))
            conn.close()
            
            if not df.empty:
                # Bereken returns
                df['return_pct'] = (df['close'] - df['open']) / df['open'] * 100
                logger.debug(f"Loaded {len(df)} klines for interval {interval}")
            
            return df
        except Exception as e:
            logger.error(f"Failed to load klines for {interval}: {e}")
            return pd.DataFrame()
    
    def calculate_rolling_sigma(self, df: pd.DataFrame, interval: str) -> pd.DataFrame:
        """
        Bereken rolling standaarddeviatie voor sigma-based event detectie.
        
        Args:
            df: DataFrame met klines inclusief return_pct
            interval: Kline interval voor window berekening
            
        Returns:
            DataFrame met sigma kolom toegevoegd
        """
        if df.empty or 'return_pct' not in df.columns:
            return df
        
        # Bereken rolling window size (baseline_hours in aantal klines)
        interval_minutes = self.INTERVAL_MINUTES.get(interval, 60)
        window_size = max(self.min_klines_for_sigma, 
                         int(self.baseline_hours * 60 / interval_minutes))
        
        # Rolling standaarddeviatie
        df['rolling_std'] = df['return_pct'].rolling(
            window=window_size, 
            min_periods=self.min_klines_for_sigma
        ).std()
        
        # Sigma score = |return| / std
        df['sigma_score'] = df['return_pct'].abs() / df['rolling_std']
        
        return df
    
    def detect_events_in_interval(self, interval: str) -> List[Dict]:
        """
        Detecteer significante koersbewegingen in een interval.
        
        Args:
            interval: Kline interval
            
        Returns:
            List van event dicts
        """
        df = self.get_klines(interval)
        if df.empty:
            return []
        
        df = self.calculate_rolling_sigma(df, interval)
        
        # Filter op significante bewegingen
        significant = df[df['sigma_score'] >= self.sigma_threshold].copy()
        
        events = []
        interval_minutes = self.INTERVAL_MINUTES.get(interval, 60)
        
        for _, row in significant.iterrows():
            kline_time = row['time']
            # Zorg dat kline_time timezone-aware is
            if kline_time.tzinfo is None:
                kline_time = kline_time.replace(tzinfo=timezone.utc)
            
            # Event time = kline CLOSE time (wanneer de beweging compleet is)
            kline_close = kline_time + timedelta(minutes=interval_minutes)
            
            events.append({
                'kline_time': kline_time,
                'kline_close': kline_close,
                'interval': interval,
                'interval_minutes': interval_minutes,
                'return_pct': row['return_pct'],
                'sigma_score': row['sigma_score'],
                'direction': 'UP' if row['return_pct'] > 0 else 'DOWN'
            })
        
        logger.info(f"Interval {interval}: found {len(events)} significant events (>{self.sigma_threshold}sigma)")
        return events
    
    def deduplicate_within_interval(self, events: List[Dict]) -> List[Dict]:
        """
        Dedupliceer events BINNEN een interval.
        
        Events binnen dedup_window_hours van elkaar worden samengevoegd.
        Het event met de hoogste sigma_score wordt behouden.
        
        Args:
            events: List van events van EEN interval, gesorteerd op tijd
            
        Returns:
            Gedediceerde list van events
        """
        if not events:
            return []
        
        # Sorteer op tijd
        events = sorted(events, key=lambda x: x['kline_time'])
        
        dedup_window = timedelta(hours=self.dedup_window_hours)
        deduplicated = []
        current_group = [events[0]]
        
        for event in events[1:]:
            # Check of binnen window van laatste event in group
            if event['kline_time'] - current_group[-1]['kline_time'] <= dedup_window:
                current_group.append(event)
            else:
                # Selecteer beste event uit group (hoogste sigma)
                best = max(current_group, key=lambda x: x['sigma_score'])
                best['merged_count'] = len(current_group)
                deduplicated.append(best)
                
                # Start nieuwe group
                current_group = [event]
        
        # Laatste group
        if current_group:
            best = max(current_group, key=lambda x: x['sigma_score'])
            best['merged_count'] = len(current_group)
            deduplicated.append(best)
        
        return deduplicated
    
    def detect_all_events(self) -> Dict[str, List[Dict]]:
        """
        Detecteer significante koersbewegingen in alle intervallen.
        Dedupliceer BINNEN elk interval, maar niet cross-interval.
        
        Returns:
            Dict met interval als key en list van events als value
        """
        events_by_interval = {}
        
        for interval in self.KLINE_INTERVALS:
            raw_events = self.detect_events_in_interval(interval)
            deduped_events = self.deduplicate_within_interval(raw_events)
            events_by_interval[interval] = deduped_events
            
            if raw_events:
                logger.info(f"  {interval}: {len(raw_events)} raw -> {len(deduped_events)} after dedup")
        
        total_events = sum(len(e) for e in events_by_interval.values())
        logger.info(f"Total events across all intervals: {total_events}")
        
        return events_by_interval
    
    def assign_cluster_groups(self, events_by_interval: Dict[str, List[Dict]]) -> List[Dict]:
        """
        Wijs cluster_groups toe aan events uit verschillende intervallen.
        
        Logica: Als de kline_time van een korter interval event valt binnen
        [kline_time, kline_close] van een langer interval event, krijgen
        ze dezelfde cluster_group.
        
        Args:
            events_by_interval: Dict met events per interval
            
        Returns:
            Flat list van alle events met cluster_group toegevoegd
        """
        # Flatten alle events en sorteer op kline_time
        all_events = []
        for interval, events in events_by_interval.items():
            for event in events:
                all_events.append(event)
        
        all_events.sort(key=lambda x: x['kline_time'])
        
        if not all_events:
            return []
        
        # Wijs cluster_group toe
        group_counter = 0
        
        for event in all_events:
            if 'cluster_group' in event:
                continue  # Al toegewezen
            
            # Nieuwe group
            group_counter += 1
            group_code = f"CG_{event['kline_time'].strftime('%Y%m%d_%H%M')}"
            event['cluster_group'] = group_code
            
            # Zoek alle andere events die overlappen met dit event
            self._assign_overlapping_events(event, all_events, group_code)
        
        # Bereken max_sigma per cluster_group
        self._calculate_max_sigma_per_group(all_events)
        
        logger.info(f"Assigned {group_counter} cluster_groups to {len(all_events)} events")
        
        return all_events
    
    def _assign_overlapping_events(self, base_event: Dict, all_events: List[Dict], group_code: str):
        """
        Recursief toewijzen van dezelfde cluster_group aan overlappende events.
        
        Een event overlapt als:
        - De kline_time van het kortere interval valt binnen [kline_time, kline_close] 
          van het langere interval
        """
        base_open = base_event['kline_time']
        base_close = base_event['kline_close']
        
        for other_event in all_events:
            if other_event is base_event:
                continue
            if 'cluster_group' in other_event:
                continue
            
            other_open = other_event['kline_time']
            other_close = other_event['kline_close']
            
            # Check overlap: de trigger time (open) van het kortste interval
            # moet binnen [open, close] van het langere interval vallen
            
            # Case 1: other is korter interval -> check of other_open binnen base window
            if other_open >= base_open and other_open <= base_close:
                other_event['cluster_group'] = group_code
                # Recursief verder zoeken
                self._assign_overlapping_events(other_event, all_events, group_code)
            
            # Case 2: base is korter interval -> check of base_open binnen other window
            elif base_open >= other_open and base_open <= other_close:
                other_event['cluster_group'] = group_code
                self._assign_overlapping_events(other_event, all_events, group_code)
    
    def _calculate_max_sigma_per_group(self, all_events: List[Dict]):
        """
        Bereken max_sigma_* velden voor elke cluster_group.
        """
        # Groepeer events per cluster_group
        groups = {}
        for event in all_events:
            group = event.get('cluster_group', '')
            if group not in groups:
                groups[group] = []
            groups[group].append(event)
        
        # Voor elke group: vind event met hoogste sigma
        for group_code, group_events in groups.items():
            max_sigma_event = max(group_events, key=lambda x: x['sigma_score'])
            
            # Voeg max_sigma info toe aan alle events in de group
            for event in group_events:
                event['max_sigma_interval'] = max_sigma_event['interval']
                event['max_sigma_kline_time'] = max_sigma_event['kline_time']
                event['max_sigma_value'] = max_sigma_event['sigma_score']
                event['max_sigma_return_pct'] = max_sigma_event['return_pct']
    
    def get_posts_for_event(self, event: Dict) -> List[Dict]:
        """
        Haal alle posts op van de lookback window voor een event.
        
        Args:
            event: Event dict met kline_close (event time)
            
        Returns:
            List van post dicts
        """
        event_time = event['kline_close']  # Posts moeten voor de kline close zijn
        start_time = event_time - timedelta(hours=self.lookback_hours)
        
        query = """
            SELECT 
                p.post_id,
                p.created_at,
                p.content_plain,
                p.account_username,
                pi.impact_label,
                pi.signal,
                pi.expected_move_pct
            FROM truth.posts p
            LEFT JOIN truth.post_impact pi ON p.post_id = pi.post_id
            WHERE p.created_at >= %s
              AND p.created_at <= %s
              AND p.content_plain IS NOT NULL
              AND p.content_plain != ''
            ORDER BY p.created_at ASC
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, (start_time, event_time))
            posts = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return posts
        except Exception as e:
            logger.error(f"Failed to get posts for event: {e}")
            return []
    
    def build_clusters(self, all_events: List[Dict]) -> List[Dict]:
        """
        Bouw clusters voor alle events.
        
        Args:
            all_events: List van events met cluster_group toegewezen
            
        Returns:
            List van cluster dicts
        """
        clusters = []
        
        for i, event in enumerate(all_events):
            posts = self.get_posts_for_event(event)
            
            cluster = {
                'event': event,
                'posts': posts,
                'post_count': len(posts),
                'first_post_time': posts[0]['created_at'] if posts else None,
                'last_post_time': posts[-1]['created_at'] if posts else None,
            }
            clusters.append(cluster)
            
            if (i + 1) % 100 == 0:
                logger.info(f"Built {i+1}/{len(all_events)} clusters...")
        
        # Filter clusters zonder posts (optioneel - we houden ze nu voor completeness)
        clusters_with_posts = [c for c in clusters if c['post_count'] > 0]
        clusters_without_posts = len(clusters) - len(clusters_with_posts)
        
        logger.info(f"Built {len(clusters)} clusters ({clusters_without_posts} without posts)")
        
        return clusters
    
    def save_clusters_to_database(self, clusters: List[Dict]):
        """
        Sla clusters op in de database met nieuwe schema.
        
        Args:
            clusters: List van cluster dicts
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Clear existing clusters (tabellen zijn al opnieuw aangemaakt)
            cursor.execute("DELETE FROM truth.cluster_posts")
            cursor.execute("DELETE FROM truth.post_clusters")
            conn.commit()
            
            inserted = 0
            
            for cluster in clusters:
                event = cluster['event']
                posts = cluster['posts']
                
                # Insert cluster met nieuwe kolommen
                cursor.execute("""
                    INSERT INTO truth.post_clusters (
                        cluster_group,
                        interval,
                        trigger_kline_time,
                        trigger_kline_close,
                        trigger_sigma,
                        trigger_return_pct,
                        max_sigma_interval,
                        max_sigma_kline_time,
                        max_sigma_value,
                        max_sigma_return_pct,
                        first_post_time, 
                        last_post_time, 
                        post_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING cluster_id
                """, (
                    event.get('cluster_group'),
                    event['interval'],
                    event['kline_time'],
                    event['kline_close'],
                    event['sigma_score'],
                    event['return_pct'],
                    event.get('max_sigma_interval'),
                    event.get('max_sigma_kline_time'),
                    event.get('max_sigma_value'),
                    event.get('max_sigma_return_pct'),
                    cluster['first_post_time'],
                    cluster['last_post_time'],
                    cluster['post_count']
                ))
                
                cluster_id = cursor.fetchone()[0]
                
                # Insert cluster_posts
                for position, post in enumerate(posts, 1):
                    cursor.execute("""
                        INSERT INTO truth.cluster_posts (
                            cluster_id, post_id, position_in_cluster
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT (cluster_id, post_id) DO NOTHING
                    """, (cluster_id, post['post_id'], position))
                
                inserted += 1
                
                if inserted % 200 == 0:
                    conn.commit()
                    logger.info(f"Saved {inserted} clusters...")
            
            conn.commit()
            logger.info(f"Saved {inserted} clusters to database")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to save clusters: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def run(self) -> Dict:
        """
        Voer volledige event-driven cluster detectie uit.
        
        Returns:
            Dict met statistieken
        """
        logger.info("Starting Event-Driven Cluster Detection (v2)...")
        logger.info("  - Deduplicatie: alleen BINNEN interval")
        logger.info("  - cluster_group: voor cross-interval overlap")
        
        # Detecteer events per interval (met binnen-interval deduplicatie)
        events_by_interval = self.detect_all_events()
        
        total_events = sum(len(e) for e in events_by_interval.values())
        if total_events == 0:
            logger.warning("No significant price events detected")
            return {'error': 'No events detected'}
        
        # Wijs cluster_groups toe (cross-interval)
        all_events = self.assign_cluster_groups(events_by_interval)
        
        # Bouw clusters (posts per event)
        clusters = self.build_clusters(all_events)
        
        if not clusters:
            logger.warning("No clusters created")
            return {'error': 'No clusters created'}
        
        # Sla op in database
        self.save_clusters_to_database(clusters)
        
        # Statistieken
        stats = self.calculate_statistics(clusters, events_by_interval)
        
        return stats
    
    def calculate_statistics(self, clusters: List[Dict], events_by_interval: Dict[str, List[Dict]]) -> Dict:
        """
        Bereken statistieken over de clusters.
        """
        post_counts = [c['post_count'] for c in clusters]
        sigma_scores = [c['event']['sigma_score'] for c in clusters]
        
        # Events per interval
        interval_counts = {interval: len(events) for interval, events in events_by_interval.items()}
        
        # Unieke cluster_groups
        unique_groups = len(set(c['event'].get('cluster_group', '') for c in clusters))
        
        stats = {
            'total_clusters': len(clusters),
            'unique_cluster_groups': unique_groups,
            'clusters_with_posts': sum(1 for c in clusters if c['post_count'] > 0),
            'clusters_without_posts': sum(1 for c in clusters if c['post_count'] == 0),
            'total_posts_in_clusters': sum(post_counts),
            'avg_posts_per_cluster': np.mean(post_counts) if post_counts else 0,
            'max_posts_in_cluster': max(post_counts) if post_counts else 0,
            'min_posts_in_cluster': min(post_counts) if post_counts else 0,
            'avg_sigma': np.mean(sigma_scores) if sigma_scores else 0,
            'max_sigma': max(sigma_scores) if sigma_scores else 0,
            'events_by_interval': interval_counts,
            'sigma_threshold': self.sigma_threshold,
            'lookback_hours': self.lookback_hours
        }
        
        logger.info("\n=== Event-Driven Cluster Statistics (v2) ===")
        logger.info(f"Total clusters: {stats['total_clusters']}")
        logger.info(f"Unique cluster_groups: {stats['unique_cluster_groups']}")
        logger.info(f"Clusters with posts: {stats['clusters_with_posts']}")
        logger.info(f"Clusters without posts: {stats['clusters_without_posts']}")
        logger.info(f"Total posts in clusters: {stats['total_posts_in_clusters']}")
        logger.info(f"Avg posts per cluster: {stats['avg_posts_per_cluster']:.1f}")
        logger.info(f"Max posts in cluster: {stats['max_posts_in_cluster']}")
        logger.info(f"Avg sigma: {stats['avg_sigma']:.2f}")
        logger.info(f"Max sigma: {stats['max_sigma']:.2f}")
        logger.info(f"Events by interval: {interval_counts}")
        
        return stats


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Event-driven cluster detection (v2)')
    parser.add_argument('--sigma', type=float, default=2.0, 
                       help='Sigma threshold for event detection (default: 2.0)')
    parser.add_argument('--lookback', type=int, default=4,
                       help='Lookback window in hours (default: 4)')
    parser.add_argument('--dedup', type=float, default=1.0,
                       help='Deduplication window in hours (default: 1.0)')
    args = parser.parse_args()
    
    logger.info("Starting Event-Driven Post Cluster Detection v2...")
    
    detector = PriceEventClusterDetector()
    
    # Override settings indien opgegeven
    if args.sigma:
        detector.sigma_threshold = args.sigma
    if args.lookback:
        detector.lookback_hours = args.lookback
    if args.dedup:
        detector.dedup_window_hours = args.dedup
    
    logger.info(f"Settings: sigma={detector.sigma_threshold}, lookback={detector.lookback_hours}h, dedup={detector.dedup_window_hours}h")
    
    stats = detector.run()
    
    if 'error' in stats:
        logger.error(f"Cluster detection failed: {stats['error']}")
        sys.exit(1)
    
    logger.info("\nDone! Clusters saved to truth.post_clusters and truth.cluster_posts")


if __name__ == "__main__":
    main()
