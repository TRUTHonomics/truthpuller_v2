"""
# INSTRUCTIONS:
# Controleer of alle posts en clusters beoordeeld zijn door de LLM.
# 
# Draai vanuit de truthpuller_v2 directory met actieve venv:
#   python scripts/check_llm_analysis_status.py
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor

# Voeg parent directory toe aan path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import SSH tunnel functie
from scripts.analyze_clusters_phase2 import ensure_ssh_tunnel

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Get database connection."""
    # Database configuratie (hardcoded uit truth_config.yaml)
    db_host = '127.0.0.1'  # localhost via SSH tunnel
    db_port = 15432
    db_name = 'KFLhyper'
    db_user = 'postgres'
    db_password = '1234'
    remote_db_port = 5432
    
    # Zorg dat SSH tunnel actief is
    if not ensure_ssh_tunnel(remote_port=remote_db_port):
        logger.warning("⚠️  SSH tunnel niet actief - database connectie kan falen")
    
    db_config = {
        'host': db_host,
        'port': db_port,
        'dbname': db_name,
        'user': db_user,
        'password': db_password
    }
    
    return psycopg2.connect(**db_config)


def check_phase1_analysis():
    """Controleer of alle posts in clusters Fase 1 analyse hebben."""
    logger.info("=== Fase 1 Analyse Status (truth.post_analysis) ===")
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Totaal aantal posts in clusters
    cursor.execute("""
        SELECT COUNT(DISTINCT cp.post_id) as total_posts_in_clusters
        FROM truth.cluster_posts cp
    """)
    total = cursor.fetchone()['total_posts_in_clusters']
    
    # Posts in clusters MET analyse
    cursor.execute("""
        SELECT COUNT(DISTINCT cp.post_id) as analyzed_posts
        FROM truth.cluster_posts cp
        INNER JOIN truth.post_analysis pa ON cp.post_id = pa.post_id
    """)
    analyzed = cursor.fetchone()['analyzed_posts']
    
    # Posts in clusters ZONDER analyse
    cursor.execute("""
        SELECT COUNT(DISTINCT cp.post_id) as unanalyzed_posts
        FROM truth.cluster_posts cp
        LEFT JOIN truth.post_analysis pa ON cp.post_id = pa.post_id
        WHERE pa.post_id IS NULL
    """)
    unanalyzed = cursor.fetchone()['unanalyzed_posts']
    
    logger.info(f"Totaal posts in clusters: {total}")
    logger.info(f"Posts met Fase 1 analyse: {analyzed}")
    logger.info(f"Posts ZONDER Fase 1 analyse: {unanalyzed}")
    
    if unanalyzed > 0:
        logger.warning(f"⚠️  {unanalyzed} posts missen nog Fase 1 analyse!")
        
        # Toon voorbeelden
        cursor.execute("""
            SELECT DISTINCT cp.post_id, p.created_at, LEFT(p.content_plain, 100) as content_preview
            FROM truth.cluster_posts cp
            JOIN truth.posts p ON cp.post_id = p.post_id
            LEFT JOIN truth.post_analysis pa ON cp.post_id = pa.post_id
            WHERE pa.post_id IS NULL
            ORDER BY p.created_at DESC
            LIMIT 10
        """)
        examples = cursor.fetchall()
        logger.info("\nVoorbeelden van posts zonder analyse:")
        for ex in examples:
            logger.info(f"  - {ex['post_id'][:20]}... ({ex['created_at']})")
    else:
        logger.info("✅ Alle posts in clusters hebben Fase 1 analyse!")
    
    cursor.close()
    conn.close()
    
    return {
        'total': total,
        'analyzed': analyzed,
        'unanalyzed': unanalyzed
    }


def check_phase2_analysis():
    """Controleer of alle clusters Phase 2 analyse hebben."""
    logger.info("\n=== Phase 2 Analyse Status (truth.post_clusters) ===")
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Totaal aantal clusters
    cursor.execute("""
        SELECT COUNT(*) as total_clusters
        FROM truth.post_clusters
        WHERE post_count > 0
    """)
    total = cursor.fetchone()['total_clusters']
    
    # Clusters per status
    cursor.execute("""
        SELECT 
            status,
            COUNT(*) as count,
            SUM(post_count) as total_posts
        FROM truth.post_clusters
        WHERE post_count > 0
        GROUP BY status
        ORDER BY status
    """)
    status_counts = cursor.fetchall()
    
    logger.info(f"Totaal clusters: {total}")
    logger.info("\nStatus breakdown:")
    for row in status_counts:
        logger.info(f"  {row['status']}: {row['count']} clusters ({row['total_posts']} posts)")
    
    # Pending clusters (nog niet geanalyseerd)
    cursor.execute("""
        SELECT COUNT(*) as pending_count
        FROM truth.post_clusters
        WHERE status = 'pending'
          AND post_count > 0
    """)
    pending = cursor.fetchone()['pending_count']
    
    # Clusters waar niet alle posts Fase 1 hebben
    cursor.execute("""
        SELECT COUNT(DISTINCT pc.cluster_id) as clusters_missing_phase1
        FROM truth.post_clusters pc
        WHERE pc.post_count > 0
          AND EXISTS (
              SELECT 1 FROM truth.cluster_posts cp
              LEFT JOIN truth.post_analysis pa ON cp.post_id = pa.post_id
              WHERE cp.cluster_id = pc.cluster_id
                AND pa.post_id IS NULL
          )
    """)
    missing_phase1 = cursor.fetchone()['clusters_missing_phase1']
    
    # LLM analyzed clusters
    cursor.execute("""
        SELECT COUNT(*) as llm_analyzed_count
        FROM truth.post_clusters
        WHERE status = 'llm_analyzed'
          AND post_count > 0
    """)
    llm_analyzed = cursor.fetchone()['llm_analyzed_count']
    
    logger.info(f"\nClusters met LLM analyse: {llm_analyzed}")
    logger.info(f"Clusters nog pending: {pending}")
    logger.info(f"Clusters waar posts Fase 1 missen: {missing_phase1}")
    
    if pending > 0:
        logger.warning(f"⚠️  {pending} clusters zijn nog pending (niet geanalyseerd door LLM)")
        
        # Toon voorbeelden van pending clusters
        cursor.execute("""
            SELECT 
                cluster_id,
                post_count,
                trigger_sigma,
                trigger_kline_time
            FROM truth.post_clusters
            WHERE status = 'pending'
              AND post_count > 0
            ORDER BY trigger_sigma DESC, post_count DESC
            LIMIT 10
        """)
        examples = cursor.fetchall()
        logger.info("\nVoorbeelden van pending clusters:")
        for ex in examples:
            logger.info(f"  - Cluster {ex['cluster_id']}: {ex['post_count']} posts, sigma={ex['trigger_sigma']:.2f}")
    else:
        logger.info("✅ Alle clusters zijn geanalyseerd door LLM!")
    
    cursor.close()
    conn.close()
    
    return {
        'total': total,
        'llm_analyzed': llm_analyzed,
        'pending': pending,
        'missing_phase1': missing_phase1,
        'status_breakdown': {row['status']: row['count'] for row in status_counts}
    }


def check_cluster_posts_rankings():
    """Controleer of cluster_posts rankings zijn gezet."""
    logger.info("\n=== Cluster Posts Rankings Status ===")
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Totaal cluster_posts entries
    cursor.execute("""
        SELECT COUNT(*) as total_entries
        FROM truth.cluster_posts
    """)
    total = cursor.fetchone()['total_entries']
    
    # Entries met rankings
    cursor.execute("""
        SELECT COUNT(*) as with_rankings
        FROM truth.cluster_posts
        WHERE rank_in_cluster IS NOT NULL
    """)
    with_rankings = cursor.fetchone()['with_rankings']
    
    # Entries met is_trigger set
    cursor.execute("""
        SELECT COUNT(*) as with_trigger
        FROM truth.cluster_posts
        WHERE is_trigger = TRUE
    """)
    with_trigger = cursor.fetchone()['with_trigger']
    
    logger.info(f"Totaal cluster_posts entries: {total}")
    logger.info(f"Entries met rankings: {with_rankings}")
    logger.info(f"Entries met is_trigger=True: {with_trigger}")
    
    if with_rankings < total:
        logger.warning(f"⚠️  {total - with_rankings} entries missen rankings")
    else:
        logger.info("✅ Alle entries hebben rankings!")
    
    cursor.close()
    conn.close()
    
    return {
        'total': total,
        'with_rankings': with_rankings,
        'with_trigger': with_trigger
    }


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("LLM Analyse Status Check")
    logger.info("=" * 60)
    
    try:
        # Check Fase 1 (post analysis)
        phase1_stats = check_phase1_analysis()
        
        # Check Phase 2 (cluster analysis)
        phase2_stats = check_phase2_analysis()
        
        # Check rankings
        rankings_stats = check_cluster_posts_rankings()
        
        # Samenvatting
        logger.info("\n" + "=" * 60)
        logger.info("SAMENVATTING")
        logger.info("=" * 60)
        
        phase1_complete = phase1_stats['unanalyzed'] == 0
        phase2_complete = phase2_stats['pending'] == 0
        
        logger.info(f"Fase 1 (Post Analyse): {'✅ COMPLEET' if phase1_complete else '⚠️  ONVOLTOOID'}")
        logger.info(f"Phase 2 (Cluster Analyse): {'✅ COMPLEET' if phase2_complete else '⚠️  ONVOLTOOID'}")
        
        if phase1_complete and phase2_complete:
            logger.info("\n🎉 Alle posts en clusters zijn beoordeeld door de LLM!")
        else:
            logger.warning("\n⚠️  Er zijn nog items die LLM analyse nodig hebben:")
            if not phase1_complete:
                logger.warning(f"  - {phase1_stats['unanalyzed']} posts missen Fase 1 analyse")
            if not phase2_complete:
                logger.warning(f"  - {phase2_stats['pending']} clusters missen Phase 2 analyse")
        
    except Exception as e:
        logger.error(f"Fout bij status check: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

