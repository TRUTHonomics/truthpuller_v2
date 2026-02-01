"""
# INSTRUCTIONS:
# Dit is de FastAPI backend voor de cluster review interface.
#
# Start de server:
#   cd F:/Containers/truthpuller_v2
#   python -m uvicorn review.app:app --host localhost --port 8082 --reload
#
# Open browser: http://localhost:8082
#
# Note: Poorten 8080-8081 zijn in gebruik, gebruik 8082
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

# Voeg parent directory toe aan path voor imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, Request, HTTPException, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor

from src.kfl_logging import setup_kfl_logging

logger = setup_kfl_logging("app")

# Load config
config_path = Path(__file__).parent.parent / 'config' / 'truth_config.yaml'
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

db_config = {
    'host': config['database']['host'],
    'port': config['database']['port'],
    'dbname': config['database']['dbname'],
    'user': config['database']['user'],
    'password': config['database']['password']
}

review_config = config.get('review_interface', {})
CHART_INTERVAL = review_config.get('chart_interval', '15m')
CHART_HOURS_BEFORE = review_config.get('chart_hours_before', 1)
CHART_HOURS_AFTER = review_config.get('chart_hours_after', 4)

# FastAPI app
app = FastAPI(title="Truthpuller v2 - Cluster Review")

# Templates en static files
templates_dir = Path(__file__).parent / 'templates'
static_dir = Path(__file__).parent / 'static'

templates_dir.mkdir(exist_ok=True)
static_dir.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(templates_dir))
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(**db_config)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, priority_only: str = Query(default="true")):
    """
    Homepage met cluster overzicht.
    
    Args:
        priority_only: Als True, toon alleen clusters met hoge priority. Als False, toon alle clusters.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get review stats
    cursor.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE post_count = 1) AS single_post_clusters,
            COUNT(*) FILTER (WHERE post_count > 1) AS multi_post_clusters,
            COUNT(*) FILTER (WHERE status = 'pending') AS pending,
            COUNT(*) FILTER (WHERE status = 'llm_analyzed') AS llm_analyzed,
            COUNT(*) FILTER (WHERE status = 'validated') AS validated,
            COUNT(*) FILTER (WHERE status = 'skipped') AS skipped
        FROM truth.post_clusters
    """)
    stats = dict(cursor.fetchone())
    
    # Convert string to bool
    priority_only_bool = priority_only.lower() in ('true', '1', 'yes', 'on')
    
    # Get clusters needing review
    if priority_only_bool:
        # Alleen clusters met hoge priority (uncertainty-based)
        query = """
            SELECT 
                pc.cluster_id,
                pc.cluster_group,
                pc.interval,
                pc.first_post_time,
                pc.post_count,
                pc.trigger_sigma,
                pc.trigger_return_pct,
                pc.status,
                pc.llm_suggested_post_id,
                pc.llm_confidence,
                pc.llm_mechanism,
                pc.llm_causation_likelihood,
                pc.review_priority,
                pc.review_reason,
                pc.score_spread,
                pc.runner_up_post_id,
                pc.validated_post_id
            FROM truth.post_clusters pc
            WHERE pc.status IN ('llm_analyzed', 'pending', 'needs_review')
            ORDER BY 
                pc.review_priority ASC NULLS LAST,
                pc.trigger_sigma DESC NULLS LAST
            LIMIT 50
        """
    else:
        # Alle clusters (geen priority filter)
        query = """
            SELECT 
                pc.cluster_id,
                pc.cluster_group,
                pc.interval,
                pc.first_post_time,
                pc.post_count,
                pc.trigger_sigma,
                pc.trigger_return_pct,
                pc.status,
                pc.llm_suggested_post_id,
                pc.llm_confidence,
                pc.llm_mechanism,
                pc.llm_causation_likelihood,
                pc.review_priority,
                pc.review_reason,
                pc.score_spread,
                pc.runner_up_post_id,
                pc.validated_post_id
            FROM truth.post_clusters pc
            WHERE pc.status IN ('llm_analyzed', 'pending', 'needs_review')
            ORDER BY 
                pc.trigger_sigma DESC NULLS LAST
            LIMIT 500
        """
    
    cursor.execute(query)
    clusters = [dict(row) for row in cursor.fetchall()]
    
    # Get all validated clusters (voor review/herziening)
    cursor.execute("""
        SELECT 
            pc.cluster_id,
            pc.cluster_group,
            pc.interval,
            pc.first_post_time,
            pc.post_count,
            pc.trigger_sigma,
            pc.trigger_return_pct,
            pc.llm_mechanism,
            pc.llm_confidence,
            pc.validated_post_id,
            pc.validated_at,
            pc.review_notes,
            p.content_plain as validated_content
        FROM truth.post_clusters pc
        LEFT JOIN truth.posts p ON pc.validated_post_id = p.post_id
        WHERE pc.status = 'validated'
        ORDER BY pc.validated_at DESC
        LIMIT 100
    """)
    validated_clusters = [dict(row) for row in cursor.fetchall()]
    
    # Get recently validated (voor homepage)
    recent_validated = validated_clusters[:10]
    
    cursor.close()
    conn.close()
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "stats": stats,
        "clusters": clusters,
        "validated_clusters": validated_clusters,
        "recent_validated": recent_validated,
        "priority_only": priority_only_bool
    })


@app.get("/cluster/{cluster_id}", response_class=HTMLResponse)
async def review_cluster(request: Request, cluster_id: int):
    """Review pagina voor een specifiek cluster."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get cluster info
    cursor.execute("""
        SELECT * FROM truth.post_clusters WHERE cluster_id = %s
    """, (cluster_id,))
    cluster = cursor.fetchone()
    
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    cluster = dict(cluster)
    
    # Get posts in cluster with Phase 1 scores (volledige data)
    cursor.execute("""
        SELECT 
            cp.position_in_cluster,
            cp.is_trigger,
            cp.rank_in_cluster,
            cp.cluster_score,
            p.post_id,
            p.created_at,
            p.content_plain,
            p.account_username,
            p.url,
            -- Phase 1 scores from post_analysis (volledige analyse)
            pa.impact_likelihood,
            pa.suspicion_score,
            pa.sentiment_intensity,
            pa.news_value,
            pa.mechanism_a_score,
            pa.mechanism_b_score,
            pa.mechanism_c_score,
            pa.likely_mechanisms,
            pa.suspicious_elements,
            pa.detected_numbers,
            pa.unusual_phrases,
            pa.reasoning as phase1_reasoning,
            pa.confidence_calibration,
            pa.model as phase1_model,
            pa.analyzed_at as phase1_analyzed_at,
            -- Original impact data
            pi.impact_label,
            pi.signal,
            pi.expected_move_pct,
            pi.confidence
        FROM truth.cluster_posts cp
        JOIN truth.posts p ON cp.post_id = p.post_id
        LEFT JOIN truth.post_analysis pa ON p.post_id = pa.post_id
        LEFT JOIN truth.post_impact pi ON p.post_id = pi.post_id
        WHERE cp.cluster_id = %s
        ORDER BY 
            COALESCE(cp.rank_in_cluster, 999),
            cp.position_in_cluster ASC
    """, (cluster_id,))
    posts = [dict(row) for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return templates.TemplateResponse("review.html", {
        "request": request,
        "cluster": cluster,
        "posts": posts,
        "chart_interval": CHART_INTERVAL,
        "chart_hours_before": CHART_HOURS_BEFORE,
        "chart_hours_after": CHART_HOURS_AFTER
    })


@app.get("/api/klines/{cluster_id}")
async def get_klines(cluster_id: int):
    """API endpoint voor kline data voor chart."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get cluster time range
    cursor.execute("""
        SELECT first_post_time, last_post_time 
        FROM truth.post_clusters 
        WHERE cluster_id = %s
    """, (cluster_id,))
    cluster = cursor.fetchone()
    
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    
    # Calculate time range for chart
    first_time = cluster['first_post_time']
    last_time = cluster['last_post_time']
    
    start_time = first_time - timedelta(hours=CHART_HOURS_BEFORE)
    end_time = last_time + timedelta(hours=CHART_HOURS_AFTER)
    
    # Get klines
    cursor.execute("""
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
          AND time <= %s
        ORDER BY time ASC
    """, (CHART_INTERVAL, start_time, end_time))
    
    klines = []
    for row in cursor.fetchall():
        klines.append({
            'time': int(row['time'].timestamp()),
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': float(row['volume']) if row['volume'] else 0
        })
    
    # Get post times for markers
    cursor.execute("""
        SELECT 
            p.post_id,
            p.created_at,
            cp.position_in_cluster
        FROM truth.cluster_posts cp
        JOIN truth.posts p ON cp.post_id = p.post_id
        WHERE cp.cluster_id = %s
        ORDER BY cp.position_in_cluster
    """, (cluster_id,))
    
    markers = []
    for row in cursor.fetchall():
        markers.append({
            'time': int(row['created_at'].timestamp()),
            'position': row['position_in_cluster'],
            'post_id': row['post_id']
        })
    
    cursor.close()
    conn.close()
    
    return JSONResponse({
        'klines': klines,
        'markers': markers,
        'first_post_time': int(first_time.timestamp()),
        'last_post_time': int(last_time.timestamp())
    })


@app.post("/api/validate/{cluster_id}")
async def validate_cluster(
    cluster_id: int,
    selected_post_id: str = Form(...),
    review_notes: str = Form(default="")
):
    """Valideer een cluster met geselecteerde trigger post."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Update cluster status
        cursor.execute("""
            UPDATE truth.post_clusters
            SET 
                validated_post_id = %s,
                validated_by = 'reviewer',
                validated_at = %s,
                review_notes = %s,
                status = 'validated'
            WHERE cluster_id = %s
        """, (
            selected_post_id,
            datetime.now(timezone.utc),
            review_notes,
            cluster_id
        ))
        
        # Reset is_trigger for all posts in cluster
        cursor.execute("""
            UPDATE truth.cluster_posts
            SET is_trigger = FALSE
            WHERE cluster_id = %s
        """, (cluster_id,))
        
        # Set is_trigger for validated post
        cursor.execute("""
            UPDATE truth.cluster_posts
            SET is_trigger = TRUE
            WHERE cluster_id = %s AND post_id = %s
        """, (cluster_id, selected_post_id))
        
        conn.commit()
        
        logger.info(f"Validated cluster {cluster_id} with post {selected_post_id}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to validate cluster: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
    
    return RedirectResponse(url="/", status_code=303)


@app.post("/api/skip/{cluster_id}")
async def skip_cluster(cluster_id: int):
    """Skip een cluster (markeer als skipped)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE truth.post_clusters
            SET status = 'skipped'
            WHERE cluster_id = %s
        """, (cluster_id,))
        conn.commit()
        
        logger.info(f"Skipped cluster {cluster_id}")
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
    
    return RedirectResponse(url="/", status_code=303)


@app.post("/api/no-impact/{cluster_id}")
async def mark_no_impact(
    cluster_id: int,
    review_notes: str = Form(default="No Trump post caused this price movement")
):
    """Markeer cluster als 'geen Trump post had invloed'."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Markeer als skipped met speciale notes
        cursor.execute("""
            UPDATE truth.post_clusters
            SET 
                status = 'skipped',
                validated_by = 'reviewer',
                validated_at = %s,
                review_notes = %s
            WHERE cluster_id = %s
        """, (
            datetime.now(timezone.utc),
            f"NO_IMPACT: {review_notes}",
            cluster_id
        ))
        
        # Reset alle is_trigger flags
        cursor.execute("""
            UPDATE truth.cluster_posts
            SET is_trigger = FALSE
            WHERE cluster_id = %s
        """, (cluster_id,))
        
        conn.commit()
        
        logger.info(f"Marked cluster {cluster_id} as no impact")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to mark no impact: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
    
    return RedirectResponse(url="/", status_code=303)


@app.get("/api/stats")
async def get_stats():
    """API endpoint voor review statistieken."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("""
        SELECT 
            COUNT(*) AS total_clusters,
            COUNT(*) FILTER (WHERE post_count = 1) AS single_post,
            COUNT(*) FILTER (WHERE post_count > 1) AS multi_post,
            COUNT(*) FILTER (WHERE status = 'pending') AS pending,
            COUNT(*) FILTER (WHERE status = 'llm_analyzed') AS llm_analyzed,
            COUNT(*) FILTER (WHERE status = 'validated') AS validated,
            COUNT(*) FILTER (WHERE status = 'skipped') AS skipped,
            COUNT(*) FILTER (WHERE status IN ('llm_analyzed', 'pending', 'needs_review')) AS need_review,
            COUNT(*) FILTER (WHERE llm_mechanism = 'B') AS mechanism_b_count,
            AVG(llm_confidence) FILTER (WHERE llm_confidence IS NOT NULL) AS avg_confidence
        FROM truth.post_clusters
    """)
    stats = dict(cursor.fetchone())
    
    cursor.close()
    conn.close()
    
    return JSONResponse(stats)


if __name__ == "__main__":
    import uvicorn
    
    host = review_config.get('host', 'localhost')
    port = review_config.get('port', 8080)
    
    logger.info(f"Starting review interface on http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)

