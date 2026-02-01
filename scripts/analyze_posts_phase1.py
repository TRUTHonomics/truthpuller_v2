"""
# INSTRUCTIONS:
# Dit script analyseert individuele posts met een LLM (Fase 2 van pipeline).
# Elke post wordt IN ISOLATIE geanalyseerd, zonder cluster context.
#
# Vereist: LM Studio draait met een krachtig model (bijv. Qwen2.5 72B)
#
# Draai vanuit de truthpuller_v2 directory met actieve venv:
#   python scripts/analyze_posts_phase1.py
#   python scripts/analyze_posts_phase1.py --limit 100     # Max aantal posts
#   python scripts/analyze_posts_phase1.py --check         # Check LLM beschikbaarheid
#   python scripts/analyze_posts_phase1.py --test          # Test met 1 post
#
# Output: scores in truth.post_analysis tabel
"""

import os
import sys
import json
import logging
import warnings
import shutil
import time
import socket
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import yaml
import httpx
import psycopg2
from psycopg2.extras import RealDictCursor

# Prompt loader voor externe prompts
from prompt_loader import get_system_prompt, get_phase1_prompt

# Voeg parent directory toe aan path voor config imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.prompts.schemas import get_response_format
from src.kfl_logging import setup_kfl_logging

# Onderdruk warnings
warnings.filterwarnings("ignore")

logger = setup_kfl_logging()


class PostAnalyzerPhase1:
    """
    Analyseert individuele posts met LLM voor Fase 2 van de training pipeline.
    
    Elke post wordt in isolatie geanalyseerd om:
    - impact_likelihood: kans op koersimpact
    - suspicion_score: hoe "gecodeerd" de post lijkt
    - mechanism scores: A (public), B (insider), C (noise)
    """
    
    # Performance thresholds (simpel)
    MAX_DURATION_SECONDS = 60     # >60s = te traag, reload nodig
    RECOVERY_THRESHOLD = 10       # 10x <60s = verhoog layers
    
    # GPU offload configuratie (in layers)
    # Model: Qwen2.5-32B heeft 65 layers totaal
    MODEL_TOTAL_LAYERS = 65
    START_GPU_LAYERS = 44         # Start layers op GPU
    MAX_GPU_LAYERS = 44           # Maximum layers op GPU (45 veroorzaakt VRAM overcommit)
    MIN_GPU_LAYERS = 32           # Minimum layers op GPU
    GPU_REDUCE_ON_RELOAD = 1      # Layers verminderen bij reload
    GPU_INCREASE_ON_RECOVERY = 1  # Layers verhogen bij herstel
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize analyzer.
        
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
        
        # LLM configuratie
        llm_cfg = self.config.get('llm', {})
        analyzer_cfg = self.config.get('cluster_analyzer', {})
        
        self.llm_base_url = llm_cfg.get('base_url', 'http://localhost:1234/v1')
        self.llm_model = analyzer_cfg.get('model', 'qwen2.5-72b-instruct.i1')
        self.temperature = analyzer_cfg.get('temperature', 0.3)
        self.max_tokens = analyzer_cfg.get('max_tokens', 512)
        self.timeout = llm_cfg.get('timeout', 600)
        
        # Performance tracking (simpel)
        self.total_reloads = 0
        self.current_gpu_layers = self.START_GPU_LAYERS
        self.consecutive_fast_analyses = 0  # Teller voor <60s analyses
        
        # ECG Heartbeat
        self.heartbeat_interval = 60  # seconds
        self.heartbeat_enabled = True
        self._last_heartbeat = 0
        self._heartbeat_stats = {"posts_analyzed": 0, "errors": 0}
        
        logger.info(f"Using LLM model: {self.llm_model}")
        logger.info(f"LLM endpoint: {self.llm_base_url}")
        logger.info(f"Target GPU layers: {self.current_gpu_layers}/{self.MODEL_TOTAL_LAYERS}")
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """Get database connection."""
        return psycopg2.connect(**self.db_config)
    
    def _send_heartbeat(self, metadata: dict = None) -> bool:
        """Stuur heartbeat naar ECG database."""
        if not self.heartbeat_enabled:
            return True
        
        try:
            now = datetime.now(timezone.utc)
            meta = metadata or {}
            meta["interval_seconds"] = self.heartbeat_interval
            meta.update(self._heartbeat_stats)
            meta["gpu_layers"] = self.current_gpu_layers
            meta["total_reloads"] = self.total_reloads
            
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO ecg.heartbeats (service_name, host, pid, last_heartbeat, metadata, status)
                VALUES (%s, %s, %s, %s, %s, 'alive')
                ON CONFLICT (service_name, host) 
                DO UPDATE SET 
                    last_heartbeat = EXCLUDED.last_heartbeat,
                    pid = EXCLUDED.pid,
                    status = 'alive',
                    metadata = EXCLUDED.metadata
            """, (
                "analyze_posts_phase1",
                socket.gethostname(),
                os.getpid(),
                now,
                json.dumps(meta),
            ))
            conn.commit()
            cursor.close()
            conn.close()
            
            self._last_heartbeat = time.time()
            logger.debug("ECG heartbeat sent")
            return True
            
        except Exception as e:
            logger.debug(f"Failed to send ECG heartbeat: {e}")
            return False
    
    def get_posts_to_analyze(self, limit: Optional[int] = None) -> List[Dict]:
        """
        Haal posts op die nog niet geanalyseerd zijn.
        Focust op posts die in clusters zitten (efficiency).
        
        Args:
            limit: Maximum aantal posts
            
        Returns:
            List van post dicts
        """
        query = """
            SELECT DISTINCT
                p.post_id,
                p.created_at,
                p.content_plain,
                p.account_username,
                'original' as post_type
            FROM truth.posts p
            -- Alleen posts die in clusters zitten
            INNER JOIN truth.cluster_posts cp ON p.post_id = cp.post_id
            -- Nog niet geanalyseerd
            LEFT JOIN truth.post_analysis pa ON p.post_id = pa.post_id
            WHERE pa.post_id IS NULL
              AND p.content_plain IS NOT NULL
              AND p.content_plain != ''
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
            
            logger.info(f"Found {len(posts)} posts to analyze")
            return posts
        except Exception as e:
            logger.error(f"Failed to get posts: {e}")
            return []
    
    def get_single_post(self, post_id: str) -> Optional[Dict]:
        """Haal een specifieke post op voor test mode."""
        query = """
            SELECT 
                p.post_id,
                p.created_at,
                p.content_plain,
                p.account_username,
                'original' as post_type
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
    
    def get_random_post_from_cluster(self) -> Optional[Dict]:
        """Haal een willekeurige post op uit een cluster voor test mode."""
        query = """
            SELECT 
                p.post_id,
                p.created_at,
                p.content_plain,
                p.account_username,
                'original' as post_type
            FROM truth.posts p
            INNER JOIN truth.cluster_posts cp ON p.post_id = cp.post_id
            WHERE p.content_plain IS NOT NULL
              AND LENGTH(p.content_plain) > 50
            ORDER BY RANDOM()
            LIMIT 1
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Failed to get random post: {e}")
            return None
    
    def build_prompt(self, post: Dict) -> str:
        """
        Bouw de user prompt voor LLM analyse uit externe TOML.
        
        Args:
            post: Post metadata
            
        Returns:
            User prompt string (system prompt wordt apart geladen)
        """
        content = post.get('content_plain', '') or '[geen content]'
        timestamp = post.get('created_at', 'unknown')
        post_type = post.get('post_type', 'original')
        
        # Truncate zeer lange content
        if len(content) > 2000:
            content = content[:2000] + '...'
        
        # REASON: Laad prompt uit externe TOML via prompt_loader
        return get_phase1_prompt(
            content=content,
            timestamp=str(timestamp),
            post_type=post_type
        )
    
    def call_llm(self, user_prompt: str, timeout: Optional[int] = None) -> Optional[Dict]:
        """
        Roep de LLM API aan met ChatML format (system + user message).
        
        REASON: Gebruikt response_format met JSON schema voor gegarandeerd valide JSON.
        Zie: https://lmstudio.ai/docs/app/api/endpoints/post-chat-completions
        
        Args:
            user_prompt: User prompt (system prompt wordt automatisch geladen)
            timeout: Override timeout
            
        Returns:
            Parsed JSON response of None bij error
        """
        use_timeout = timeout if timeout is not None else self.timeout
        
        # REASON: Laad system prompt uit externe TOML voor ChatML format
        system_prompt = get_system_prompt()
        
        # REASON: JSON schema enforcement via response_format
        response_format = get_response_format("post_analysis")
        
        try:
            with httpx.Client(timeout=use_timeout) as client:
                response = client.post(
                    f"{self.llm_base_url}/chat/completions",
                    json={
                        "model": self.llm_model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt}
                        ],
                        "temperature": self.temperature,
                        "max_tokens": self.max_tokens,
                        "response_format": response_format  # Enforce JSON schema
                    }
                )
                response.raise_for_status()
                
                result = response.json()
                content = result['choices'][0]['message']['content']
                
                # REASON: Met response_format is content gegarandeerd valide JSON
                # Geen find/rfind parsing meer nodig
                return json.loads(content)
                    
        except httpx.HTTPError as e:
            logger.error(f"LLM API error: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error calling LLM: {e}")
            return None
    
    def analyze_post(self, post: Dict, timeout: Optional[int] = None) -> Optional[Dict]:
        """
        Analyseer een enkele post.
        
        Args:
            post: Post metadata
            timeout: Override timeout voor LLM call
            
        Returns:
            Analysis result of None bij error
        """
        post_id = post['post_id']
        
        # Bouw prompt
        prompt = self.build_prompt(post)
        logger.debug(f"Prompt for post {post_id}:\n{prompt[:300]}...")
        
        # Call LLM
        result = self.call_llm(prompt, timeout=timeout)
        if result is None:
            return None
        
        # Valideer en normaliseer result
        try:
            def clamp(val, min_val=0.0, max_val=1.0):
                """Clamp waarde tussen min en max."""
                if val is None:
                    return 0.5
                return min(max_val, max(min_val, float(val)))
            
            # Haal mechanism scores op
            mech_a = clamp(result.get('mechanism_a_score'))
            mech_b = clamp(result.get('mechanism_b_score'))
            mech_c = clamp(result.get('mechanism_c_score'))
            
            # REASON: Normaliseer mechanism scores naar sum=1.0 (conform commentaar doc)
            total = mech_a + mech_b + mech_c
            if total > 0:
                mech_a = mech_a / total
                mech_b = mech_b / total
                mech_c = mech_c / total
            else:
                # Fallback als alle scores 0 zijn
                mech_c = 1.0
            
            return {
                'post_id': post_id,
                'impact_likelihood': clamp(result.get('impact_likelihood')),
                'suspicion_score': clamp(result.get('suspicion_score')),
                'sentiment_intensity': clamp(result.get('sentiment_intensity')),
                'news_value': clamp(result.get('news_value')),
                'mechanism_a_score': round(mech_a, 3),
                'mechanism_b_score': round(mech_b, 3),
                'mechanism_c_score': round(mech_c, 3),
                'likely_mechanisms': str(result.get('likely_mechanisms', 'C'))[:10],
                'suspicious_elements': result.get('suspicious_elements'),
                'detected_numbers': result.get('detected_numbers'),
                'unusual_phrases': result.get('unusual_phrases'),
                'reasoning': result.get('reasoning', ''),
                'confidence_calibration': clamp(result.get('confidence_calibration')),
                'model': self.llm_model
            }
        except Exception as e:
            logger.error(f"Failed to process LLM result for {post_id}: {e}")
            return None
    
    def save_analysis(self, analysis: Dict):
        """
        Sla analyse op in database.
        
        Args:
            analysis: Analysis result dict
        """
        query = """
            INSERT INTO truth.post_analysis (
                post_id, impact_likelihood, suspicion_score, sentiment_intensity,
                news_value, mechanism_a_score, mechanism_b_score, mechanism_c_score,
                likely_mechanisms, suspicious_elements, detected_numbers, unusual_phrases,
                reasoning, model, analyzed_at, confidence_calibration
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (post_id) DO UPDATE SET
                impact_likelihood = EXCLUDED.impact_likelihood,
                suspicion_score = EXCLUDED.suspicion_score,
                sentiment_intensity = EXCLUDED.sentiment_intensity,
                news_value = EXCLUDED.news_value,
                mechanism_a_score = EXCLUDED.mechanism_a_score,
                mechanism_b_score = EXCLUDED.mechanism_b_score,
                mechanism_c_score = EXCLUDED.mechanism_c_score,
                likely_mechanisms = EXCLUDED.likely_mechanisms,
                suspicious_elements = EXCLUDED.suspicious_elements,
                detected_numbers = EXCLUDED.detected_numbers,
                unusual_phrases = EXCLUDED.unusual_phrases,
                reasoning = EXCLUDED.reasoning,
                model = EXCLUDED.model,
                analyzed_at = EXCLUDED.analyzed_at,
                confidence_calibration = EXCLUDED.confidence_calibration,
                analysis_version = truth.post_analysis.analysis_version + 1
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (
                analysis['post_id'],
                analysis['impact_likelihood'],
                analysis['suspicion_score'],
                analysis['sentiment_intensity'],
                analysis['news_value'],
                analysis['mechanism_a_score'],
                analysis['mechanism_b_score'],
                analysis['mechanism_c_score'],
                analysis['likely_mechanisms'],
                analysis['suspicious_elements'],
                analysis['detected_numbers'],
                analysis['unusual_phrases'],
                analysis['reasoning'],
                analysis['model'],
                datetime.now(timezone.utc),
                analysis['confidence_calibration']
            ))
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.debug(f"Saved analysis for post {analysis['post_id']}")
        except Exception as e:
            logger.error(f"Failed to save analysis: {e}")
    
    def run_test(self) -> Dict:
        """
        Test mode: analyseer 1 willekeurige post uit een cluster.
        
        Returns:
            Dict met analyse resultaat
        """
        logger.info("=== TEST MODE: Analyzing single random post ===")
        
        # Check LLM availability
        if not self.check_llm_available():
            logger.error("LLM service not available. Start LM Studio first.")
            return {'error': 'LLM not available'}
        
        # Zorg dat model geladen is met juiste GPU layers
        if not self.ensure_correct_gpu_layers():
            logger.error("Failed to load model with correct GPU layers")
            return {'error': 'Model load failed'}
        
        # Haal willekeurige post
        post = self.get_random_post_from_cluster()
        if not post:
            logger.error("No posts found in clusters")
            return {'error': 'No posts found'}
        
        logger.info(f"Selected post: ID={post['post_id']}")
        logger.info(f"Timestamp: {post['created_at']}")
        logger.info(f"Type: {post['post_type']}")
        logger.info(f"Content: {(post['content_plain'] or '')[:200]}...")
        
        # Analyseer
        logger.info("\nCalling LLM...")
        start_time = datetime.now()
        
        # REASON: Gebruik timeout uit config (geen hardcoded waarde)
        analysis = self.analyze_post(post, timeout=None)  # None = gebruik self.timeout uit config
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"LLM response received in {elapsed:.1f}s")
        
        if analysis:
            logger.info(f"\n=== LLM Analysis Result ===")
            logger.info(f"Impact likelihood: {analysis['impact_likelihood']:.3f}")
            logger.info(f"Suspicion score: {analysis['suspicion_score']:.3f}")
            logger.info(f"Sentiment intensity: {analysis['sentiment_intensity']:.3f}")
            logger.info(f"News value: {analysis['news_value']:.3f}")
            logger.info(f"Mechanism A (public): {analysis['mechanism_a_score']:.3f}")
            logger.info(f"Mechanism B (insider): {analysis['mechanism_b_score']:.3f}")
            logger.info(f"Mechanism C (noise): {analysis['mechanism_c_score']:.3f}")
            logger.info(f"Likely mechanism: {analysis['likely_mechanisms']}")
            logger.info(f"Reasoning: {analysis['reasoning']}")
            
            if analysis['suspicious_elements']:
                logger.info(f"Suspicious elements: {analysis['suspicious_elements']}")
            if analysis['detected_numbers']:
                logger.info(f"Detected numbers: {analysis['detected_numbers']}")
            
            # Sla op
            self.save_analysis(analysis)
            logger.info("\nAnalysis saved to database")
            
            return {'success': True, 'analysis': analysis, 'elapsed_seconds': elapsed}
        else:
            logger.error("LLM analysis failed")
            return {'error': 'LLM analysis failed'}
    
    def run(self, limit: Optional[int] = None) -> Dict:
        """
        Analyseer alle pending posts.
        
        Args:
            limit: Maximum aantal posts om te analyseren
            
        Returns:
            Dict met statistieken
        """
        logger.info("Starting Phase 1: Per-Post LLM Analysis...")
        
        # Check LLM availability
        if not self.check_llm_available():
            logger.error("LLM service not available. Start LM Studio first.")
            return {'error': 'LLM not available'}
        
        # Zorg dat model geladen is met juiste GPU layers
        if not self.ensure_correct_gpu_layers():
            logger.error("Failed to load model with correct GPU layers")
            return {'error': 'Model load failed'}
        
        # Haal posts op
        posts = self.get_posts_to_analyze(limit)
        
        if not posts:
            logger.info("No pending posts to analyze")
            return {'analyzed': 0, 'errors': 0}
        
        # Analyseer posts
        analyzed = 0
        errors = 0
        start_time = datetime.now()
        
        for i, post in enumerate(posts):
            post_id = post['post_id']
            content_preview = (post['content_plain'] or '')[:50]
            
            # Progress en ETA
            if analyzed > 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = analyzed / elapsed
                remaining = len(posts) - i
                eta_seconds = remaining / rate if rate > 0 else 0
                eta_str = f", ETA: {eta_seconds/60:.1f}m" if eta_seconds > 60 else f", ETA: {eta_seconds:.0f}s"
            else:
                eta_str = ""
            
            logger.info(f"[{i+1}/{len(posts)}] Analyzing post {post_id[:20]}...{eta_str}")
            logger.debug(f"  Content: {content_preview}...")
            
            # Send ECG heartbeat periodically
            if time.time() - self._last_heartbeat >= self.heartbeat_interval:
                self._heartbeat_stats = {
                    "posts_analyzed": analyzed,
                    "errors": errors,
                    "progress": f"{i+1}/{len(posts)}",
                }
                self._send_heartbeat()
            
            # Time individual post analysis
            post_start = datetime.now()
            analysis = self.analyze_post(post)
            post_duration = (datetime.now() - post_start).total_seconds()
            
            if analysis:
                self.save_analysis(analysis)
                analyzed += 1
                
                # Performance check
                perf_status = self.check_performance(post_duration)
                
                # Format duration string
                if perf_status == 'critical':
                    duration_str = f"🔴 {post_duration:.1f}s"
                elif perf_status == 'recover':
                    duration_str = f"📈 {post_duration:.1f}s"
                else:
                    duration_str = f"{post_duration:.1f}s"
                
                logger.info(f"  -> [{duration_str}] [GPU:{self.current_gpu_layers}] "
                          f"impact={analysis['impact_likelihood']:.2f}, "
                          f"suspicion={analysis['suspicion_score']:.2f}, "
                          f"mechanism={analysis['likely_mechanisms']}")
                
                # Handle critical performance degradation
                if perf_status == 'critical':
                    if self.reload_model():
                        logger.info("Model reloaded, continuing analysis...")
                    else:
                        logger.error("Model reload failed! Consider manual LM Studio restart.")
                        # Continue anyway, might recover
                
                # Handle recovery (verhoog GPU layers na 10 goede analyses)
                elif perf_status == 'recover':
                    if self.increase_gpu_layers():
                        logger.info("GPU layers verhoogd, continuing analysis...")
                    else:
                        logger.warning("GPU layer increase failed, continuing with current settings...")
            else:
                errors += 1
                logger.warning(f"  -> Failed to analyze post {post_id}")
        
        total_time = (datetime.now() - start_time).total_seconds()
        
        stats = {
            'total_posts': len(posts),
            'analyzed': analyzed,
            'errors': errors,
            'elapsed_seconds': total_time,
            'rate_per_minute': analyzed / (total_time / 60) if total_time > 0 else 0,
            'model_reloads': self.total_reloads,
            'final_gpu_layers': self.current_gpu_layers
        }
        
        logger.info(f"\n=== Phase 1 Analysis Complete ===")
        logger.info(f"Analyzed: {analyzed}")
        logger.info(f"Errors: {errors}")
        logger.info(f"Model reloads: {self.total_reloads}")
        logger.info(f"Final GPU layers: {self.current_gpu_layers}/{self.MODEL_TOTAL_LAYERS}")
        logger.info(f"Time: {total_time/60:.1f} minutes")
        logger.info(f"Rate: {stats['rate_per_minute']:.1f} posts/minute")
        logger.info(f"Max allowed duration: {self.MAX_DURATION_SECONDS}s/post")
        
        # Toon nog niet geanalyseerde posts
        self._show_remaining_posts()
        
        return stats
    
    def _show_remaining_posts(self):
        """Toon aantal posts dat nog geanalyseerd moet worden."""
        query = """
            SELECT COUNT(DISTINCT p.post_id) as remaining
            FROM truth.posts p
            INNER JOIN truth.cluster_posts cp ON p.post_id = cp.post_id
            LEFT JOIN truth.post_analysis pa ON p.post_id = pa.post_id
            WHERE pa.post_id IS NULL
              AND p.content_plain IS NOT NULL
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            remaining = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            if remaining > 0:
                logger.info(f"\n[!] Nog {remaining} posts te analyseren")
            else:
                logger.info("\n[OK] Alle posts in clusters zijn geanalyseerd!")
                
        except Exception as e:
            logger.error(f"Failed to count remaining posts: {e}")
    
    def check_llm_available(self) -> bool:
        """Check of LLM service beschikbaar is."""
        try:
            with httpx.Client(timeout=5) as client:
                response = client.get(f"{self.llm_base_url}/models")
                return response.status_code == 200
        except Exception:
            return False
    
    def ensure_correct_gpu_layers(self) -> bool:
        """
        Zorg dat het model geladen is met de juiste GPU layers.
        
        ALTIJD unload + reload om correcte layers te garanderen.
        
        Returns:
            True als model correct geladen is
        """
        logger.info(f"Loading model with {self.current_gpu_layers}/{self.MODEL_TOTAL_LAYERS} GPU layers...")
        return self._load_model_with_layers(self.current_gpu_layers)
    
    def _load_model_with_layers(self, target_layers: int) -> bool:
        """
        Laad model met specifiek aantal GPU layers via lms CLI.
        Unload eerst bestaande modellen.
        """
        # Eject huidige model
        self.eject_model()
        time.sleep(3)
        
        # Bereken GPU offload ratio voor lms CLI
        gpu_ratio = (target_layers + 0.5) / self.MODEL_TOTAL_LAYERS
        
        try:
            cmd = [
                "lms", "load",
                self.llm_model,
                "--gpu", f"{gpu_ratio:.2f}",
                "--context-length", "4096",
                "--yes"
            ]
            
            logger.info(f"Running: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
                encoding='utf-8',
                errors='replace'
            )
            
            if result.returncode == 0:
                logger.info(f"Model loaded with {target_layers}/{self.MODEL_TOTAL_LAYERS} GPU layers")
                logger.info("Waiting for model to initialize...")
                time.sleep(10)
                return self._verify_model_loaded()
            else:
                logger.error(f"lms load failed: {result.stderr}")
                return False
                
        except FileNotFoundError:
            logger.warning("lms CLI not found - using model as-is from LM Studio")
            return True
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

    def eject_model(self) -> bool:
        """
        Eject (unload) het huidige model uit LM Studio via lms CLI.
        """
        try:
            result = subprocess.run(
                ["lms", "unload", "--all"],
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='replace'
            )
            
            if result.returncode == 0:
                logger.info(f"Model {self.llm_model} ejected successfully")
                return True
            else:
                logger.warning(f"Model eject returned: {result.stderr}")
                return True  # Ga toch door
                
        except FileNotFoundError:
            logger.warning("lms CLI not found for unload")
            return True
        except Exception as e:
            logger.error(f"Failed to eject model: {e}")
            return False
    
    def reload_model(self, reduce_layers: bool = True) -> bool:
        """
        Reload het model met aangepast aantal GPU layers.
        
        Args:
            reduce_layers: Als True, verminder GPU layers met GPU_REDUCE_ON_RELOAD
        
        Returns:
            True als reload succesvol
        """
        self.total_reloads += 1
        
        # Verminder GPU layers als dat gevraagd is
        if reduce_layers:
            old_layers = self.current_gpu_layers
            self.current_gpu_layers = max(
                self.MIN_GPU_LAYERS,
                self.current_gpu_layers - self.GPU_REDUCE_ON_RELOAD
            )
            logger.warning(f"=== RELOADING MODEL ({old_layers} → {self.current_gpu_layers} GPU layers) ===")
        else:
            logger.info(f"=== RELOADING MODEL ({self.current_gpu_layers} GPU layers) ===")
        
        # Reset consecutive fast analyses
        self.consecutive_fast_analyses = 0
        
        # Laad model met nieuwe layers
        if self._load_model_with_layers(self.current_gpu_layers):
            return True
        
        # Fallback naar API reload
        return self._reload_model_via_api()
    
    def increase_gpu_layers(self) -> bool:
        """
        Verhoog GPU layers na succesvolle herstelperiode.
        
        Returns:
            True als reload succesvol
        """
        if self.current_gpu_layers >= self.MAX_GPU_LAYERS:
            logger.info(f"GPU layers al op maximum ({self.MAX_GPU_LAYERS})")
            self.consecutive_fast_analyses = 0
            return True
        
        old_layers = self.current_gpu_layers
        self.current_gpu_layers = min(
            self.MAX_GPU_LAYERS,
            self.current_gpu_layers + self.GPU_INCREASE_ON_RECOVERY
        )
        
        logger.info(f"📈 RECOVERY: {self.consecutive_fast_analyses} snelle analyses → "
                   f"GPU layers verhogen ({old_layers} → {self.current_gpu_layers})")
        
        # Reset teller
        self.consecutive_fast_analyses = 0
        
        # Reload met meer layers
        return self.reload_model(reduce_layers=False)
    
    def _reload_model_via_api(self) -> bool:
        """Fallback: reload model via simpele API request (auto-load)."""
        logger.info("Attempting API-based reload (auto-load)...")
        try:
            with httpx.Client(timeout=180) as client:
                response = client.post(
                    f"{self.llm_base_url}/chat/completions",
                    json={
                        "model": self.llm_model,
                        "messages": [{"role": "user", "content": "test"}],
                        "max_tokens": 5
                    }
                )
                if response.status_code == 200:
                    logger.info("Model reloaded via API (default settings)")
                    # NB: Baseline wordt NIET gereset - blijft behouden tijdens hele run
                    return True
        except Exception as e:
            logger.error(f"API reload failed: {e}")
        return False
    
    def _verify_model_loaded(self, max_retries: int = 3) -> bool:
        """
        Verifieer dat het model geladen is met een test request.
        Probeert meerdere keren met pauzes.
        """
        for attempt in range(max_retries):
            try:
                with httpx.Client(timeout=60) as client:
                    response = client.post(
                        f"{self.llm_base_url}/chat/completions",
                        json={
                            "model": self.llm_model,
                            "messages": [{"role": "user", "content": "test"}],
                            "max_tokens": 5
                        }
                    )
                    if response.status_code == 200:
                        logger.info("Model verification successful")
                        return True
                    else:
                        logger.warning(f"Model verification attempt {attempt+1}/{max_retries}: status {response.status_code}")
            except Exception as e:
                logger.warning(f"Model verification attempt {attempt+1}/{max_retries}: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(5)  # Wacht voor volgende poging
        
        return False
    
    def check_performance(self, duration: float) -> str:
        """
        Check performance - simpele logica.
        
        >60s = te traag, reload met -3 layers
        10x <60s = verhoog +1 layer
        
        Returns:
            'ok', 'critical', of 'recover'
        """
        # Te traag? -> reload nodig
        if duration > self.MAX_DURATION_SECONDS:
            logger.error(f"🔴 TOO SLOW: {duration:.1f}s (>{self.MAX_DURATION_SECONDS}s)")
            self.consecutive_fast_analyses = 0
            return 'critical'
        
        # Snel genoeg -> tel op
        self.consecutive_fast_analyses += 1
        
        # Na 10 snelle analyses, probeer layers te verhogen
        if (self.consecutive_fast_analyses >= self.RECOVERY_THRESHOLD and 
            self.current_gpu_layers < self.MAX_GPU_LAYERS):
            return 'recover'
        
        return 'ok'


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze individual posts with LLM (Phase 1)')
    parser.add_argument('--limit', type=int, help='Maximum number of posts to analyze')
    parser.add_argument('--check', action='store_true', help='Only check LLM availability')
    parser.add_argument('--test', action='store_true', help='Test mode: analyze single random post')
    args = parser.parse_args()
    
    analyzer = PostAnalyzerPhase1()
    
    if args.check:
        if analyzer.check_llm_available():
            logger.info("LLM service is available")
            sys.exit(0)
        else:
            logger.error("LLM service is NOT available")
            sys.exit(1)
    
    if args.test:
        result = analyzer.run_test()
        if 'error' in result:
            sys.exit(1)
        sys.exit(0)
    
    stats = analyzer.run(limit=args.limit)
    
    if 'error' in stats:
        sys.exit(1)
    
    logger.info("\nDone! Results saved to truth.post_analysis")


if __name__ == "__main__":
    main()

