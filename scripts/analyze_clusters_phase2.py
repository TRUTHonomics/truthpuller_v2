"""
# INSTRUCTIONS:
# Dit script vergelijkt posts binnen clusters met een LLM (Fase 3 van pipeline).
# Gebruikt Fase 2 scores (uit truth.post_analysis) voor efficiente vergelijking.
#
# Vereist: LM Studio draait (model wordt automatisch geladen via lms CLI)
# Model: qwen2.5-32b-instruct-i1 (44 GPU layers, 65 totaal)
#
# Draai vanuit de truthpuller_v2 directory met actieve venv:
#   python scripts/analyze_clusters_phase2.py
#   python scripts/analyze_clusters_phase2.py --limit 100     # Max aantal clusters
#   python scripts/analyze_clusters_phase2.py --check         # Check LLM beschikbaarheid
#   python scripts/analyze_clusters_phase2.py --test          # Test met 1 cluster
#
# Features:
# - Automatische GPU layer management (44 layers start, max 44)
# - Auto-reload bij performance degradatie (>120s per cluster)
# - ECG heartbeat monitoring
#
# Output: trigger selectie, rankings en review prioriteit in truth.post_clusters
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
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
import yaml
import httpx
import psycopg2
from psycopg2.extras import RealDictCursor

# Prompt loader voor externe prompts
from prompt_loader import get_system_prompt, get_phase2_prompt

# Voeg parent directory toe aan path voor config imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.prompts.schemas import get_response_format

# Onderdruk warnings
warnings.filterwarnings('ignore')


def setup_logging() -> logging.Logger:
    """Setup logging met file output en archivering."""
    script_dir = Path(__file__).parent.parent
    log_dir = script_dir / '_log'
    archive_dir = log_dir / 'archive'
    
    log_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / 'analyze_clusters_phase2.log'
    
    if log_file.exists():
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        archive_path = archive_dir / f'analyze_clusters_phase2_{timestamp}.log'
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


def ensure_ssh_tunnel(ssh_host: str = "10.10.10.1", ssh_user: str = "bart", 
                      ssh_key: str = r"F:\Containers\.ssh\id_rsa",
                      local_port: int = 15432, remote_port: int = 5432) -> bool:
    """
    Zorg dat SSH tunnel actief is voor database connectie.
    
    REASON: PostgreSQL draait op 10.10.10.1 (mogelijk in Docker container).
    Als PostgreSQL in container draait, moet container poort 5432 exposed zijn naar host poort 5432.
    SSH tunnel wijst naar localhost:5432 op remote server, wat doorstuurt naar container.
    
    Args:
        ssh_host: SSH server host (default: 10.10.10.1)
        ssh_user: SSH user (default: bart)
        ssh_key: Path naar SSH private key (default: F:\\Containers\\.ssh\\id_rsa)
        local_port: Lokale tunnel poort (default: 15432)
        remote_port: Remote database poort op host (default: 5432, moet gemapped zijn naar container)
    
    Returns:
        True als tunnel actief is of succesvol is opgezet, False anders
    """
    import socket
    
    # Check of tunnel al actief is
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('127.0.0.1', local_port))
        sock.close()
        if result == 0:
            logger.info(f"SSH tunnel al actief op poort {local_port}")
            return True
    except Exception as e:
        logger.debug(f"Tunnel check fout: {e}")
    
    # SSH key path controleren
    ssh_key_path = Path(ssh_key)
    if not ssh_key_path.exists():
        logger.error(f"SSH key niet gevonden: {ssh_key}")
        logger.error("💡 Controleer of SSH key bestaat op: F:\\Containers\\.ssh\\id_rsa")
        return False
    
    # Start SSH tunnel in achtergrond
    logger.info(f"Start SSH tunnel: {ssh_user}@{ssh_host} -> localhost:{local_port}")
    try:
        ssh_process = subprocess.Popen(
            [
                'ssh',
                '-i', str(ssh_key_path),
                '-N',  # Geen remote commando
                '-L', f'{local_port}:localhost:{remote_port}',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'PreferredAuthentications=publickey',
                '-o', 'PasswordAuthentication=no',
                '-o', 'ServerAliveInterval=60',
                f'{ssh_user}@{ssh_host}'
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wacht kort en check of tunnel actief is
        time.sleep(2)
        
        # Check of proces nog draait
        if ssh_process.poll() is not None:
            # Proces is gestopt - lees error
            _, stderr = ssh_process.communicate()
            error_msg = stderr.decode('utf-8', errors='ignore')
            logger.error(f"SSH tunnel gefaald: {error_msg}")
            logger.error("💡 Controleer:")
            logger.error("   1. SSH key permissions (alleen eigenaar)")
            logger.error("   2. Public key staat op server (~/.ssh/authorized_keys)")
            logger.error("   3. SSH service draait op 10.10.10.1")
            logger.error("   4. Firewall blokkeert SSH niet")
            return False
        
        # Check of poort nu open is
        max_retries = 5
        for i in range(max_retries):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(('127.0.0.1', local_port))
                sock.close()
                if result == 0:
                    logger.info(f"✅ SSH tunnel actief op poort {local_port}")
                    return True
            except Exception:
                pass
            time.sleep(1)
        
        logger.error(f"SSH tunnel gestart maar poort {local_port} niet bereikbaar na {max_retries} pogingen")
        ssh_process.terminate()
        return False
        
    except FileNotFoundError:
        logger.error("SSH command niet gevonden - installeer OpenSSH of gebruik WSL")
        return False
    except Exception as e:
        logger.error(f"Fout bij opzetten SSH tunnel: {e}")
        return False


class ClusterAnalyzerPhase2:
    """
    Vergelijkt posts binnen clusters om trigger te bepalen (Fase 3).
    
    Gebruikt Fase 2 scores en timing om:
    - Trigger post te selecteren
    - Posts te ranken binnen cluster
    - Review prioriteit te berekenen
    """
    
    # Performance thresholds (simpel)
    MAX_DURATION_SECONDS = 120    # >120s = te traag, reload nodig (clusters zijn groter dan posts)
    RECOVERY_THRESHOLD = 5        # 5x <120s = verhoog layers
    
    # GPU offload configuratie (in layers)
    # Model: Qwen2.5-32B heeft 65 layers totaal
    MODEL_TOTAL_LAYERS = 65
    START_GPU_LAYERS = 43         # Start layers op GPU (lager dan phase1 vanwege grotere context)
    MAX_GPU_LAYERS = 43           # Maximum layers op GPU (44 veroorzaakt VRAM overcommit bij clusters)
    MIN_GPU_LAYERS = 32           # Minimum layers op GPU
    GPU_REDUCE_ON_RELOAD = 1      # Layers verminderen bij reload
    GPU_INCREASE_ON_RECOVERY = 1  # Layers verhogen bij herstel
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize analyzer."""
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config' / 'truth_config.yaml'
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # REASON: Database draait op 10.10.10.1 (mogelijk in Docker container)
        # Script draait op Windows (10.10.10.2) en gebruikt SSH tunnel naar localhost:15432
        # Als PostgreSQL in container draait, moet container poort 5432 exposed zijn naar host poort 5432
        # SSH tunnel: -L 15432:localhost:5432 wijst naar host poort 5432, die doorstuurt naar container
        db_host = self.config['database']['host']
        if db_host == 'localhost':
            db_host = '127.0.0.1'  # Gebruik IPv4 expliciet
        
        # Remote database poort (op host, moet gemapped zijn naar container)
        remote_db_port = self.config['database'].get('remote_port', 5432)
        
        # Zorg dat SSH tunnel actief is
        if not ensure_ssh_tunnel(remote_port=remote_db_port):
            logger.warning("⚠️  SSH tunnel niet actief - database connectie kan falen")
            logger.warning("💡 Start handmatig: ssh -i \"F:\\Containers\\.ssh\\id_rsa\" -N -L 15432:localhost:5432 bart@10.10.10.1")
            logger.warning("💡 Als PostgreSQL in container draait, zorg dat container poort 5432 exposed is naar host poort 5432")
        
        self.db_config = {
            'host': db_host,
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
        self.max_tokens = analyzer_cfg.get('max_tokens', 1024)  # Meer tokens voor rankings
        self.timeout = llm_cfg.get('timeout', 600)
        self.base_context_length = analyzer_cfg.get('context_length', 10240)
        self.max_context_length = analyzer_cfg.get('max_context_length', 24576)
        self.current_context_length = self.base_context_length
        self.last_token_error: Optional[Dict[str, int]] = None
        self._context_retry_in_progress = False
        self.max_prompt_chars = analyzer_cfg.get('max_prompt_chars', 12000)  # REASON: contextbudget voor grote clusters
        
        # ECG Heartbeat
        self.heartbeat_interval = 60  # seconds
        self.heartbeat_enabled = True
        self._last_heartbeat = 0
        self._heartbeat_stats = {"clusters_analyzed": 0, "errors": 0}
        
        # Performance tracking
        self.total_reloads = 0
        self.current_gpu_layers = self.START_GPU_LAYERS
        self.consecutive_fast_analyses = 0
        
        logger.info(f"Using LLM model: {self.llm_model}")
        logger.info(f"Target GPU layers: {self.current_gpu_layers}/{self.MODEL_TOTAL_LAYERS}")
    
    def get_connection(self, retries: int = 3) -> psycopg2.extensions.connection:
        """
        Get database connection met retry logic.
        
        REASON: Als PostgreSQL in container draait, moet container poort 5432 exposed zijn naar host.
        SSH tunnel wijst naar localhost:5432 op remote server, wat doorstuurt naar container.
        
        Args:
            retries: Aantal retry pogingen bij connectie fout
        
        Returns:
            Database connection
            
        Raises:
            psycopg2.OperationalError: Als connectie na retries nog steeds faalt
        """
        for attempt in range(retries):
            try:
                return psycopg2.connect(**self.db_config)
            except psycopg2.OperationalError as e:
                if attempt < retries - 1:
                    logger.warning(f"Database connectie gefaald (poging {attempt + 1}/{retries}): {e}")
                    logger.info("Wacht 2 seconden en probeer opnieuw...")
                    time.sleep(2)
                    # Probeer tunnel opnieuw op te zetten
                    ensure_ssh_tunnel()
                else:
                    logger.error(f"Database connectie definitief gefaald na {retries} pogingen: {e}")
                    logger.error("💡 Controleer:")
                    logger.error("   1. SSH tunnel draait: ssh -i \"F:\\Containers\\.ssh\\id_rsa\" -N -L 15432:localhost:5432 bart@10.10.10.1")
                    logger.error("   2. Database draait op 10.10.10.1")
                    logger.error("   3. Als PostgreSQL in container draait:")
                    logger.error("      - Container poort 5432 moet exposed zijn naar host poort 5432")
                    logger.error("      - Check: docker ps | grep postgres (of docker-compose ps)")
                    logger.error("      - Check: netstat -tlnp | grep 5432 op 10.10.10.1")
                    logger.error("   4. Firewall blokkeert connectie niet")
                    raise
    
    def _send_heartbeat(self, metadata: dict = None) -> bool:
        """Stuur heartbeat naar ECG database."""
        if not self.heartbeat_enabled:
            return True
        
        try:
            now = datetime.now(timezone.utc)
            meta = metadata or {}
            meta["interval_seconds"] = self.heartbeat_interval
            meta.update(self._heartbeat_stats)
            
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
                "analyze_clusters_phase2",
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
    
    def get_clusters_to_analyze(self, limit: Optional[int] = None) -> List[Dict]:
        """
        Haal clusters op waar alle posts Fase 2 analyse hebben.
        
        Args:
            limit: Maximum aantal clusters
            
        Returns:
            List van cluster dicts
        """
        query = """
            SELECT 
                pc.cluster_id,
                pc.cluster_group,
                pc.interval,
                pc.trigger_kline_time,
                pc.trigger_kline_close,
                pc.trigger_sigma,
                pc.trigger_return_pct,
                pc.post_count,
                pc.max_sigma_value
            FROM truth.post_clusters pc
            WHERE pc.status = 'pending'
              AND pc.post_count > 0
              -- Alle posts in cluster moeten Fase 2 analyse hebben
              AND NOT EXISTS (
                  SELECT 1 FROM truth.cluster_posts cp
                  LEFT JOIN truth.post_analysis pa ON cp.post_id = pa.post_id
                  WHERE cp.cluster_id = pc.cluster_id
                    AND pa.post_id IS NULL
              )
            ORDER BY pc.trigger_sigma DESC, pc.post_count DESC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            clusters = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(clusters)} clusters ready for Phase 2 analysis")
            return clusters
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            return []
    
    def get_cluster_posts_with_scores(self, cluster_id: int) -> List[Dict]:
        """
        Haal posts van een cluster op met hun Fase 2 scores.
        
        Args:
            cluster_id: Cluster ID
            
        Returns:
            List van post dicts met scores
        """
        query = """
            SELECT 
                cp.post_id,
                cp.position_in_cluster,
                p.created_at,
                p.content_plain,
                pa.impact_likelihood,
                pa.suspicion_score,
                pa.sentiment_intensity,
                pa.news_value,
                pa.mechanism_a_score,
                pa.mechanism_b_score,
                pa.mechanism_c_score,
                pa.likely_mechanisms,
                pa.suspicious_elements,
                pa.reasoning as phase1_reasoning
            FROM truth.cluster_posts cp
            JOIN truth.posts p ON cp.post_id = p.post_id
            LEFT JOIN truth.post_analysis pa ON cp.post_id = pa.post_id
            WHERE cp.cluster_id = %s
            ORDER BY p.created_at ASC
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, (cluster_id,))
            posts = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return posts
        except Exception as e:
            logger.error(f"Failed to get cluster posts: {e}")
            return []
    
    def build_prompt(self, cluster: Dict, posts: List[Dict]) -> str:
        """
        Bouw user prompt voor cluster vergelijking uit externe TOML.
        
        Args:
            cluster: Cluster metadata
            posts: Posts met scores
            
        Returns:
            User prompt string (system prompt wordt apart geladen)
        """
        # REASON: Dynamisch inkorten om context-limiet te respecteren bij grote clusters
        per_post_budget = max(
            80,
            min(200, (self.max_prompt_chars // max(len(posts), 1)) - 180)
        )
        truncated_count = 0
        
        # Posts sectie bouwen
        posts_lines = []
        for i, post in enumerate(posts, 1):
            raw_content = post.get('content_plain') or ''
            content = raw_content[:per_post_budget]
            if len(raw_content) > per_post_budget:
                content += '...'
                truncated_count += 1
            
            impact = post.get('impact_likelihood') or 0.5
            suspicion = post.get('suspicion_score') or 0.0
            mech_a = post.get('mechanism_a_score') or 0.3
            mech_b = post.get('mechanism_b_score') or 0.1
            mech_c = post.get('mechanism_c_score') or 0.6
            mechanisms = post.get('likely_mechanisms') or 'C'
            
            posts_lines.append(f"""
POST {i} [{post['post_id'][:15]}...]
Time: {post['created_at']}
Content: {content}
Phase 1 Scores:
  - impact_likelihood: {impact:.3f}
  - suspicion_score: {suspicion:.3f}
  - mechanism_a (public): {mech_a:.3f}
  - mechanism_b (insider): {mech_b:.3f}
  - mechanism_c (noise): {mech_c:.3f}
  - likely_mechanism: {mechanisms}
""")
        
        posts_section = "\n---\n".join(posts_lines)
        if truncated_count:
            logger.debug(
                f"Prompt ingekort voor {truncated_count} posts tot {per_post_budget} chars "
                f"(budget {self.max_prompt_chars})"
            )
        
        # REASON: Laad prompt uit externe TOML via prompt_loader
        return get_phase2_prompt(
            interval=cluster['interval'],
            return_pct=float(cluster['trigger_return_pct'] or 0),
            sigma=float(cluster['trigger_sigma'] or 0),
            trigger_time=str(cluster['trigger_kline_close']),
            post_count=len(posts),
            posts_section=posts_section
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
        response_format = get_response_format("cluster_comparison")
        
        try:
            self.last_token_error = None
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
                
        except httpx.HTTPStatusError as e:
            error_text = ""
            try:
                error_text = e.response.text or ""
            except Exception:
                pass

            # DEBUG: Log de volledige fouttekst om regex te kunnen verbeteren
            logger.info(f"Full error response: {error_text}")

            parsed = self._parse_token_overflow(error_text)
            if parsed:
                self.last_token_error = parsed
                logger.error(
                    f"LLM API error (context overflow): requested {parsed['requested']} > limit {parsed['limit']}"
                )
            else:
                logger.error(f"LLM API error: {e}")
                # Als het een 400 is en we geen overflow kunnen parsen, probeer dan toch een context retry
                if e.response.status_code == 400:
                    logger.warning("400 error detected, attempting context retry as fallback")
                    # Probeer een standaard context increase
                    if not self._context_retry_in_progress:
                        self.last_token_error = {"requested": self.current_context_length + 4096, "limit": self.current_context_length}
            return None
        except httpx.HTTPError as e:
            logger.error(f"LLM API error: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error calling LLM: {e}")
            return None
    
    def _parse_token_overflow(self, text: str) -> Optional[Dict[str, int]]:
        """Detecteer context overflow uit foutmelding."""
        if not text:
            return None
        try:
            # Probeer eerst JSON te parsen als het een JSON error response is
            try:
                json_data = json.loads(text)
                if isinstance(json_data, dict) and "error" in json_data:
                    text = json_data["error"]
            except json.JSONDecodeError:
                pass  # Geen JSON, ga door met text parsing

            # Patroon 1: LM Studio specifieke fout - "Trying to keep the first X tokens when context overflows. However, the model is loaded with context length of only Y tokens"
            match = re.search(
                r"Trying to keep the first\s+(\d+)\s+tokens.*?loaded with context length of only\s+(\d+)\s+tokens",
                text,
                flags=re.IGNORECASE | re.DOTALL
            )
            if match:
                return {"requested": int(match.group(1)), "limit": int(match.group(2))}

            # Patroon 2: "maximum context length is X, you requested Y"
            match = re.search(
                r"maximum context length is\s+(\d+).*?you requested\s+(\d+)",
                text,
                flags=re.IGNORECASE | re.DOTALL
            )
            if match:
                return {"limit": int(match.group(1)), "requested": int(match.group(2))}

            # Patroon 3: "context length X requested Y" of varianten
            match = re.search(
                r"context length[^0-9]*(\d+)[^0-9]+request(ed)?[^0-9]*(\d+)",
                text,
                flags=re.IGNORECASE | re.DOTALL
            )
            if match:
                return {"limit": int(match.group(1)), "requested": int(match.group(3))}

            # Patroon 4: "requested X exceeds limit Y" of "X > Y"
            match = re.search(
                r"request(ed)?\s+(\d+).*?(exceeds|>\s*|over).*?(\d+)",
                text,
                flags=re.IGNORECASE | re.DOTALL
            )
            if match:
                return {"requested": int(match.group(2)), "limit": int(match.group(4))}

            # Patroon 5: Alleen nummers in context van tokens/length
            if "context" in text.lower() or "token" in text.lower():
                numbers = re.findall(r'\d+', text)
                if len(numbers) >= 2:
                    # Neem de laatste twee nummers als requested en limit
                    return {"requested": int(numbers[-2]), "limit": int(numbers[-1])}

        except Exception as e:
            logger.debug(f"Error parsing token overflow: {e}")
            return None
        return None
    
    def calculate_review_priority(self, analysis: Dict, cluster: Dict, posts: List[Dict]) -> Tuple[int, str]:
        """
        Bereken review prioriteit op basis van uncertainty signals.
        
        Returns:
            Tuple van (priority_score, reason)
            Lager = hogere prioriteit
        """
        reasons = []
        priority = 100  # Start met lage prioriteit
        
        confidence = analysis.get('confidence', 0.5)
        mechanism = analysis.get('likely_mechanism', 'C')
        causation = analysis.get('causation_likelihood', 0.5)
        combined = analysis.get('combined_effect', False)
        
        # Factor 1: Lage confidence
        if confidence < 0.5:
            priority -= 40
            reasons.append(f"low_confidence ({confidence:.2f})")
        elif confidence < 0.7:
            priority -= 20
            reasons.append(f"medium_confidence ({confidence:.2f})")
        
        # Factor 2: Mechanism B (insider signals)
        if mechanism == 'B':
            priority -= 35
            reasons.append("mechanism_B")
        
        # Factor 3: Hoge max suspicion score in cluster
        max_suspicion = max((p.get('suspicion_score') or 0) for p in posts)
        if max_suspicion > 0.7:
            priority -= 30
            reasons.append(f"high_suspicion ({max_suspicion:.2f})")
        elif max_suspicion > 0.5:
            priority -= 15
            reasons.append(f"moderate_suspicion ({max_suspicion:.2f})")
        
        # Factor 4: Close race (score spread)
        rankings = analysis.get('rankings', [])
        if len(rankings) >= 2:
            scores = sorted([r.get('score', 0) for r in rankings], reverse=True)
            spread = scores[0] - scores[1] if len(scores) >= 2 else 1.0
            if spread < 0.15:
                priority -= 25
                reasons.append(f"close_race ({spread:.2f})")
        
        # Factor 5: Combined effect
        if combined:
            priority -= 15
            reasons.append("combined_effect")
        
        # Factor 6: Groot cluster
        if len(posts) > 10:
            priority -= 10
            reasons.append(f"large_cluster ({len(posts)})")
        
        # Factor 7: Lage causation likelihood
        if causation < 0.4:
            priority -= 20
            reasons.append(f"low_causation ({causation:.2f})")
        
        # Bereken score spread voor opslag
        if len(rankings) >= 2:
            scores = sorted([r.get('score', 0) for r in rankings], reverse=True)
            self._last_score_spread = scores[0] - scores[1] if len(scores) >= 2 else 1.0
        else:
            self._last_score_spread = 1.0
        
        reason_str = ", ".join(reasons) if reasons else "standard"
        return max(1, priority), reason_str
    
    def analyze_cluster(self, cluster: Dict, timeout: Optional[int] = None) -> Optional[Dict]:
        """
        Analyseer een enkel cluster.
        
        Args:
            cluster: Cluster metadata
            timeout: Override timeout
            
        Returns:
            Analysis result of None
        """
        cluster_id = cluster['cluster_id']
        
        # Haal posts met scores
        posts = self.get_cluster_posts_with_scores(cluster_id)
        if not posts:
            logger.warning(f"No posts found for cluster {cluster_id}")
            return None
        
        # Single post cluster: simplified handling
        if len(posts) == 1:
            post = posts[0]
            return {
                'cluster_id': cluster_id,
                'selected_post_id': post['post_id'],
                'selected_position': 1,
                'confidence': 0.8,  # Single post = high confidence it's the trigger
                'mechanism': str(post.get('likely_mechanisms', 'C'))[:1],  # REASON: Zorg dat mechanism altijd 1 karakter is
                'causation_likelihood': post.get('impact_likelihood', 0.5),
                'combined_effect': False,
                'reasoning': "Single post in cluster - automatic selection",
                'rankings': [{'position': 1, 'post_id': post['post_id'], 'score': 1.0}],
                'runner_up_position': None,
                'review_priority': 80,  # Low priority for single posts
                'review_reason': "single_post"
            }
        
        # Bouw prompt
        prompt = self.build_prompt(cluster, posts)
        logger.debug(f"Prompt length: {len(prompt)} chars")
        
        # Call LLM
        result = self.call_llm(prompt, timeout=timeout)
        
        # Eenmalige retry met grotere context als er overflow-info is
        if (not result and self.last_token_error and
                not self._context_retry_in_progress):
            requested = self.last_token_error.get('requested')
            limit = self.last_token_error.get('limit')
            logger.info(f"Context retry condition met: requested={requested}, limit={limit}, current_ctx={self.current_context_length}")
            target_ctx = min(
                self.max_context_length,
                max(self.base_context_length, (requested or 0) + 512)
            )

            logger.info(f"Target context calculated: {target_ctx} (max: {self.max_context_length})")

            if target_ctx > self.current_context_length:
                logger.warning(
                    f"Context overflow voor cluster {cluster_id}: requested {requested} > limit {limit}. "
                    f"Reload model met context {target_ctx} en retry."
                )
                previous_ctx = self.current_context_length
                self._context_retry_in_progress = True
                try:
                    self.current_context_length = target_ctx
                    if self.reload_model(reduce_layers=False):
                        logger.info(f"Model reloaded with context {target_ctx}, retrying cluster {cluster_id}")
                        result = self.call_llm(prompt, timeout=timeout)
                        if result:
                            logger.info(f"Context retry successful voor cluster {cluster_id}")
                        else:
                            logger.warning(f"Context retry failed voor cluster {cluster_id}")
                    else:
                        logger.error("Reload met groter context faalde")
                finally:
                    if self.current_context_length != self.base_context_length:
                        logger.info(f"Resetting context back to {self.base_context_length} for next cluster")
                        self.current_context_length = self.base_context_length
                        if not self.reload_model(reduce_layers=False):
                            logger.warning("Terug naar basis context reload faalde, ga verder met huidige model")
                    self._context_retry_in_progress = False
            else:
                logger.info(f"Target context {target_ctx} <= current {self.current_context_length}, geen reload nodig")
            
        if not result:
            return None
        
        try:
            # Bereken review prioriteit
            priority, reason = self.calculate_review_priority(result, cluster, posts)
            
            # Valideer en map post_id
            selected_pos = result.get('selected_position', 1)
            if selected_pos < 1 or selected_pos > len(posts):
                selected_pos = 1
            
            selected_post = posts[selected_pos - 1]
            
            # Runner up
            runner_up_pos = result.get('runner_up_position')
            runner_up_id = None
            runner_up_conf = None
            if runner_up_pos and 1 <= runner_up_pos <= len(posts):
                runner_up_id = posts[runner_up_pos - 1]['post_id']
                rankings = result.get('rankings', [])
                for r in rankings:
                    if r.get('position') == runner_up_pos:
                        runner_up_conf = r.get('score')
                        break
            
            return {
                'cluster_id': cluster_id,
                'selected_post_id': selected_post['post_id'],
                'selected_position': selected_pos,
                'confidence': min(1.0, max(0.0, float(result.get('confidence', 0.5)))),
                'mechanism': str(result.get('likely_mechanism', 'C'))[:1],
                'causation_likelihood': min(1.0, max(0.0, float(result.get('causation_likelihood', 0.5)))),
                'combined_effect': bool(result.get('combined_effect', False)),
                'reasoning': result.get('reasoning', ''),
                'rankings': result.get('rankings', []),
                'runner_up_post_id': runner_up_id,
                'runner_up_confidence': runner_up_conf,
                'review_priority': priority,
                'review_reason': reason[:100],  # REASON: Beperk review_reason lengte om database fouten te voorkomen
                'score_spread': getattr(self, '_last_score_spread', 1.0)
            }
            
        except Exception as e:
            logger.error(f"Failed to process LLM result: {e}")
            return None
    
    def save_analysis(self, analysis: Dict, posts: List[Dict]):
        """
        Sla analyse resultaten op.
        
        Args:
            analysis: Analysis result
            posts: Posts in cluster voor ranking updates
        """
        cluster_id = analysis['cluster_id']
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Update post_clusters
            cursor.execute("""
                UPDATE truth.post_clusters SET
                    llm_suggested_post_id = %s,
                    llm_confidence = %s,
                    llm_reasoning = %s,
                    llm_mechanism = %s,
                    llm_causation_likelihood = %s,
                    llm_model = %s,
                    llm_analyzed_at = %s,
                    review_priority = %s,
                    review_reason = %s,
                    score_spread = %s,
                    runner_up_post_id = %s,
                    runner_up_confidence = %s,
                    combined_effect = %s,
                    status = 'llm_analyzed'
                WHERE cluster_id = %s
            """, (
                analysis['selected_post_id'],
                analysis['confidence'],
                analysis['reasoning'],
                analysis['mechanism'],
                analysis['causation_likelihood'],
                self.llm_model,
                datetime.now(timezone.utc),
                analysis['review_priority'],
                analysis['review_reason'],
                analysis.get('score_spread'),
                analysis.get('runner_up_post_id'),
                analysis.get('runner_up_confidence'),
                analysis.get('combined_effect', False),
                cluster_id
            ))
            
            # Update cluster_posts met rankings en is_trigger
            rankings = analysis.get('rankings', [])
            ranking_map = {r.get('post_id', ''): (r.get('position', 99), r.get('score', 0)) 
                          for r in rankings if r.get('post_id')}
            
            for post in posts:
                post_id = post['post_id']
                is_trigger = (post_id == analysis['selected_post_id'])
                
                # Zoek ranking info
                if post_id in ranking_map:
                    rank, score = ranking_map[post_id]
                else:
                    # Niet in rankings: assign default
                    rank = 99
                    score = 0.0
                
                cursor.execute("""
                    UPDATE truth.cluster_posts SET
                        is_trigger = %s,
                        rank_in_cluster = %s,
                        cluster_score = %s
                    WHERE cluster_id = %s AND post_id = %s
                """, (is_trigger, rank, score, cluster_id, post_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.debug(f"Saved analysis for cluster {cluster_id}")
            
        except Exception as e:
            logger.error(f"Failed to save analysis: {e}")
    
    def run_test(self) -> Dict:
        """Test mode: analyseer 1 cluster."""
        logger.info("=== TEST MODE: Analyzing single cluster ===")
        
        if not self.check_llm_available():
            return {'error': 'LLM not available'}
        
        # Zorg dat model geladen is met juiste GPU layers
        if not self.ensure_correct_gpu_layers():
            logger.error("Failed to load model with correct GPU layers")
            return {'error': 'Model load failed'}
        
        # Haal 1 cluster met meeste posts
        query = """
            SELECT 
                pc.cluster_id,
                pc.cluster_group,
                pc.interval,
                pc.trigger_kline_time,
                pc.trigger_kline_close,
                pc.trigger_sigma,
                pc.trigger_return_pct,
                pc.post_count,
                pc.max_sigma_value
            FROM truth.post_clusters pc
            WHERE pc.post_count > 1
              AND NOT EXISTS (
                  SELECT 1 FROM truth.cluster_posts cp
                  LEFT JOIN truth.post_analysis pa ON cp.post_id = pa.post_id
                  WHERE cp.cluster_id = pc.cluster_id
                    AND pa.post_id IS NULL
              )
            ORDER BY pc.post_count DESC
            LIMIT 1
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            cluster = cursor.fetchone()
            cursor.close()
            conn.close()
        except Exception as e:
            return {'error': f'Database error: {e}'}
        
        if not cluster:
            return {'error': 'No eligible cluster found'}
        
        cluster = dict(cluster)
        logger.info(f"Selected cluster {cluster['cluster_id']}: {cluster['post_count']} posts, sigma={cluster['trigger_sigma']}")
        
        # Haal posts
        posts = self.get_cluster_posts_with_scores(cluster['cluster_id'])
        logger.info(f"Posts in cluster: {len(posts)}")
        
        for i, p in enumerate(posts[:5], 1):
            logger.info(f"  {i}. {p['post_id'][:20]}... impact={p.get('impact_likelihood', 'N/A'):.3f}")
        
        # Analyseer
        logger.info("\nCalling LLM...")
        start = datetime.now(timezone.utc)
        
        analysis = self.analyze_cluster(cluster, timeout=600)
        
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"LLM response in {elapsed:.1f}s")
        
        if analysis:
            logger.info(f"\n=== Analysis Result ===")
            logger.info(f"Selected post: {analysis['selected_post_id']}")
            logger.info(f"Confidence: {analysis['confidence']:.2f}")
            logger.info(f"Mechanism: {analysis['mechanism']}")
            logger.info(f"Causation: {analysis['causation_likelihood']:.2f}")
            logger.info(f"Review priority: {analysis['review_priority']} ({analysis['review_reason']})")
            logger.info(f"Reasoning: {analysis['reasoning']}")
            
            # Sla op
            self.save_analysis(analysis, posts)
            logger.info("\nAnalysis saved to database")
            
            return {'success': True, 'analysis': analysis}
        else:
            return {'error': 'Analysis failed'}
    
    def run(self, limit: Optional[int] = None) -> Dict:
        """
        Analyseer alle pending clusters.
        
        Args:
            limit: Maximum aantal clusters
            
        Returns:
            Statistics dict
        """
        logger.info("Starting Phase 2: Cluster Comparison Analysis...")
        
        if not self.check_llm_available():
            logger.error("LLM service is NOT available - check if LM Studio is running")
            logger.error(f"Trying to connect to: {self.llm_base_url}")
            return {'error': 'LLM not available'}
        
        logger.info("LLM service is available")
        
        # Zorg dat model geladen is met juiste GPU layers
        if not self.ensure_correct_gpu_layers():
            logger.error("Failed to load model with correct GPU layers")
            return {'error': 'Model load failed'}
        
        logger.info("Model loaded successfully")
        
        clusters = self.get_clusters_to_analyze(limit)
        
        if not clusters:
            logger.info("No pending clusters to analyze")
            return {'analyzed': 0, 'errors': 0}
        
        analyzed = 0
        errors = 0
        start_time = datetime.now(timezone.utc)
        
        for i, cluster in enumerate(clusters):
            cluster_id = cluster['cluster_id']
            
            # Progress
            if analyzed > 0:
                elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                rate = analyzed / elapsed
                remaining = len(clusters) - i
                eta = remaining / rate if rate > 0 else 0
                eta_str = f", ETA: {eta/60:.1f}m"
            else:
                eta_str = ""
            
            logger.info(f"[{i+1}/{len(clusters)}] Cluster {cluster_id} "
                       f"({cluster['post_count']} posts, sigma={cluster['trigger_sigma']:.2f}){eta_str}")
            
            # Send ECG heartbeat periodically
            if time.time() - self._last_heartbeat >= self.heartbeat_interval:
                self._heartbeat_stats = {
                    "clusters_analyzed": analyzed,
                    "errors": errors,
                    "progress": f"{i+1}/{len(clusters)}",
                }
                self._send_heartbeat()
            
            # Time individual cluster analysis
            cluster_start = datetime.now(timezone.utc)
            posts = self.get_cluster_posts_with_scores(cluster_id)
            analysis = self.analyze_cluster(cluster)
            cluster_duration = (datetime.now(timezone.utc) - cluster_start).total_seconds()
            
            if analysis:
                self.save_analysis(analysis, posts)
                analyzed += 1
                
                # Performance check
                perf_status = self.check_performance(cluster_duration)
                
                # Format duration string
                if perf_status == 'critical':
                    duration_str = f"🔴 {cluster_duration:.1f}s"
                elif perf_status == 'recover':
                    duration_str = f"📈 {cluster_duration:.1f}s"
                else:
                    duration_str = f"{cluster_duration:.1f}s"
                
                logger.info(f"  -> [{duration_str}] [GPU:{self.current_gpu_layers}] "
                          f"trigger={analysis['selected_post_id'][:15]}..., "
                          f"conf={analysis['confidence']:.2f}, "
                          f"priority={analysis['review_priority']}")
                
                # Handle critical performance degradation
                if perf_status == 'critical':
                    if self.reload_model():
                        logger.info("Model reloaded, continuing analysis...")
                    else:
                        logger.error("Model reload failed! Consider manual LM Studio restart.")
                
                # Handle recovery (verhoog GPU layers na 5 goede analyses)
                elif perf_status == 'recover':
                    if self.increase_gpu_layers():
                        logger.info("GPU layers verhoogd, continuing analysis...")
                    else:
                        logger.warning("GPU layer increase failed, continuing with current settings...")
            else:
                errors += 1
                logger.warning(f"  -> [{cluster_duration:.1f}s] Analysis failed")
        
        total_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        
        stats = {
            'total_clusters': len(clusters),
            'analyzed': analyzed,
            'errors': errors,
            'elapsed_seconds': total_time,
            'rate_per_minute': analyzed / (total_time / 60) if total_time > 0 else 0,
            'model_reloads': self.total_reloads,
            'final_gpu_layers': self.current_gpu_layers
        }
        
        logger.info(f"\n=== Phase 2 Complete ===")
        logger.info(f"Analyzed: {analyzed}")
        logger.info(f"Errors: {errors}")
        logger.info(f"Model reloads: {self.total_reloads}")
        logger.info(f"Final GPU layers: {self.current_gpu_layers}/{self.MODEL_TOTAL_LAYERS}")
        logger.info(f"Time: {total_time/60:.1f} minutes")
        logger.info(f"Max allowed duration: {self.MAX_DURATION_SECONDS}s/cluster")
        
        self._show_remaining_clusters()
        
        return stats
    
    def _show_remaining_clusters(self):
        """Toon aantal clusters nog te analyseren."""
        query = """
            SELECT COUNT(*) FROM truth.post_clusters
            WHERE status = 'pending'
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            remaining = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            if remaining > 0:
                logger.info(f"\n[!] Nog {remaining} pending clusters")
            else:
                logger.info("\n[OK] Alle clusters geanalyseerd!")
                
        except Exception as e:
            logger.error(f"Failed to count remaining: {e}")
    
    def check_llm_available(self) -> bool:
        """Check of LLM beschikbaar is."""
        try:
            with httpx.Client(timeout=5) as client:
                response = client.get(f"{self.llm_base_url}/models")
                if response.status_code == 200:
                    return True
                else:
                    logger.warning(f"LLM check returned status {response.status_code}")
                    return False
        except httpx.ConnectError as e:
            logger.debug(f"LLM connection error: {e}")
            return False
        except Exception as e:
            logger.debug(f"LLM check error: {e}")
            return False
    
    def ensure_correct_gpu_layers(self) -> bool:
        """
        Zorg dat het model geladen is met de juiste GPU layers en context length.

        Check eerst of model al geladen is, skip reload als dat zo is met juiste settings.

        Returns:
            True als model correct geladen is
        """
        # Forceer altijd een reload om juiste context length te garanderen
        # Het model kan al geladen zijn met verkeerde context settings
        logger.info(f"Loading model with {self.current_gpu_layers}/{self.MODEL_TOTAL_LAYERS} GPU layers and context {self.current_context_length}...")
        return self._load_model_with_layers(self.current_gpu_layers)
    
    def _is_model_loaded(self) -> bool:
        """
        Check of het model al geladen is via LM Studio API.
        
        Returns:
            True als model geladen is
        """
        try:
            # Gebruik LM Studio REST API v0 endpoint
            api_base = self.llm_base_url.replace('/v1', '/api/v0')
            with httpx.Client(timeout=5) as client:
                response = client.get(f"{api_base}/models/{self.llm_model}")
                if response.status_code == 200:
                    data = response.json()
                    state = data.get('state', 'not-loaded')
                    if state == 'loaded':
                        logger.debug(f"Model {self.llm_model} state: loaded")
                        return True
                    else:
                        logger.debug(f"Model {self.llm_model} state: {state}")
                        return False
                else:
                    # Fallback: probeer via /v1/models (OpenAI compat)
                    response = client.get(f"{self.llm_base_url}/models")
                    if response.status_code == 200:
                        models = response.json().get('data', [])
                        model_ids = [m.get('id') for m in models]
                        # Als model in lijst staat, is het waarschijnlijk geladen
                        return self.llm_model in model_ids
                    return False
        except Exception as e:
            logger.debug(f"Failed to check model state: {e}")
            return False
    
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
        
        # Timeout verhogen voor grote modellen (32B+ kan lang duren)
        load_timeout = 300 if target_layers >= 40 else 180
        
        try:
            cmd = [
                "lms", "load",
                self.llm_model,
                "--gpu", f"{gpu_ratio:.2f}",
                "--context-length", str(self.current_context_length),
                "--yes"
            ]
            
            logger.info(f"Running: {' '.join(cmd)} (timeout: {load_timeout}s)")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=load_timeout,
                encoding='utf-8',
                errors='replace'
            )
            
            if result.returncode == 0:
                logger.info(f"Model loaded with {target_layers}/{self.MODEL_TOTAL_LAYERS} GPU layers")
                logger.info("Waiting for model to initialize...")
                time.sleep(10)
                verified = self._verify_model_loaded()
                if not verified:
                    logger.warning("Model loaded but verification failed - continuing anyway")
                return verified
            else:
                logger.error(f"lms load failed (returncode {result.returncode})")
                if result.stdout:
                    logger.error(f"stdout: {result.stdout}")
                if result.stderr:
                    logger.error(f"stderr: {result.stderr}")
                return False
                
        except FileNotFoundError:
            logger.warning("lms CLI not found - checking if model is already loaded in LM Studio")
            # Check of model al geladen is via API
            return self._verify_model_loaded()
        except subprocess.TimeoutExpired:
            logger.warning(f"lms load command timed out after {load_timeout} seconds")
            logger.info("Checking if model was loaded despite timeout...")
            # Check of model toch geladen is (soms duurt het langer maar werkt het wel)
            time.sleep(5)
            if self._is_model_loaded() and self._verify_model_loaded():
                logger.info("Model is loaded and verified despite timeout - continuing")
                return True
            logger.error("Model load timeout and verification failed")
            return False
        except Exception as e:
            logger.error(f"Failed to load model: {e}", exc_info=True)
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
                    return True
        except Exception as e:
            logger.error(f"API reload failed: {e}")
        return False
    
    def _verify_model_loaded(self, max_retries: int = 3) -> bool:
        """
        Verifieer dat het model geladen is met een test request.
        Probeert meerdere keren met pauzes.
        """
        logger.debug(f"Verifying model {self.llm_model} is loaded (max {max_retries} attempts)")
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
                        error_detail = ""
                        try:
                            error_detail = response.text[:200]
                        except:
                            pass
                        logger.warning(f"Model verification attempt {attempt+1}/{max_retries}: status {response.status_code}")
                        if error_detail:
                            logger.debug(f"Response: {error_detail}")
            except httpx.ConnectError as e:
                logger.warning(f"Model verification attempt {attempt+1}/{max_retries}: Connection error - {e}")
            except httpx.TimeoutException:
                logger.warning(f"Model verification attempt {attempt+1}/{max_retries}: Timeout")
            except Exception as e:
                logger.warning(f"Model verification attempt {attempt+1}/{max_retries}: {e}")
            
            if attempt < max_retries - 1:
                logger.debug(f"Waiting 5 seconds before retry...")
                time.sleep(5)  # Wacht voor volgende poging
        
        logger.error(f"Model verification failed after {max_retries} attempts")
        return False
    
    def check_performance(self, duration: float) -> str:
        """
        Check performance - simpele logica.
        
        >120s = te traag, reload met -1 layers
        5x <120s = verhoog +1 layer
        
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
        
        # Na 5 snelle analyses, probeer layers te verhogen
        if (self.consecutive_fast_analyses >= self.RECOVERY_THRESHOLD and 
            self.current_gpu_layers < self.MAX_GPU_LAYERS):
            return 'recover'
        
        return 'ok'


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Cluster comparison analysis (Phase 2)')
    parser.add_argument('--limit', type=int, help='Maximum number of clusters')
    parser.add_argument('--check', action='store_true', help='Check LLM availability')
    parser.add_argument('--test', action='store_true', help='Test mode: analyze single cluster')
    args = parser.parse_args()
    
    analyzer = ClusterAnalyzerPhase2()
    
    if args.check:
        if analyzer.check_llm_available():
            logger.info("LLM service is available")
            sys.exit(0)
        else:
            logger.error("LLM service is NOT available")
            sys.exit(1)
    
    if args.test:
        result = analyzer.run_test()
        sys.exit(0 if 'success' in result else 1)
    
    stats = analyzer.run(limit=args.limit)
    
    if 'error' in stats:
        logger.error(f"Script failed: {stats.get('error', 'Unknown error')}")
        sys.exit(1)
    
    logger.info("\nDone! Review queue available at truth.review_queue view")


def test_context_overflow_parsing():
    """Test functie voor context overflow parsing."""
    analyzer = ClusterAnalyzerPhase2()

    # Test cases voor verschillende foutmeldingen
    test_cases = [
        '{"error":"Trying to keep the first 4632 tokens when context the overflows. However, the model is loaded with context length of only 4096 tokens, which is not enough. Try to load the model with a larger context length, or provide a shorter input"}',
        "maximum context length is 4096, you requested 8192",
        "Context length exceeded: requested 12000, limit 8192",
        "maximum context length is 8192 but you requested 16384 tokens",
        "requested 24576 exceeds limit 16384",
        "context length 8192 requested 16384",
        "You requested 32768 tokens but the maximum is 24576"
    ]

    print("Testing context overflow parsing:")
    for test_text in test_cases:
        result = analyzer._parse_token_overflow(test_text)
        print(f"Input: {test_text}")
        print(f"Parsed: {result}")
        print()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test-overflow":
        test_context_overflow_parsing()
    else:
        main()

