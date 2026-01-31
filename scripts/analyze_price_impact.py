"""
# INSTRUCTIONS:
# Dit script analyseert de koersimpact van Truth Social posts.
# Draai vanuit de truthpuller_v2 directory met actieve venv:
#   python scripts/analyze_price_impact.py
#
# Output: truth.post_impact tabel in PostgreSQL database
#
# Vereist: Eerst create_post_impact_table.sql uitvoeren om de tabel aan te maken.
"""

import os
import sys
import json
import logging
import warnings
import shutil
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
import yaml
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor

# Onderdruk SQLAlchemy warnings van pandas
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')


def setup_logging() -> logging.Logger:
    """
    Setup logging met file output en archivering van bestaande logs.
    
    Returns:
        Configured logger
    """
    # Bepaal log directory
    script_dir = Path(__file__).parent.parent
    log_dir = script_dir / '_log'
    archive_dir = log_dir / 'archive'
    
    # Maak directories aan
    log_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    # Log bestandsnaam
    log_file = log_dir / 'analyze_price_impact.log'
    
    # Archiveer bestaand log bestand indien aanwezig
    if log_file.exists():
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_name = f'analyze_price_impact_{timestamp}.log'
        archive_path = archive_dir / archive_name
        shutil.move(str(log_file), str(archive_path))
    
    # Configureer logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Verwijder bestaande handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


# Setup logging
logger = setup_logging()


class PriceImpactAnalyzer:
    """
    Analyseert de koersimpact van Truth Social posts op TRUMP coin.
    
    Bepaalt welke posts daadwerkelijk significante koersbewegingen veroorzaakten
    door te kijken naar:
    - Koersverandering binnen verschillende timeframes en intervallen
    - Vergelijking met baseline volatiliteit
    - Volume spikes
    
    BELANGRIJK: Look-ahead bias wordt voorkomen door alleen klines te gebruiken
    die BESCHIKBAAR waren op het moment van de post. Een kline met time=14:00
    en interval 5m is pas beschikbaar om 14:05 (close time).
    """
    
    # Mapping van interval string naar minuten
    INTERVAL_MINUTES = {
        '1m': 1,
        '5m': 5,
        '15m': 15,
        '1h': 60,
        '4h': 240,
        '12h': 720,
        '1d': 1440,
        '1w': 10080
    }
    
    # Intervallen om te analyseren (van kort naar lang)
    # NOTE: Alleen intervallen gebruiken die in truth.klines_raw beschikbaar zijn
    # Beschikbaar: 1m, 5m, 15m, 1h, 4h, 12h, 1d, 1w (geen 30m!)
    ANALYSIS_INTERVALS = ['1m', '5m', '15m', '1h', '4h', '12h']
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize analyzer with configuration.
        
        Args:
            config_path: Path naar config YAML, default config/truth_config.yaml
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
        
        # Impact thresholds
        impact_cfg = self.config.get('impact_analysis', {})
        # Timeframes zijn nu in minuten NA de post om te meten (niet interval)
        self.measurement_windows = impact_cfg.get('timeframes', [15, 60, 240])
        
        # Threshold methode: "fixed" of "sigma"
        self.threshold_method = impact_cfg.get('threshold_method', 'fixed')
        
        # Vaste drempels
        self.thresholds = impact_cfg.get('thresholds', {})
        self.weak_move_pct = self.thresholds.get('weak_move_pct', 1.0)
        self.significant_move_pct = self.thresholds.get('significant_move_pct', 3.0)
        self.strong_move_pct = self.thresholds.get('strong_move_pct', 5.0)
        
        # Sigma-gebaseerde drempels (stddev van returns)
        sigma_thresholds = impact_cfg.get('sigma_thresholds', {})
        self.weak_sigma = sigma_thresholds.get('weak_sigma', 1.0)
        self.moderate_sigma = sigma_thresholds.get('moderate_sigma', 2.0)
        self.strong_sigma = sigma_thresholds.get('strong_sigma', 3.0)
        self.min_klines_for_sigma = impact_cfg.get('min_klines_for_sigma', 20)
        
        self.volume_spike_multiplier = impact_cfg.get('volume_spike_multiplier', 2.0)
        self.baseline_hours = impact_cfg.get('baseline_hours', 24)
        
        # Minimum post datum - TRUMP coin lanceerde op 2025-01-17
        # Posts van voor deze datum hebben geen kline data
        self.min_post_date = impact_cfg.get('min_post_date', '2025-01-17T17:00:00+00:00')
        
        logger.info(f"Using threshold method: {self.threshold_method}")
        logger.info(f"Filtering posts from: {self.min_post_date}")
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """Get database connection."""
        return psycopg2.connect(**self.db_config)
    
    def get_all_posts(self) -> pd.DataFrame:
        """
        Haal alle posts op uit de database.
        
        Filtert op posts vanaf min_post_date (default: 2025-01-17 17:00 UTC)
        omdat TRUMP coin toen pas lanceerde.
        
        Returns:
            DataFrame met post_id, created_at, content_plain, account_username
        """
        query = f"""
            SELECT 
                post_id,
                created_at,
                content_plain,
                account_username,
                replies_count,
                reblogs_count,
                favourites_count
            FROM {self.schema}.posts
            WHERE content_plain IS NOT NULL
              AND content_plain != ''
              AND created_at >= %s
            ORDER BY created_at ASC
        """
        
        try:
            conn = self.get_connection()
            df = pd.read_sql(query, conn, params=(self.min_post_date,))
            conn.close()
            logger.info(f"Loaded {len(df)} posts from database (since {self.min_post_date})")
            return df
        except Exception as e:
            logger.error(f"Failed to load posts: {e}")
            return pd.DataFrame()
    
    def get_trump_asset_id(self) -> Optional[int]:
        """Get asset_id voor TRUMP coin."""
        trump_symbol = self.config['signal_generator']['trump_asset_symbol']
        
        query = """
            SELECT id FROM symbols.symbols 
            WHERE bybit_symbol = %s OR kraken_symbol ILIKE %s
            LIMIT 1
        """
        
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(query, (trump_symbol, f'%{trump_symbol}%'))
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            if result:
                logger.info(f"Found TRUMP asset_id: {result[0]}")
                return result[0]
            else:
                logger.warning(f"TRUMP asset ({trump_symbol}) not found in symbols table")
                return None
        except Exception as e:
            logger.error(f"Failed to get TRUMP asset_id: {e}")
            return None
    
    def get_kline_data(
        self,
        start_time: datetime,
        end_time: datetime,
        interval_str: str = '1m'
    ) -> pd.DataFrame:
        """
        Haal kline data op voor TRUMP coin binnen een tijdsvenster.
        
        Args:
            start_time: Start timestamp
            end_time: End timestamp
            interval_str: Interval als string ('1m', '5m', '15m', '1h', etc.)
            
        Returns:
            DataFrame met time, open, high, low, close, volume, close_time
        """
        # REASON: TRUMP klines staan in truth.klines_raw (alleen TRUMP, geen asset_id)
        # Kolom heet 'time' (niet timestamp), interval_min is VARCHAR ('1m', niet 1)
        
        # Bereken interval in minuten voor close_time berekening
        interval_minutes = self.INTERVAL_MINUTES.get(interval_str, 1)
        
        query = """
            SELECT 
                time,
                open,
                high,
                low,
                close,
                volume,
                time + INTERVAL '%s minutes' as close_time
            FROM truth.klines_raw
            WHERE interval_min = %s
              AND time >= %s
              AND time <= %s
            ORDER BY time ASC
        """
        
        try:
            conn = self.get_connection()
            # REASON: interval moet als string in query, niet als parameter
            query_formatted = query % (interval_minutes, '%s', '%s', '%s')
            df = pd.read_sql(query_formatted, conn, params=(interval_str, start_time, end_time))
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Failed to load kline data for {interval_str}: {e}")
            return pd.DataFrame()
    
    def get_all_intervals_kline_data(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, pd.DataFrame]:
        """
        Haal kline data op voor alle analyse intervallen.
        
        Args:
            start_time: Start timestamp
            end_time: End timestamp
            
        Returns:
            Dict van interval_str -> DataFrame
        """
        result = {}
        for interval in self.ANALYSIS_INTERVALS:
            df = self.get_kline_data(start_time, end_time, interval)
            if not df.empty:
                result[interval] = df
                logger.debug(f"Loaded {len(df)} {interval} klines")
        return result
    
    def calculate_price_change(
        self,
        klines: pd.DataFrame,
        post_time: datetime,
        minutes_after: int,
        interval_str: str
    ) -> Optional[Dict[str, float]]:
        """
        Bereken koersverandering na een post, ZONDER look-ahead bias.
        
        BELANGRIJK: Look-ahead bias preventie:
        - Kline time = OPEN time
        - Kline is pas BESCHIKBAAR op close_time = time + interval
        - Start prijs = close van laatste BESCHIKBARE kline voor post
        - Eind prijs = close van laatste BESCHIKBARE kline na X minuten
        
        Args:
            klines: DataFrame met kline data (moet 'time' en 'close_time' kolommen hebben)
            post_time: Timestamp van de post
            minutes_after: Aantal minuten na post om te meten
            interval_str: Interval string voor logging
            
        Returns:
            Dict met price_change_pct, start_price, end_price, max_price, min_price
        """
        if klines.empty:
            return None
        
        post_time_utc = post_time.replace(tzinfo=timezone.utc) if post_time.tzinfo is None else post_time
        
        # Converteer timestamps naar UTC als nodig
        klines = klines.copy()
        if klines['time'].dtype == 'datetime64[ns]':
            klines['time'] = pd.to_datetime(klines['time']).dt.tz_localize('UTC')
        if klines['close_time'].dtype == 'datetime64[ns]':
            klines['close_time'] = pd.to_datetime(klines['close_time']).dt.tz_localize('UTC')
        
        # EXPL: Look-ahead bias preventie
        # Start kline = laatste kline die BESCHIKBAAR was op moment van post
        # Beschikbaar betekent: close_time <= post_time
        available_at_post = klines[klines['close_time'] <= post_time_utc]
        
        if available_at_post.empty:
            # Geen data beschikbaar op moment van post
            return None
        
        # Laatste beschikbare kline voor de post
        start_candle = available_at_post.iloc[-1]
        start_price = float(start_candle['close'])
        start_close_time = start_candle['close_time']
        
        if start_price == 0:
            return None
        
        # Eind kline = laatste kline die BESCHIKBAAR was X minuten na post
        end_measurement_time = post_time_utc + timedelta(minutes=minutes_after)
        available_at_end = klines[klines['close_time'] <= end_measurement_time]
        
        if available_at_end.empty or len(available_at_end) <= len(available_at_post):
            # Geen nieuwe data na de post
            return None
        
        end_candle = available_at_end.iloc[-1]
        end_price = float(end_candle['close'])
        
        # Window voor max/min berekening: alle klines die BESCHIKBAAR kwamen
        # NA de post tot aan het meetmoment
        # Dit zijn klines met: close_time > post_time AND close_time <= end_measurement_time
        window_klines = klines[
            (klines['close_time'] > post_time_utc) & 
            (klines['close_time'] <= end_measurement_time)
        ]
        
        if window_klines.empty:
            return None
        
        price_change_pct = ((end_price - start_price) / start_price) * 100
        
        return {
            'price_change_pct': price_change_pct,
            'start_price': start_price,
            'end_price': end_price,
            'max_price': float(window_klines['high'].max()),
            'min_price': float(window_klines['low'].min()),
            'max_drawdown_pct': ((float(window_klines['low'].min()) - start_price) / start_price) * 100,
            'max_runup_pct': ((float(window_klines['high'].max()) - start_price) / start_price) * 100,
            'total_volume': float(window_klines['volume'].sum()),
            'interval': interval_str,
            'start_close_time': start_close_time,
            'num_candles': len(window_klines)
        }
    
    def calculate_baseline_volatility(
        self,
        klines: pd.DataFrame,
        post_time: datetime
    ) -> Optional[float]:
        """
        Bereken baseline volatiliteit in de uren VOOR de post.
        Gebruikt alleen klines die BESCHIKBAAR waren voor de post (geen look-ahead).
        
        Args:
            klines: DataFrame met kline data (moet 'close_time' kolom hebben)
            post_time: Timestamp van de post
            
        Returns:
            Standaarddeviatie van returns als percentage
        """
        if klines.empty:
            return None
        
        post_time_utc = post_time.replace(tzinfo=timezone.utc) if post_time.tzinfo is None else post_time
        baseline_start = post_time_utc - timedelta(hours=self.baseline_hours)
        
        # Converteer timestamps als nodig
        klines = klines.copy()
        if klines['close_time'].dtype == 'datetime64[ns]':
            klines['close_time'] = pd.to_datetime(klines['close_time']).dt.tz_localize('UTC')
        
        # Filter klines voor baseline periode
        # Alleen klines die BESCHIKBAAR waren (close_time) binnen baseline window
        mask = (klines['close_time'] >= baseline_start) & (klines['close_time'] <= post_time_utc)
        baseline_klines = klines[mask]
        
        if len(baseline_klines) < 2:
            return None
        
        # Bereken returns
        returns = baseline_klines['close'].pct_change().dropna() * 100
        
        return float(returns.std())
    
    def calculate_stddev_returns(
        self,
        klines: pd.DataFrame,
        post_time: datetime
    ) -> Optional[float]:
        """
        Bereken standaarddeviatie (σ) van returns over de baseline periode.
        
        Sigma is een maatstaf voor volatiliteit gebaseerd op percentage returns.
        Drempels kunnen dan uitgedrukt worden als "n × sigma move".
        
        Args:
            klines: DataFrame met kline data (close)
            post_time: Timestamp van de post
            
        Returns:
            Standaarddeviatie van returns in %, of None als onvoldoende data
        """
        if klines.empty or len(klines) < self.min_klines_for_sigma:
            return None
        
        post_time_utc = post_time.replace(tzinfo=timezone.utc) if post_time.tzinfo is None else post_time
        
        # Filter klines VOOR de post (beschikbaar = close_time <= post_time)
        klines = klines.copy()
        if 'close_time' in klines.columns:
            mask = klines['close_time'] <= post_time_utc
            klines = klines[mask]
        
        if len(klines) < self.min_klines_for_sigma:
            return None
        
        # Bereken percentage returns: (close[t] - close[t-1]) / close[t-1] * 100
        klines = klines.copy()
        klines['return_pct'] = klines['close'].pct_change() * 100
        
        # Bereken standaarddeviatie van returns (excl eerste rij die NaN heeft)
        returns = klines['return_pct'].dropna()
        
        if len(returns) < 2:
            return None
        
        sigma = returns.std()
        
        logger.debug(f"Sigma (stddev returns): {sigma:.4f}% over {len(returns)} returns")
        return sigma
    
    def get_dynamic_thresholds(self, sigma_pct: Optional[float]) -> Tuple[float, float, float]:
        """
        Bepaal drempels op basis van threshold methode en sigma.
        
        Args:
            sigma_pct: Standaarddeviatie van returns in %, of None
            
        Returns:
            Tuple van (weak_threshold, moderate_threshold, strong_threshold) in %
        """
        if self.threshold_method == 'sigma' and sigma_pct is not None and sigma_pct > 0:
            # Sigma-gebaseerde drempels: n × σ
            weak = sigma_pct * self.weak_sigma
            moderate = sigma_pct * self.moderate_sigma
            strong = sigma_pct * self.strong_sigma
            logger.debug(f"Sigma thresholds: weak={weak:.2f}% (1σ), mod={moderate:.2f}% (2σ), strong={strong:.2f}% (3σ), σ={sigma_pct:.4f}%")
            return (weak, moderate, strong)
        else:
            # Vaste drempels
            return (self.weak_move_pct, self.significant_move_pct, self.strong_move_pct)
    
    def classify_impact(
        self,
        price_changes: Dict[int, Dict[str, float]],
        baseline_volatility: Optional[float]
    ) -> Dict[str, Any]:
        """
        Classificeer de impact van een post op basis van koersveranderingen.
        
        Args:
            price_changes: Dict van timeframe -> price change data
            baseline_volatility: Baseline volatiliteit percentage
            
        Returns:
            Dict met impact_label, signal, confidence, reasoning
        """
        # Default: geen impact
        result = {
            'impact_label': 'NO_IMPACT',
            'signal': 'HOLD',
            'confidence': 0.0,
            'reasoning': ''
        }
        
        if not price_changes:
            result['reasoning'] = 'No price data available'
            return result
        
        # Analyseer alle timeframes
        max_move = 0.0
        max_move_tf = 0
        direction = 0  # 1 = up, -1 = down
        
        for tf, data in price_changes.items():
            if data is None:
                continue
            
            pct_change = data['price_change_pct']
            abs_change = abs(pct_change)
            
            if abs_change > abs(max_move):
                max_move = pct_change
                max_move_tf = tf
                direction = 1 if pct_change > 0 else -1
        
        abs_max_move = abs(max_move)
        
        # Classificeer op basis van beweging
        if abs_max_move >= self.strong_move_pct:
            result['impact_label'] = 'STRONG_IMPACT'
            result['confidence'] = min(0.9, 0.5 + (abs_max_move - self.strong_move_pct) * 0.1)
        elif abs_max_move >= self.significant_move_pct:
            result['impact_label'] = 'MODERATE_IMPACT'
            result['confidence'] = min(0.7, 0.4 + (abs_max_move - self.significant_move_pct) * 0.15)
        elif abs_max_move >= 1.0:
            result['impact_label'] = 'WEAK_IMPACT'
            result['confidence'] = 0.3
        else:
            result['impact_label'] = 'NO_IMPACT'
            result['confidence'] = 0.2
            result['reasoning'] = f'Minimal price movement: {max_move:.2f}%'
            return result
        
        # Bepaal signal richting
        if direction > 0:
            result['signal'] = 'BUY'
        else:
            result['signal'] = 'SELL'
        
        # Verhoog confidence als beweging groter is dan baseline volatiliteit
        if baseline_volatility and baseline_volatility > 0:
            vol_ratio = abs_max_move / baseline_volatility
            if vol_ratio > 2.0:
                result['confidence'] = min(0.95, result['confidence'] + 0.1)
                result['reasoning'] = f'Price moved {max_move:.2f}% in {max_move_tf}min ({vol_ratio:.1f}x baseline volatility)'
            else:
                result['reasoning'] = f'Price moved {max_move:.2f}% in {max_move_tf}min (within normal volatility)'
        else:
            result['reasoning'] = f'Price moved {max_move:.2f}% in {max_move_tf}min'
        
        return result
    
    def classify_impact_multi_interval(
        self,
        price_changes: Dict[str, Optional[Dict[str, float]]],
        baseline_volatility: Optional[float],
        sigma_pct: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Classificeer de impact van een post over meerdere intervallen en windows.
        
        Zoekt de meest significante beweging over alle interval/window combinaties.
        Ondersteunt zowel vaste als sigma-gebaseerde drempels.
        
        Args:
            price_changes: Dict van "interval_window" -> price change data
                           bijv. "1m_15m", "5m_60m", "1h_240m"
            baseline_volatility: Baseline volatiliteit percentage
            sigma_pct: Sigma (stddev returns) in % voor volatiliteits-relatieve drempels
            
        Returns:
            Dict met impact_label, signal, confidence, reasoning, best_interval, best_window, impact_horizon, expected_move_pct
        """
        # Default: geen impact
        result = {
            'impact_label': 'NO_IMPACT',
            'signal': 'HOLD',
            'confidence': 0.0,
            'reasoning': '',
            'best_interval': None,
            'best_window': None,
            'impact_horizon': None,  # SHORT (<60m), MEDIUM (60-240m), LONG (>240m)
            'expected_move_pct': 0.0  # De werkelijke beweging voor training
        }
        
        if not price_changes:
            result['reasoning'] = 'No price data available'
            return result
        
        # Zoek de grootste beweging over alle metingen
        max_move = 0.0
        max_move_key = ''
        direction = 0  # 1 = up, -1 = down
        
        for key, data in price_changes.items():
            if data is None:
                continue
            
            pct_change = data['price_change_pct']
            abs_change = abs(pct_change)
            
            if abs_change > abs(max_move):
                max_move = pct_change
                max_move_key = key
                direction = 1 if pct_change > 0 else -1
        
        abs_max_move = abs(max_move)
        
        # Parse best interval en window uit key
        if max_move_key and '_' in max_move_key:
            parts = max_move_key.rsplit('_', 1)
            result['best_interval'] = parts[0]
            result['best_window'] = parts[1] if len(parts) > 1 else None
        
        # Sla de werkelijke beweging op voor training
        result['expected_move_pct'] = round(max_move, 2)
        
        # Bepaal impact_horizon op basis van best_window
        # SHORT: <60m, MEDIUM: 60-240m (1-4h), LONG: >240m (>4h)
        if result['best_window']:
            window_str = result['best_window']
            # Parse window minuten uit string (bijv. "15m" -> 15, "60m" -> 60)
            try:
                window_min = int(window_str.replace('m', ''))
                if window_min < 60:
                    result['impact_horizon'] = 'SHORT'
                elif window_min <= 240:
                    result['impact_horizon'] = 'MEDIUM'
                else:
                    result['impact_horizon'] = 'LONG'
            except ValueError:
                result['impact_horizon'] = 'UNKNOWN'
        
        # Bepaal signal richting eerst (nodig voor alle impact levels behalve NO_IMPACT)
        if direction > 0:
            signal = 'BUY'
        elif direction < 0:
            signal = 'SELL'
        else:
            signal = 'HOLD'
        
        # Haal dynamische drempels op (vaste of ATR-relatieve)
        weak_thresh, sig_thresh, strong_thresh = self.get_dynamic_thresholds(sigma_pct)
        
        # Classificeer op basis van beweging
        # REASON: WEAK_IMPACT heeft ook een richting, alleen NO_IMPACT is HOLD
        if abs_max_move >= strong_thresh:
            result['impact_label'] = 'STRONG_IMPACT'
            result['confidence'] = min(0.9, 0.5 + (abs_max_move - strong_thresh) * 0.1)
            result['signal'] = signal
        elif abs_max_move >= sig_thresh:
            result['impact_label'] = 'MODERATE_IMPACT'
            result['confidence'] = min(0.7, 0.4 + (abs_max_move - sig_thresh) * 0.15)
            result['signal'] = signal
        elif abs_max_move >= weak_thresh:
            result['impact_label'] = 'WEAK_IMPACT'
            result['confidence'] = 0.3 + (abs_max_move - weak_thresh) * 0.1
            result['signal'] = signal  # BUY of SELL, niet HOLD
        else:
            result['impact_label'] = 'NO_IMPACT'
            result['confidence'] = 0.2
            result['signal'] = 'HOLD'  # Alleen NO_IMPACT is HOLD
            result['impact_horizon'] = None  # Geen horizon bij NO_IMPACT
            result['expected_move_pct'] = round(max_move, 2)
            result['reasoning'] = f'Minimal price movement: {max_move:+.2f}%'
            return result
        
        # Verhoog confidence als beweging groter is dan baseline volatiliteit
        horizon_str = f', horizon={result["impact_horizon"]}' if result['impact_horizon'] else ''
        if baseline_volatility and baseline_volatility > 0:
            vol_ratio = abs_max_move / baseline_volatility
            if vol_ratio > 2.0:
                result['confidence'] = min(0.95, result['confidence'] + 0.1)
                result['reasoning'] = f'Price moved {max_move:+.2f}% ({max_move_key}{horizon_str}, {vol_ratio:.1f}x baseline vol)'
            else:
                result['reasoning'] = f'Price moved {max_move:+.2f}% ({max_move_key}{horizon_str}, within normal vol)'
        else:
            result['reasoning'] = f'Price moved {max_move:+.2f}% ({max_move_key}{horizon_str})'
        
        return result
    
    def analyze_post(
        self,
        post_id: str,
        post_time: datetime
    ) -> Dict[str, Any]:
        """
        Analyseer de koersimpact van een enkele post over meerdere intervallen.
        
        Gebruikt alle beschikbare intervallen (1m, 5m, 15m, 1h, 4h, 12h) en
        meet de koersverandering over verschillende tijdsvensters NA de post.
        
        Look-ahead bias wordt voorkomen door alleen klines te gebruiken die
        BESCHIKBAAR waren op het moment van meting.
        
        Args:
            post_id: Post ID
            post_time: Timestamp van de post
            
        Returns:
            Dict met alle impact metrics en classificatie per interval
        """
        # Haal kline data op voor analyse window
        # We hebben data nodig van baseline_hours VOOR tot max measurement window NA de post
        max_window = max(self.measurement_windows)
        start_time = post_time - timedelta(hours=self.baseline_hours)
        end_time = post_time + timedelta(minutes=max_window + 60)  # Extra marge
        
        # Haal alle intervallen op
        all_klines = self.get_all_intervals_kline_data(start_time, end_time)
        
        if not all_klines:
            logger.warning(f"No kline data for post {post_id} at {post_time}")
            return {
                'post_id': post_id,
                'post_time': post_time,
                'data_available': False,
                'impact_label': 'UNKNOWN',
                'signal': 'HOLD',
                'confidence': 0.0,
                'reasoning': 'No price data available'
            }
        
        # Bereken baseline volatiliteit (gebruik 1m klines als beschikbaar, anders 5m)
        baseline_klines = all_klines.get('1m', all_klines.get('5m', pd.DataFrame()))
        baseline_vol = self.calculate_baseline_volatility(baseline_klines, post_time)
        
        # Bereken sigma (stddev returns) voor volatiliteits-relatieve drempels
        # Gebruik 1h klines voor stabielere sigma berekening
        sigma_klines = all_klines.get('1h', all_klines.get('4h', pd.DataFrame()))
        sigma_pct = self.calculate_stddev_returns(sigma_klines, post_time) if not sigma_klines.empty else None
        
        # Bereken koersverandering voor elk interval EN elk measurement window
        all_price_changes = {}
        
        for interval_str, klines in all_klines.items():
            for window_minutes in self.measurement_windows:
                key = f"{interval_str}_{window_minutes}m"
                change = self.calculate_price_change(klines, post_time, window_minutes, interval_str)
                all_price_changes[key] = change
        
        # Classificeer impact (kijk naar beste resultaat over alle metingen)
        classification = self.classify_impact_multi_interval(all_price_changes, baseline_vol, sigma_pct)
        
        # Bouw resultaat
        result = {
            'post_id': post_id,
            'post_time': post_time,
            'data_available': True,
            'baseline_volatility': baseline_vol,
            'sigma_pct': sigma_pct,  # Sigma (stddev returns) voor debugging/transparantie
            'threshold_method': self.threshold_method,
            **classification
        }
        
        # Voeg per-interval/window metrics toe
        for key, data in all_price_changes.items():
            if data:
                result[f'pct_change_{key}'] = data['price_change_pct']
                result[f'max_runup_{key}'] = data['max_runup_pct']
                result[f'max_drawdown_{key}'] = data['max_drawdown_pct']
                result[f'volume_{key}'] = data['total_volume']
            else:
                result[f'pct_change_{key}'] = None
                result[f'max_runup_{key}'] = None
                result[f'max_drawdown_{key}'] = None
                result[f'volume_{key}'] = None
        
        # Per-horizon labels toevoegen
        # Maakt later multi-task learning mogelijk (apart model per horizon)
        for window_min in self.measurement_windows:
            horizon_changes = {}
            for key, data in all_price_changes.items():
                # Filter alleen de metingen voor dit window
                if data and key.endswith(f'_{window_min}m'):
                    horizon_changes[key] = data
            
            if horizon_changes:
                # Classificeer alleen deze horizon
                horizon_class = self.classify_impact_multi_interval(horizon_changes, baseline_vol, sigma_pct)
                result[f'impact_{window_min}m'] = horizon_class['impact_label']
                result[f'signal_{window_min}m'] = horizon_class['signal']
                result[f'confidence_{window_min}m'] = horizon_class['confidence']
            else:
                result[f'impact_{window_min}m'] = 'UNKNOWN'
                result[f'signal_{window_min}m'] = 'HOLD'
                result[f'confidence_{window_min}m'] = 0.0
        
        return result
    
    def analyze_all_posts(self) -> pd.DataFrame:
        """
        Analyseer alle posts en genereer impact dataset.
        
        Returns:
            DataFrame met alle posts en hun koersimpact classificaties
        """
        # Haal alle posts op
        posts_df = self.get_all_posts()
        if posts_df.empty:
            logger.error("No posts found in database")
            return pd.DataFrame()
        
        # REASON: TRUMP klines staan in truth.klines_raw zonder asset_id
        # We hoeven geen asset_id op te halen, TRUMP is het enige asset in die tabel
        logger.info(f"Analyzing {len(posts_df)} posts for TRUMP price impact...")
        
        results = []
        total_posts = len(posts_df)
        posts_with_data = 0
        posts_without_data = 0
        start_time = datetime.now(timezone.utc)
        
        for idx, row in posts_df.iterrows():
            # Voortgang logging elke 10 posts of bij eerste post
            if idx % 10 == 0:
                elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                rate = (idx + 1) / elapsed if elapsed > 0 else 0
                eta_seconds = (total_posts - idx - 1) / rate if rate > 0 else 0
                eta_min = eta_seconds / 60
                
                logger.info(
                    f"Progress: {idx + 1}/{total_posts} ({100*(idx+1)/total_posts:.1f}%) | "
                    f"Data: {posts_with_data} | No data: {posts_without_data} | "
                    f"Rate: {rate:.1f}/s | ETA: {eta_min:.1f}min"
                )
            
            impact = self.analyze_post(
                post_id=row['post_id'],
                post_time=row['created_at']
            )
            
            # Track data availability
            if impact.get('data_available', False):
                posts_with_data += 1
            else:
                posts_without_data += 1
            
            # Voeg post content toe
            impact['content'] = row['content_plain']
            impact['account_username'] = row['account_username']
            impact['replies_count'] = row['replies_count']
            impact['reblogs_count'] = row['reblogs_count']
            impact['favourites_count'] = row['favourites_count']
            
            results.append(impact)
        
        # Eindsamenvatting
        total_elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(f"Completed {total_posts} posts in {total_elapsed/60:.1f} minutes")
        
        result_df = pd.DataFrame(results)
        
        # Log statistieken
        if not result_df.empty:
            logger.info("\n=== Impact Analysis Summary ===")
            logger.info(f"Total posts analyzed: {len(result_df)}")
            logger.info(f"Posts with data: {result_df['data_available'].sum()}")
            logger.info("\nImpact distribution:")
            logger.info(result_df['impact_label'].value_counts().to_string())
            logger.info("\nSignal distribution:")
            logger.info(result_df['signal'].value_counts().to_string())
        
        return result_df
    
    def save_to_database(self, df: pd.DataFrame, batch_size: int = 100):
        """
        Sla analyse resultaten op naar truth.post_impact tabel.
        
        Gebruikt INSERT ... ON CONFLICT UPDATE voor idempotente reruns.
        
        Args:
            df: DataFrame met analyse resultaten
            batch_size: Aantal records per batch insert
        """
        if df.empty:
            logger.warning("No results to save")
            return
        
        # Converteer price_changes kolommen naar JSONB
        def build_price_changes_json(row: pd.Series) -> dict:
            """Extract alle pct_change_* kolommen naar één JSON object."""
            price_data = {}
            for col in row.index:
                if col.startswith('pct_change_') or col.startswith('max_runup_') or col.startswith('max_drawdown_') or col.startswith('volume_'):
                    val = row[col]
                    if pd.notna(val):
                        price_data[col] = float(val)
            return price_data
        
        insert_query = """
            INSERT INTO truth.post_impact (
                post_id, analyzed_at, impact_label, signal, confidence,
                impact_horizon, expected_move_pct, best_interval, best_window,
                sigma_pct, baseline_volatility, threshold_method,
                impact_15m, signal_15m, confidence_15m,
                impact_60m, signal_60m, confidence_60m,
                impact_240m, signal_240m, confidence_240m,
                impact_720m, signal_720m, confidence_720m,
                price_changes, reasoning
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (post_id) DO UPDATE SET
                analyzed_at = EXCLUDED.analyzed_at,
                impact_label = EXCLUDED.impact_label,
                signal = EXCLUDED.signal,
                confidence = EXCLUDED.confidence,
                impact_horizon = EXCLUDED.impact_horizon,
                expected_move_pct = EXCLUDED.expected_move_pct,
                best_interval = EXCLUDED.best_interval,
                best_window = EXCLUDED.best_window,
                sigma_pct = EXCLUDED.sigma_pct,
                baseline_volatility = EXCLUDED.baseline_volatility,
                threshold_method = EXCLUDED.threshold_method,
                impact_15m = EXCLUDED.impact_15m,
                signal_15m = EXCLUDED.signal_15m,
                confidence_15m = EXCLUDED.confidence_15m,
                impact_60m = EXCLUDED.impact_60m,
                signal_60m = EXCLUDED.signal_60m,
                confidence_60m = EXCLUDED.confidence_60m,
                impact_240m = EXCLUDED.impact_240m,
                signal_240m = EXCLUDED.signal_240m,
                confidence_240m = EXCLUDED.confidence_240m,
                impact_720m = EXCLUDED.impact_720m,
                signal_720m = EXCLUDED.signal_720m,
                confidence_720m = EXCLUDED.confidence_720m,
                price_changes = EXCLUDED.price_changes,
                reasoning = EXCLUDED.reasoning
        """
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        inserted = 0
        updated = 0
        errors = 0
        
        try:
            for idx, row in df.iterrows():
                try:
                    # Build price_changes JSON
                    price_changes_json = json.dumps(build_price_changes_json(row))
                    
                    # Prepare values, converting NaN to None
                    def safe_val(val, default=None):
                        if pd.isna(val):
                            return default
                        return val
                    
                    values = (
                        row['post_id'],
                        datetime.now(timezone.utc),
                        safe_val(row.get('impact_label'), 'UNKNOWN'),
                        safe_val(row.get('signal'), 'HOLD'),
                        safe_val(row.get('confidence'), 0.0),
                        safe_val(row.get('impact_horizon')),
                        safe_val(row.get('expected_move_pct')),
                        safe_val(row.get('best_interval')),
                        safe_val(row.get('best_window')),
                        safe_val(row.get('sigma_pct')),
                        safe_val(row.get('baseline_volatility')),
                        safe_val(row.get('threshold_method'), 'fixed'),
                        safe_val(row.get('impact_15m')),
                        safe_val(row.get('signal_15m')),
                        safe_val(row.get('confidence_15m')),
                        safe_val(row.get('impact_60m')),
                        safe_val(row.get('signal_60m')),
                        safe_val(row.get('confidence_60m')),
                        safe_val(row.get('impact_240m')),
                        safe_val(row.get('signal_240m')),
                        safe_val(row.get('confidence_240m')),
                        safe_val(row.get('impact_720m')),
                        safe_val(row.get('signal_720m')),
                        safe_val(row.get('confidence_720m')),
                        price_changes_json,
                        safe_val(row.get('reasoning'))
                    )
                    
                    cursor.execute(insert_query, values)
                    inserted += 1
                    
                    # Commit in batches
                    if inserted % batch_size == 0:
                        conn.commit()
                        logger.info(f"Committed batch: {inserted} records")
                        
                except Exception as e:
                    errors += 1
                    logger.error(f"Error inserting post {row.get('post_id', 'unknown')}: {e}")
                    conn.rollback()
            
            # Final commit
            conn.commit()
            logger.info(f"Saved {inserted} records to truth.post_impact (errors: {errors})")
            
        finally:
            cursor.close()
            conn.close()
    
    def save_results(self, df: pd.DataFrame, output_path: Optional[Path] = None):
        """
        DEPRECATED: Gebruik save_to_database() in plaats hiervan.
        Sla analyse resultaten op naar CSV (voor backwards compatibility).
        
        Args:
            df: DataFrame met analyse resultaten
            output_path: Output path, default data/price_impact_analysis.csv
        """
        logger.warning("save_results() is deprecated, use save_to_database() instead")
        if output_path is None:
            output_path = Path(__file__).parent.parent / 'data' / 'price_impact_analysis.csv'
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Saved analysis results to {output_path}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze price impact of Truth Social posts')
    parser.add_argument('--csv', action='store_true', help='Also save to CSV (deprecated)')
    args = parser.parse_args()
    
    logger.info("Starting Price Impact Analysis...")
    logger.info("Using multiple intervals: " + ", ".join(PriceImpactAnalyzer.ANALYSIS_INTERVALS))
    logger.info("Look-ahead bias prevention: enabled (using close_time)")
    
    analyzer = PriceImpactAnalyzer()
    logger.info(f"Threshold method: {analyzer.threshold_method}")
    
    # Analyseer alle posts
    results_df = analyzer.analyze_all_posts()
    
    if results_df.empty:
        logger.error("No results generated. Check database connection and data availability.")
        sys.exit(1)
    
    # Sla resultaten op naar database
    logger.info("Saving results to truth.post_impact table...")
    analyzer.save_to_database(results_df)
    
    # Optioneel: ook naar CSV (voor backwards compatibility)
    if args.csv:
        analyzer.save_results(results_df)
    
    # Print samenvatting van impactful posts
    impactful = results_df[results_df['impact_label'].isin(['STRONG_IMPACT', 'MODERATE_IMPACT'])]
    logger.info(f"\n=== Found {len(impactful)} impactful posts ===")
    
    if not impactful.empty:
        logger.info("\nTop 10 highest impact posts:")
        # Zoek de beste metric kolom (1m_60m als default)
        pct_col = None
        for col in ['pct_change_1m_60m', 'pct_change_5m_60m', 'pct_change_1m_15m']:
            if col in impactful.columns:
                pct_col = col
                break
        
        if pct_col:
            impactful_sorted = impactful.copy()
            impactful_sorted['abs_change'] = impactful_sorted[pct_col].abs()
            top_10 = impactful_sorted.nlargest(10, 'abs_change')
            
            for _, row in top_10.iterrows():
                content_preview = row['content'][:70] + '...' if len(str(row['content'])) > 70 else row['content']
                pct_val = row[pct_col] if pd.notna(row[pct_col]) else 0
                logger.info(f"  [{row['signal']}] {pct_val:+.2f}% ({row.get('best_interval', 'N/A')}) - {content_preview}")
        else:
            logger.warning("No price change columns found in results")
    
    logger.info("\nDone! Results saved to truth.post_impact table.")


if __name__ == "__main__":
    main()

