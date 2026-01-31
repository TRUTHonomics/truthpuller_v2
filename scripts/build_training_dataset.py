"""
# INSTRUCTIONS:
# Dit script genereert de training dataset voor LLM fine-tuning.
# Vereist: eerst analyze_price_impact.py draaien om impact labels te genereren.
#
# Draai vanuit de truthpuller_v2 directory met actieve venv:
#   python scripts/build_training_dataset.py
#
# Output: data/training_dataset.jsonl in Alpaca/Instruction format
"""

import os
import sys
import json
import logging
import warnings
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import yaml
import pandas as pd

# Onderdruk SQLAlchemy warnings van pandas
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')


def setup_logging() -> logging.Logger:
    """Setup logging met file output en archivering."""
    script_dir = Path(__file__).parent.parent
    log_dir = script_dir / '_log'
    archive_dir = log_dir / 'archive'
    
    log_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / 'build_training_dataset.log'
    
    if log_file.exists():
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_path = archive_dir / f'build_training_dataset_{timestamp}.log'
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


class TrainingDatasetBuilder:
    """
    Bouwt training dataset voor LLM fine-tuning op basis van impact-gelabelde posts.
    
    Genereert JSONL in Alpaca/Instruction format voor Unsloth training:
    {
        "instruction": "...",
        "input": "[POST CONTENT]",
        "output": "{\"signal\": \"BUY/SELL/HOLD\", \"confidence\": 0.0-1.0, \"reasoning\": \"...\"}"
    }
    """
    
    # System prompt die het model moet leren - verbeterd JSON schema
    SYSTEM_INSTRUCTION = """Je bent een crypto trading analist gespecialiseerd in het analyseren van Truth Social posts van Donald Trump. 
Analyseer de post en bepaal het handelssignaal voor TRUMP coin.

Geef je antwoord in strict JSON format:
{
  "signal": "BUY | SELL | HOLD",
  "impact_level": "NO_IMPACT | WEAK | MODERATE | STRONG",
  "confidence": 0.0-1.0,
  "horizon": "SHORT | MEDIUM | LONG",
  "expected_move_pct": -10.0 to +10.0,
  "reasoning": "korte uitleg"
}

Horizon uitleg:
- SHORT: impact binnen 1 uur
- MEDIUM: impact binnen 1-4 uur  
- LONG: impact pas na 4+ uur"""
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize dataset builder.
        
        Args:
            config_path: Path naar config YAML
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config' / 'truth_config.yaml'
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.data_dir = Path(__file__).parent.parent / 'data'
        
        # Database configuratie
        self.db_config = {
            'host': self.config['database']['host'],
            'port': self.config['database']['port'],
            'dbname': self.config['database']['dbname'],
            'user': self.config['database']['user'],
            'password': self.config['database']['password']
        }
    
    def get_connection(self):
        """Get database connection."""
        import psycopg2
        return psycopg2.connect(**self.db_config)
    
    def load_impact_data(self, use_cluster_validation: bool = True) -> pd.DataFrame:
        """
        Laad de impact analyse resultaten uit truth.post_impact tabel.
        
        Voert een JOIN uit met truth.posts om de content op te halen.
        
        Cluster Validatie:
        - Als use_cluster_validation=True, worden cluster-gevalideerde posts verwerkt:
          - Posts in een gevalideerd cluster die NIET de validated_post zijn krijgen NO_IMPACT
          - De validated_post behoudt zijn originele impact label
        - Dit zorgt voor schonere training data door alleen de echte trigger posts te labelen
        
        Args:
            use_cluster_validation: Pas cluster validatie toe (default True)
        
        Returns:
            DataFrame met impact-gelabelde posts
        """
        # Basis query zonder cluster correctie
        query = """
            SELECT 
                pi.post_id,
                p.created_at as post_time,
                p.content_plain as content,
                pi.impact_label,
                pi.signal,
                pi.confidence,
                pi.impact_horizon,
                pi.expected_move_pct,
                pi.best_interval,
                pi.best_window,
                pi.sigma_pct,
                pi.baseline_volatility,
                pi.threshold_method,
                pi.impact_15m, pi.signal_15m, pi.confidence_15m,
                pi.impact_60m, pi.signal_60m, pi.confidence_60m,
                pi.impact_240m, pi.signal_240m, pi.confidence_240m,
                pi.impact_720m, pi.signal_720m, pi.confidence_720m,
                pi.reasoning,
                pi.analyzed_at,
                -- Mark data as available (records exist in post_impact)
                TRUE as data_available
            FROM truth.post_impact pi
            JOIN truth.posts p ON pi.post_id = p.post_id
            WHERE pi.impact_label IS NOT NULL
            ORDER BY p.created_at ASC
        """
        
        try:
            conn = self.get_connection()
            df = pd.read_sql(query, conn)
            logger.info(f"Loaded {len(df)} posts from truth.post_impact")
            
            # Pas cluster validatie toe indien gewenst
            if use_cluster_validation:
                df = self._apply_cluster_validation(df, conn)
            
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Failed to load impact data from database: {e}")
            logger.error("Make sure truth.post_impact table exists and analyze_price_impact.py has been run")
            return pd.DataFrame()
    
    def _apply_cluster_validation(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Pas cluster validatie toe op de impact data.
        
        Voor posts in gevalideerde clusters:
        - Als post is_trigger=TRUE heeft: behoud impact label
        - Als post is_trigger=FALSE heeft of niet gemarkeerd: zet naar NO_IMPACT/HOLD
        
        Dit gebruikt de nieuwe truth.cluster_posts.is_trigger kolom die wordt gezet
        door analyze_clusters_phase2.py en/of de review interface.
        
        Args:
            df: DataFrame met impact data
            conn: Database connection
            
        Returns:
            Aangepast DataFrame met cluster-gecorrigeerde labels
        """
        try:
            # Query posts in gevalideerde of LLM-geanalyseerde clusters
            # - is_trigger = TRUE: deze post is de trigger (behoud label)
            # - is_trigger = FALSE of NULL: niet de trigger (downgrade naar NO_IMPACT)
            cluster_query = """
                SELECT 
                    cp.post_id,
                    cp.is_trigger,
                    cp.rank_in_cluster,
                    pc.cluster_id,
                    pc.status,
                    pc.llm_confidence,
                    pc.llm_mechanism
                FROM truth.cluster_posts cp
                JOIN truth.post_clusters pc ON cp.cluster_id = pc.cluster_id
                WHERE pc.status IN ('validated', 'llm_analyzed')
                  AND pc.post_count > 1
            """
            cluster_df = pd.read_sql(cluster_query, conn)
            
            if cluster_df.empty:
                logger.info("No validated/analyzed clusters found, skipping cluster correction")
                return df
            
            # Posts die is_trigger = TRUE zijn
            trigger_posts = set(cluster_df[cluster_df['is_trigger'] == True]['post_id'])
            
            # Alle posts in multi-post clusters
            all_cluster_posts = set(cluster_df['post_id'])
            
            # Posts die NIET de trigger zijn
            non_trigger_posts = all_cluster_posts - trigger_posts
            
            logger.info(f"Cluster correction: {len(trigger_posts)} trigger posts, "
                       f"{len(non_trigger_posts)} non-trigger posts to downgrade")
            
            # Stats voor validated vs LLM-analyzed
            validated_count = len(cluster_df[cluster_df['status'] == 'validated']['cluster_id'].unique())
            llm_count = len(cluster_df[cluster_df['status'] == 'llm_analyzed']['cluster_id'].unique())
            logger.info(f"Source: {validated_count} human-validated clusters, {llm_count} LLM-only clusters")
            
            # Corrigeer non-trigger posts: zet naar NO_IMPACT/HOLD
            before_impact = df[df['post_id'].isin(non_trigger_posts) & (df['impact_label'] != 'NO_IMPACT')].shape[0]
            
            mask = df['post_id'].isin(non_trigger_posts)
            df.loc[mask, 'impact_label'] = 'NO_IMPACT'
            df.loc[mask, 'signal'] = 'HOLD'
            df.loc[mask, 'confidence'] = 0.2
            df.loc[mask, 'reasoning'] = 'Downgraded: not the trigger post in cluster (cluster-validated)'
            
            logger.info(f"Downgraded {before_impact} posts from other impact levels to NO_IMPACT")
            
            # Optioneel: voeg Phase 1 scores toe als extra features
            df = self._add_phase1_features(df, conn)
            
            return df
            
        except Exception as e:
            logger.warning(f"Could not apply cluster validation (tables may not exist yet): {e}")
            return df
    
    def _add_phase1_features(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        """
        Voeg Phase 1 LLM analyse scores toe als extra features.
        
        Deze scores kunnen optioneel worden meegenomen in de training data
        om het model te helpen begrijpen waarom bepaalde posts wel/niet
        impact hebben.
        
        Args:
            df: DataFrame met impact data
            conn: Database connection
            
        Returns:
            DataFrame met toegevoegde Phase 1 scores
        """
        try:
            phase1_query = """
                SELECT 
                    post_id,
                    impact_likelihood,
                    suspicion_score,
                    sentiment_intensity,
                    news_value,
                    mechanism_a_score,
                    mechanism_b_score,
                    mechanism_c_score,
                    likely_mechanisms,
                    suspicious_elements
                FROM truth.post_analysis
            """
            phase1_df = pd.read_sql(phase1_query, conn)
            
            if phase1_df.empty:
                logger.info("No Phase 1 analysis data found")
                return df
            
            # Merge Phase 1 scores met main dataframe
            df = df.merge(phase1_df, on='post_id', how='left')
            
            logger.info(f"Added Phase 1 features for {phase1_df['post_id'].isin(df['post_id']).sum()} posts")
            
            return df
            
        except Exception as e:
            logger.debug(f"Could not add Phase 1 features (table may not exist): {e}")
            return df
    
    def create_training_example(
        self,
        content: str,
        signal: str,
        confidence: float,
        reasoning: str,
        impact_level: str = 'NO_IMPACT',
        horizon: Optional[str] = None,
        expected_move_pct: float = 0.0
    ) -> Dict[str, str]:
        """
        Maak een training voorbeeld in Alpaca format met verbeterd JSON schema.
        
        Args:
            content: Post content (input)
            signal: BUY/SELL/HOLD
            confidence: 0.0-1.0
            reasoning: Uitleg
            impact_level: NO_IMPACT/WEAK/MODERATE/STRONG
            horizon: SHORT/MEDIUM/LONG of None
            expected_move_pct: Werkelijke koersverandering
            
        Returns:
            Dict met instruction, input, output
        """
        # Verbeter reasoning met werkelijke data
        if abs(expected_move_pct) >= 1.0:
            if abs(expected_move_pct) >= 5:
                impact_desc = "sterke"
            elif abs(expected_move_pct) >= 3:
                impact_desc = "significante"
            else:
                impact_desc = "lichte"
            
            direction = "stijging" if expected_move_pct > 0 else "daling"
            horizon_desc = f" binnen {horizon.lower()} termijn" if horizon else ""
            enhanced_reasoning = f"Post veroorzaakte {impact_desc} {direction} ({expected_move_pct:+.1f}%){horizon_desc}. {reasoning}"
        else:
            enhanced_reasoning = reasoning
        
        # Strict JSON output volgens nieuw schema
        output_json = {
            "signal": signal,
            "impact_level": impact_level,
            "confidence": round(confidence, 2),
            "horizon": horizon if horizon else "SHORT",  # Default naar SHORT voor HOLD
            "expected_move_pct": round(expected_move_pct, 2),
            "reasoning": enhanced_reasoning
        }
        
        return {
            "instruction": self.SYSTEM_INSTRUCTION,
            "input": content,
            "output": json.dumps(output_json, ensure_ascii=False)
        }
    
    def filter_quality_examples(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter voor hoogwaardige training voorbeelden.
        
        Args:
            df: Impact analysis DataFrame
            
        Returns:
            Gefilterde DataFrame
        """
        # Alleen posts met beschikbare data
        df_filtered = df[df['data_available'] == True].copy()
        
        # Verwijder posts zonder content
        df_filtered = df_filtered[df_filtered['content'].notna()]
        df_filtered = df_filtered[df_filtered['content'].str.len() > 10]
        
        # Verwijder zeer korte posts (waarschijnlijk niet informatief)
        df_filtered = df_filtered[df_filtered['content'].str.len() >= 20]
        
        logger.info(f"After quality filtering: {len(df_filtered)} posts")
        return df_filtered
    
    def balance_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Balanceer de dataset zodat niet alle voorbeelden HOLD zijn.
        
        Args:
            df: DataFrame met training voorbeelden
            
        Returns:
            Gebalanceerde DataFrame
        """
        # Tel voorbeelden per signal
        signal_counts = df['signal'].value_counts()
        logger.info(f"Original signal distribution:\n{signal_counts.to_string()}")
        
        # Bepaal minimum count (exclusief HOLD als die overheerst)
        buy_count = signal_counts.get('BUY', 0)
        sell_count = signal_counts.get('SELL', 0)
        hold_count = signal_counts.get('HOLD', 0)
        
        # Als HOLD overheerst, beperk het
        if hold_count > (buy_count + sell_count) * 2:
            # Neem max 2x zoveel HOLD als BUY+SELL samen
            max_hold = (buy_count + sell_count) * 2
            
            hold_df = df[df['signal'] == 'HOLD'].sample(n=min(max_hold, hold_count), random_state=42)
            other_df = df[df['signal'] != 'HOLD']
            
            df_balanced = pd.concat([hold_df, other_df])
            logger.info(f"Balanced dataset: {len(df_balanced)} posts")
            logger.info(f"New distribution:\n{df_balanced['signal'].value_counts().to_string()}")
            return df_balanced
        
        return df
    
    def build_dataset(self, output_path: Optional[Path] = None) -> List[Dict[str, str]]:
        """
        Bouw de volledige training dataset.
        
        Args:
            output_path: Output path voor JSONL file
            
        Returns:
            List van training voorbeelden
        """
        if output_path is None:
            output_path = self.data_dir / 'training_dataset.jsonl'
        
        # Laad impact data
        df = self.load_impact_data()
        if df.empty:
            return []
        
        # Filter kwaliteit
        df = self.filter_quality_examples(df)
        if df.empty:
            logger.error("No quality examples found")
            return []
        
        # Balanceer dataset
        df = self.balance_dataset(df)
        
        # Genereer training voorbeelden
        examples = []
        for _, row in df.iterrows():
            # Haal velden op uit impact analysis
            impact_level = row.get('impact_label', 'NO_IMPACT')
            horizon = row.get('impact_horizon')  # SHORT/MEDIUM/LONG of NaN
            expected_move = row.get('expected_move_pct', 0.0)
            
            # Handle NaN values
            if pd.isna(expected_move):
                expected_move = 0.0
            
            # Horizon kan NaN zijn voor NO_IMPACT
            if pd.isna(horizon):
                horizon = None
            
            example = self.create_training_example(
                content=row['content'],
                signal=row['signal'],
                confidence=row['confidence'],
                reasoning=row['reasoning'],
                impact_level=impact_level,
                horizon=horizon,
                expected_move_pct=float(expected_move)
            )
            examples.append(example)
        
        # Sla op als JSONL
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            for example in examples:
                f.write(json.dumps(example, ensure_ascii=False) + '\n')
        
        logger.info(f"Saved {len(examples)} training examples to {output_path}")
        return examples
    
    def create_validation_split(
        self,
        train_path: Optional[Path] = None,
        val_path: Optional[Path] = None,
        val_ratio: float = 0.15
    ):
        """
        Maak TIJDSGEBASEERDE train/validation split van de dataset.
        
        REASON: Random split kan leakage veroorzaken door overlappende marktregimes.
        We splitsen strikt op tijd: eerste 85% = train, laatste 15% = validation.
        Dit voorkomt dat vrijwel identieke posts/marketregimes in zowel train als val zitten.
        
        Args:
            train_path: Output path voor training set
            val_path: Output path voor validation set
            val_ratio: Fractie voor validation (default 15%)
        """
        if train_path is None:
            train_path = self.data_dir / 'train.jsonl'
        if val_path is None:
            val_path = self.data_dir / 'val.jsonl'
        
        # Laad impact data voor tijdssortering
        df = self.load_impact_data()
        if df.empty:
            logger.error("Cannot create split: no impact data")
            return
        
        # Sorteer op post_time voor tijdsgebaseerde split
        if 'post_time' in df.columns:
            df = df.sort_values('post_time', ascending=True)
            logger.info("Using time-based split (sorted by post_time)")
        else:
            logger.warning("post_time not found, falling back to index order")
        
        # Filter en balanceer (zelfde als build_dataset)
        df = self.filter_quality_examples(df)
        df = self.balance_dataset(df)
        
        # Tijdsgebaseerde split: eerste 85% = train, laatste 15% = val
        split_idx = int(len(df) * (1 - val_ratio))
        train_df = df.iloc[:split_idx]
        val_df = df.iloc[split_idx:]
        
        logger.info(f"Time-based split: train posts from earliest to index {split_idx}")
        logger.info(f"Validation posts from index {split_idx} to end (most recent)")
        
        # Genereer voorbeelden voor beide sets
        def generate_examples(subset_df: pd.DataFrame) -> List[Dict[str, str]]:
            examples = []
            for _, row in subset_df.iterrows():
                impact_level = row.get('impact_label', 'NO_IMPACT')
                horizon = row.get('impact_horizon')
                expected_move = row.get('expected_move_pct', 0.0)
                
                # Handle NaN values
                if pd.isna(expected_move):
                    expected_move = 0.0
                if pd.isna(horizon):
                    horizon = None
                
                example = self.create_training_example(
                    content=row['content'],
                    signal=row['signal'],
                    confidence=row['confidence'],
                    reasoning=row['reasoning'],
                    impact_level=impact_level,
                    horizon=horizon,
                    expected_move_pct=float(expected_move)
                )
                examples.append(example)
            return examples
        
        train_examples = generate_examples(train_df)
        val_examples = generate_examples(val_df)
        
        # Sla op
        for path, data in [(train_path, train_examples), (val_path, val_examples)]:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                for example in data:
                    f.write(json.dumps(example, ensure_ascii=False) + '\n')
        
        logger.info(f"Created TIME-BASED train/val split: {len(train_examples)} train, {len(val_examples)} val")
        logger.info("Note: Validation set contains the MOST RECENT posts to simulate real-world deployment")


def main():
    """Main entry point."""
    logger.info("Building Training Dataset...")
    
    builder = TrainingDatasetBuilder()
    
    # Bouw dataset
    examples = builder.build_dataset()
    
    if not examples:
        logger.error("No training examples generated!")
        sys.exit(1)
    
    # Maak train/val split
    builder.create_validation_split()
    
    # Print voorbeelden
    logger.info("\n=== Sample Training Examples ===")
    for i, ex in enumerate(examples[:3]):
        logger.info(f"\n--- Example {i+1} ---")
        logger.info(f"Input: {ex['input'][:100]}...")
        output = json.loads(ex['output'])
        logger.info(f"Signal: {output['signal']}, Confidence: {output['confidence']}")
        logger.info(f"Reasoning: {output['reasoning'][:100]}...")
    
    logger.info(f"\n=== Dataset Statistics ===")
    logger.info(f"Total examples: {len(examples)}")
    
    # Signal distributie
    signals = [json.loads(ex['output'])['signal'] for ex in examples]
    from collections import Counter
    signal_counts = Counter(signals)
    logger.info(f"Signal distribution: {dict(signal_counts)}")


if __name__ == "__main__":
    main()

