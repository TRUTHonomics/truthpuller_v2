"""
# INSTRUCTIONS:
# Dit script analyseert de kalibratie van LLM confidence scores.
# Vergelijkt LLM confidence met menselijke validatie om te bepalen 
# of het model goed gekalibreerd is.
#
# Draai vanuit de truthpuller_v2 directory met actieve venv:
#   python scripts/calibration_report.py
#
# Vereist: validated clusters in truth.post_clusters
"""

import os
import sys
import logging
import warnings
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import yaml
import pandas as pd
import numpy as np

# Onderdruk SQLAlchemy warnings
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.kfl_logging import setup_kfl_logging

logger = setup_kfl_logging()


class CalibrationAnalyzer:
    """
    Analyseert LLM confidence kalibratie.
    
    Doel: Bepalen of de LLM confidence scores voorspellend zijn voor
    de daadwerkelijke correctheid (geverifieerd door menselijke review).
    
    Goed gekalibreerd model:
    - Bij 70% confidence, zou ~70% van de suggesties correct moeten zijn
    - Bij 90% confidence, zou ~90% correct moeten zijn
    
    Slecht gekalibreerd model:
    - Overconfident: hoge confidence maar lage accuraatheid
    - Underconfident: lage confidence maar hoge accuraatheid
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize analyzer."""
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
    
    def get_connection(self):
        """Get database connection."""
        import psycopg2
        return psycopg2.connect(**self.db_config)
    
    def load_validation_data(self) -> pd.DataFrame:
        """
        Laad alle gevalideerde clusters met LLM suggesties.
        
        Returns:
            DataFrame met clusters waar LLM suggestie en menselijke validatie beide bekend zijn
        """
        query = """
            SELECT 
                pc.cluster_id,
                pc.interval,
                pc.post_count,
                pc.trigger_sigma,
                pc.llm_suggested_post_id,
                pc.llm_confidence,
                pc.llm_mechanism,
                pc.llm_causation_likelihood,
                pc.validated_post_id,
                pc.status,
                pc.validated_at,
                pc.review_priority,
                pc.score_spread,
                CASE 
                    WHEN pc.llm_suggested_post_id = pc.validated_post_id THEN TRUE 
                    ELSE FALSE 
                END as llm_correct
            FROM truth.post_clusters pc
            WHERE pc.status = 'validated'
              AND pc.llm_suggested_post_id IS NOT NULL
              AND pc.validated_post_id IS NOT NULL
            ORDER BY pc.validated_at ASC
        """
        
        try:
            conn = self.get_connection()
            df = pd.read_sql(query, conn)
            conn.close()
            logger.info(f"Loaded {len(df)} validated clusters with LLM suggestions")
            return df
        except Exception as e:
            logger.error(f"Failed to load validation data: {e}")
            return pd.DataFrame()
    
    def calculate_calibration_bins(
        self, 
        df: pd.DataFrame, 
        n_bins: int = 10
    ) -> pd.DataFrame:
        """
        Bereken kalibratie per confidence bin.
        
        Args:
            df: DataFrame met validation data
            n_bins: Aantal bins voor confidence ranges
            
        Returns:
            DataFrame met kalibratie per bin
        """
        if df.empty:
            return pd.DataFrame()
        
        # Maak bins van 0-10%, 10-20%, ..., 90-100%
        bins = np.linspace(0, 1, n_bins + 1)
        bin_labels = [f"{int(bins[i]*100)}-{int(bins[i+1]*100)}%" for i in range(n_bins)]
        
        df = df.copy()
        df['confidence_bin'] = pd.cut(
            df['llm_confidence'], 
            bins=bins, 
            labels=bin_labels,
            include_lowest=True
        )
        
        # Bereken statistieken per bin
        calibration = df.groupby('confidence_bin', observed=True).agg({
            'llm_correct': ['sum', 'count', 'mean'],
            'llm_confidence': 'mean'
        }).reset_index()
        
        # Flatten column names
        calibration.columns = [
            'confidence_bin', 
            'correct_count', 
            'total_count', 
            'actual_accuracy',
            'avg_confidence'
        ]
        
        # Bereken kalibratie error
        calibration['calibration_error'] = abs(
            calibration['avg_confidence'] - calibration['actual_accuracy']
        )
        
        return calibration
    
    def calculate_ece(self, df: pd.DataFrame) -> float:
        """
        Bereken Expected Calibration Error (ECE).
        
        ECE is een gewogen gemiddelde van de kalibratie errors per bin,
        gewogen naar het aantal voorbeelden in elke bin.
        
        Lagere ECE = beter gekalibreerd model.
        
        Args:
            df: DataFrame met validation data
            
        Returns:
            ECE waarde (0.0 = perfect, 1.0 = worst)
        """
        if df.empty:
            return float('nan')
        
        calibration = self.calculate_calibration_bins(df)
        if calibration.empty:
            return float('nan')
        
        total_samples = calibration['total_count'].sum()
        if total_samples == 0:
            return float('nan')
        
        ece = (
            calibration['calibration_error'] * calibration['total_count']
        ).sum() / total_samples
        
        return ece
    
    def analyze_by_mechanism(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyseer kalibratie per gesuggereerde mechanism.
        
        Args:
            df: DataFrame met validation data
            
        Returns:
            DataFrame met statistieken per mechanism
        """
        if df.empty:
            return pd.DataFrame()
        
        mechanism_stats = df.groupby('llm_mechanism').agg({
            'llm_correct': ['sum', 'count', 'mean'],
            'llm_confidence': 'mean',
            'llm_causation_likelihood': 'mean'
        }).reset_index()
        
        mechanism_stats.columns = [
            'mechanism',
            'correct_count',
            'total_count', 
            'accuracy',
            'avg_confidence',
            'avg_causation_likelihood'
        ]
        
        mechanism_stats['calibration_error'] = abs(
            mechanism_stats['avg_confidence'] - mechanism_stats['accuracy']
        )
        
        return mechanism_stats
    
    def analyze_by_cluster_size(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyseer kalibratie per cluster grootte.
        
        Args:
            df: DataFrame met validation data
            
        Returns:
            DataFrame met statistieken per cluster size category
        """
        if df.empty:
            return pd.DataFrame()
        
        df = df.copy()
        
        # Categoriseer cluster grootte
        def size_category(n):
            if n == 1:
                return '1 post'
            elif n <= 3:
                return '2-3 posts'
            elif n <= 5:
                return '4-5 posts'
            elif n <= 10:
                return '6-10 posts'
            else:
                return '10+ posts'
        
        df['size_category'] = df['post_count'].apply(size_category)
        
        size_stats = df.groupby('size_category').agg({
            'llm_correct': ['sum', 'count', 'mean'],
            'llm_confidence': 'mean',
            'score_spread': 'mean'
        }).reset_index()
        
        size_stats.columns = [
            'cluster_size',
            'correct_count',
            'total_count',
            'accuracy',
            'avg_confidence',
            'avg_score_spread'
        ]
        
        return size_stats
    
    def analyze_temporal_stability(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyseer of kalibratie stabiel is over tijd.
        
        Belangrijk voor: detecteren van concept drift of veranderende
        market dynamics die het model kunnen beïnvloeden.
        
        Args:
            df: DataFrame met validation data
            
        Returns:
            DataFrame met statistieken per week
        """
        if df.empty or 'validated_at' not in df.columns:
            return pd.DataFrame()
        
        df = df.copy()
        df['week'] = pd.to_datetime(df['validated_at']).dt.to_period('W')
        
        weekly_stats = df.groupby('week').agg({
            'llm_correct': ['sum', 'count', 'mean'],
            'llm_confidence': 'mean'
        }).reset_index()
        
        weekly_stats.columns = [
            'week',
            'correct_count',
            'total_count',
            'accuracy',
            'avg_confidence'
        ]
        
        weekly_stats['calibration_error'] = abs(
            weekly_stats['avg_confidence'] - weekly_stats['accuracy']
        )
        
        return weekly_stats
    
    def generate_report(self) -> Dict:
        """
        Genereer volledig kalibratie rapport.
        
        Returns:
            Dict met alle kalibratie statistieken
        """
        df = self.load_validation_data()
        
        if df.empty:
            logger.warning("No validation data available for calibration analysis")
            return {'error': 'No validation data available'}
        
        report = {
            'summary': {
                'total_validated': len(df),
                'llm_correct': int(df['llm_correct'].sum()),
                'overall_accuracy': float(df['llm_correct'].mean()),
                'expected_calibration_error': float(self.calculate_ece(df)),
                'avg_confidence': float(df['llm_confidence'].mean()),
            },
            'calibration_bins': self.calculate_calibration_bins(df).to_dict('records'),
            'by_mechanism': self.analyze_by_mechanism(df).to_dict('records'),
            'by_cluster_size': self.analyze_by_cluster_size(df).to_dict('records'),
            'temporal_stability': self.analyze_temporal_stability(df).to_dict('records'),
        }
        
        # Voeg interpretatie toe
        ece = report['summary']['expected_calibration_error']
        if ece < 0.05:
            calibration_quality = "Excellent"
        elif ece < 0.10:
            calibration_quality = "Good"
        elif ece < 0.20:
            calibration_quality = "Fair"
        else:
            calibration_quality = "Poor"
        
        report['summary']['calibration_quality'] = calibration_quality
        
        # Check voor over/under confidence
        avg_conf = report['summary']['avg_confidence']
        accuracy = report['summary']['overall_accuracy']
        if avg_conf > accuracy + 0.1:
            report['summary']['confidence_bias'] = 'Overconfident'
        elif avg_conf < accuracy - 0.1:
            report['summary']['confidence_bias'] = 'Underconfident'
        else:
            report['summary']['confidence_bias'] = 'Well-calibrated'
        
        return report
    
    def print_report(self, report: Dict):
        """
        Print formatted kalibratie rapport.
        
        Args:
            report: Report dict van generate_report()
        """
        if 'error' in report:
            logger.error(report['error'])
            return
        
        summary = report['summary']
        
        print("\n" + "="*60)
        print("LLM CONFIDENCE CALIBRATION REPORT")
        print("="*60)
        
        print(f"\n📊 SUMMARY")
        print(f"   Total validated clusters: {summary['total_validated']}")
        print(f"   LLM correct predictions: {summary['llm_correct']}")
        print(f"   Overall accuracy: {summary['overall_accuracy']:.1%}")
        print(f"   Average confidence: {summary['avg_confidence']:.1%}")
        print(f"   Expected Calibration Error: {summary['expected_calibration_error']:.3f}")
        print(f"   Calibration quality: {summary['calibration_quality']}")
        print(f"   Confidence bias: {summary['confidence_bias']}")
        
        print(f"\n📈 CALIBRATION BY CONFIDENCE BIN")
        print("-"*60)
        print(f"{'Bin':<15} {'Count':<8} {'Accuracy':<12} {'Avg Conf':<12} {'Error':<10}")
        print("-"*60)
        for row in report['calibration_bins']:
            print(f"{row['confidence_bin']:<15} {row['total_count']:<8} "
                  f"{row['actual_accuracy']:.1%}{'':>6} {row['avg_confidence']:.1%}{'':>6} "
                  f"{row['calibration_error']:.3f}")
        
        print(f"\n🔍 CALIBRATION BY MECHANISM")
        print("-"*60)
        for row in report['by_mechanism']:
            print(f"   Mechanism {row['mechanism']}: "
                  f"{row['accuracy']:.1%} accuracy (n={row['total_count']}), "
                  f"avg confidence {row['avg_confidence']:.1%}")
        
        print(f"\n📦 CALIBRATION BY CLUSTER SIZE")
        print("-"*60)
        for row in report['by_cluster_size']:
            print(f"   {row['cluster_size']}: "
                  f"{row['accuracy']:.1%} accuracy (n={row['total_count']}), "
                  f"avg confidence {row['avg_confidence']:.1%}")
        
        if report['temporal_stability']:
            print(f"\n📅 TEMPORAL STABILITY (by week)")
            print("-"*60)
            for row in report['temporal_stability'][-5:]:  # Last 5 weeks
                print(f"   Week {row['week']}: "
                      f"{row['accuracy']:.1%} accuracy (n={row['total_count']})")
        
        print("\n" + "="*60)
        print("END OF REPORT")
        print("="*60 + "\n")
    
    def save_report(self, report: Dict, output_path: Optional[Path] = None):
        """
        Sla rapport op als JSON.
        
        Args:
            report: Report dict
            output_path: Output path (default: data/calibration_report.json)
        """
        import json
        
        if output_path is None:
            output_path = Path(__file__).parent.parent / 'data' / 'calibration_report.json'
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert non-serializable types
        def convert(obj):
            if isinstance(obj, (np.integer, np.floating)):
                return float(obj)
            elif isinstance(obj, pd.Period):
                return str(obj)
            elif pd.isna(obj):
                return None
            return obj
        
        serializable_report = {}
        for key, value in report.items():
            if isinstance(value, dict):
                serializable_report[key] = {k: convert(v) for k, v in value.items()}
            elif isinstance(value, list):
                serializable_report[key] = [
                    {k: convert(v) for k, v in item.items()} 
                    if isinstance(item, dict) else convert(item)
                    for item in value
                ]
            else:
                serializable_report[key] = convert(value)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(serializable_report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved calibration report to {output_path}")


def main():
    """Main entry point."""
    logger.info("Starting LLM Calibration Analysis...")
    
    analyzer = CalibrationAnalyzer()
    report = analyzer.generate_report()
    
    # Print en save rapport
    analyzer.print_report(report)
    analyzer.save_report(report)
    
    logger.info("Calibration analysis complete.")


if __name__ == "__main__":
    main()

