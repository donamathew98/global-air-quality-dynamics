"""
============================================================
Main Orchestrator - Global Air Quality Dynamics Project
============================================================
Runs the complete automated pipeline:
  1. Generate 1.5GB+ synthetic dataset (or seed 50 records)
  2. Upload to AWS S3
  3. Download from S3 (demonstrates retrieval)
  4. Spark distributed processing (MapReduce)
  5. Store results in SQLite database
  6. Run comprehensive analysis & visualizations
  7. Hyperparameter tuning for AQI prediction models

Usage:
    python main.py              # Run full pipeline
    python main.py generate     # Only generate data
    python main.py seed         # Seed 50 sample records
    python main.py upload       # Only upload to S3
    python main.py spark        # Only run Spark processing
    python main.py analyze      # Only run analysis
    python main.py tune         # Only run model tuning
    python main.py all          # Full pipeline
============================================================
"""

import sys
import time
from datetime import datetime

from config import AWSConfig, DatasetConfig, ProjectInfo


def print_banner():
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   🌍  GLOBAL AIR QUALITY DYNAMICS & INDUSTRIAL POLICY  🌍   ║
║                                                              ║
║   Module: {ProjectInfo.MODULE_CODE:<20}                            ║
║   Version: {ProjectInfo.VERSION:<19}                            ║
║   Date: {datetime.now().strftime('%Y-%m-%d %H:%M'):<22}                            ║
║                                                              ║
║   Pipeline Stages:                                           ║
║     1. 📊 Generate / Seed Air Quality Dataset               ║
║     2. ☁️  Upload to Amazon AWS S3                            ║
║     3. ⬇️  Download from S3 (verification)                    ║
║     4. 🔥 Spark Distributed Processing (MapReduce)           ║
║     5. 🗄️  Store Results in SQLite Database                   ║
║     6. 🔬 Run Comprehensive Analysis                         ║
║     7. 📈 Generate Visualizations & Reports                  ║
║     8. ⚙️  Hyperparameter Tuning (ML Models)                  ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
    """)


def run_seed():
    """Seed 50 sample records."""
    print("\n" + "="*60)
    print("  STAGE: SEED 50 SAMPLE RECORDS")
    print("="*60)
    from seed_data import seed_50_records
    seed_50_records()


def run_generate():
    """Stage 1: Generate dataset."""
    print("\n" + "="*60)
    print("  STAGE 1: DATA GENERATION")
    print("="*60)
    from data_generator import AirQualityDataGenerator
    generator = AirQualityDataGenerator()
    generator.generate()


def run_upload():
    """Stage 2: Upload to S3."""
    print("\n" + "="*60)
    print("  STAGE 2: AWS S3 UPLOAD")
    print("="*60)
    if not AWSConfig.validate():
        print("⚠️  Skipping S3 upload - credentials not configured")
        print("   Fill in your .env file and run: python s3_manager.py upload")
        return
    from s3_manager import S3Manager
    manager = S3Manager()
    manager.upload_all_data()


def run_download():
    """Stage 3: Download from S3."""
    print("\n" + "="*60)
    print("  STAGE 3: AWS S3 DOWNLOAD (Verification)")
    print("="*60)
    if not AWSConfig.validate():
        print("⚠️  Skipping S3 download - credentials not configured")
        return
    from s3_manager import S3Manager
    manager = S3Manager()
    manager.show_bucket_status()


def run_spark():
    """Stage 4-5: Spark processing + DB storage."""
    print("\n" + "="*60)
    print("  STAGE 4-5: SPARK DISTRIBUTED PROCESSING + DB STORAGE")
    print("="*60)
    from spark_processor import SparkProcessor
    processor = SparkProcessor()
    processor.run_full_processing()


def run_analyze():
    """Stage 6-7: Analysis and visualization."""
    print("\n" + "="*60)
    print("  STAGE 6-7: ANALYSIS & VISUALIZATION")
    print("="*60)
    from data_analysis import AirQualityAnalyzer
    analyzer = AirQualityAnalyzer()
    analyzer.run_full_analysis()


def run_tune():
    """Stage 8: Model tuning."""
    print("\n" + "="*60)
    print("  STAGE 8: HYPERPARAMETER TUNING")
    print("="*60)
    from model_tuning import AQIModelTuner
    tuner = AQIModelTuner()
    tuner.run_full_tuning()


def main():
    print_banner()
    DatasetConfig.ensure_directories()

    command = sys.argv[1].lower() if len(sys.argv) > 1 else 'all'
    start = time.time()

    if command in ('all', 'full'):
        run_generate()
        run_upload()
        run_download()
        run_spark()
        run_analyze()
        run_tune()
    elif command == 'seed':
        run_seed()
    elif command == 'generate':
        run_generate()
    elif command == 'upload':
        run_upload()
    elif command == 'download':
        run_download()
    elif command == 'spark':
        run_spark()
    elif command == 'analyze':
        run_analyze()
    elif command == 'tune':
        run_tune()
    else:
        print(f"Unknown command: {command}")
        print("Usage: python main.py [all|seed|generate|upload|download|spark|analyze|tune]")
        return

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"  ✅ PIPELINE COMPLETE - Total time: {elapsed/60:.1f} minutes")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
