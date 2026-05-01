"""
============================================================
Model Tuning - Global Air Quality Dynamics Project
============================================================
Hyperparameter tuning for AQI prediction models using
GridSearchCV and RandomizedSearchCV on multiple algorithms.

Models tuned:
  1. Random Forest Regressor
  2. Gradient Boosting Regressor
  3. XGBoost Regressor

Usage:
    python model_tuning.py
============================================================
"""

import os
import sys
import time
import json
import sqlite3
import warnings
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import (
    train_test_split, GridSearchCV, RandomizedSearchCV, cross_val_score
)
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import (
    mean_squared_error, mean_absolute_error, r2_score
)
from sklearn.pipeline import Pipeline

try:
    from xgboost import XGBRegressor
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False

from config import DatasetConfig, ProjectInfo

warnings.filterwarnings('ignore')
plt.style.use('seaborn-v0_8-darkgrid')


class AQIModelTuner:
    """Hyperparameter tuning engine for AQI prediction models."""

    def __init__(self):
        DatasetConfig.ensure_directories()
        self.charts_dir = DatasetConfig.CHARTS_DIR
        self.reports_dir = DatasetConfig.REPORTS_DIR
        self.db_path = DatasetConfig.BASE_DIR / 'air_quality_results.db'
        self.results = {}

    def load_and_prepare_data(self):
        """Load data and prepare features for ML."""
        print("\n📂 Loading data for model tuning...")
        data_dir = DatasetConfig.DATA_DIR
        csv_files = sorted(data_dir.glob('air_quality_part_*.csv'))

        if not csv_files:
            print("❌ No data files found!")
            sys.exit(1)

        # Sample data for tuning (use manageable size)
        frames = []
        for f in csv_files:
            chunk = pd.read_csv(f, nrows=50000)
            frames.append(chunk)

        df = pd.concat(frames, ignore_index=True)
        print(f"   ✅ Loaded {len(df):,} records")

        # Feature engineering
        feature_cols = [
            'PM2_5', 'PM10', 'NO2', 'SO2', 'CO', 'O3', 'VOC', 'Lead',
            'temperature_c', 'humidity_pct', 'wind_speed_mps', 'pressure_hpa',
            'industrial_output_index', 'active_factory_count',
            'emission_standard_level', 'has_carbon_tax', 'green_energy_pct',
            'regulatory_compliance_rate', 'regulation_stringency_score',
            'year', 'month', 'hour', 'day_of_week', 'is_weekend',
            'latitude', 'longitude', 'elevation_m'
        ]

        target = 'AQI'

        # Encode categorical columns
        le = LabelEncoder()
        if 'station_type' in df.columns:
            df['station_type_enc'] = le.fit_transform(df['station_type'].astype(str))
            feature_cols.append('station_type_enc')

        # Drop rows with NaN in features or target
        available = [c for c in feature_cols if c in df.columns]
        df_clean = df[available + [target]].dropna()

        self.X = df_clean[available]
        self.y = df_clean[target]

        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            self.X, self.y, test_size=0.2, random_state=42
        )

        print(f"   📊 Features: {len(available)}")
        print(f"   🔀 Train: {len(self.X_train):,} | Test: {len(self.X_test):,}")

    def tune_random_forest(self):
        """Tune Random Forest with GridSearchCV."""
        print("\n🌲 Tuning Random Forest Regressor...")
        start = time.time()

        param_grid = {
            'n_estimators': [50, 100, 200],
            'max_depth': [10, 20, 30, None],
            'min_samples_split': [2, 5, 10],
            'min_samples_leaf': [1, 2, 4],
            'max_features': ['sqrt', 'log2']
        }

        rf = RandomForestRegressor(random_state=42, n_jobs=-1)

        # Use RandomizedSearchCV for speed
        search = RandomizedSearchCV(
            rf, param_grid, n_iter=20, cv=3, scoring='r2',
            random_state=42, n_jobs=-1, verbose=1
        )
        search.fit(self.X_train, self.y_train)

        best = search.best_estimator_
        y_pred = best.predict(self.X_test)

        metrics = {
            'model': 'RandomForest',
            'best_params': search.best_params_,
            'best_cv_score': round(search.best_score_, 4),
            'test_r2': round(r2_score(self.y_test, y_pred), 4),
            'test_rmse': round(np.sqrt(mean_squared_error(self.y_test, y_pred)), 4),
            'test_mae': round(mean_absolute_error(self.y_test, y_pred), 4),
            'duration_s': round(time.time() - start, 2)
        }

        self.results['random_forest'] = metrics
        self.rf_model = best
        self.rf_search = search

        print(f"   ✅ Best R²: {metrics['test_r2']}")
        print(f"   📊 RMSE: {metrics['test_rmse']} | MAE: {metrics['test_mae']}")
        print(f"   ⚙️  Params: {metrics['best_params']}")
        print(f"   ⏱️  Duration: {metrics['duration_s']}s")

    def tune_gradient_boosting(self):
        """Tune Gradient Boosting with GridSearchCV."""
        print("\n🚀 Tuning Gradient Boosting Regressor...")
        start = time.time()

        param_grid = {
            'n_estimators': [100, 200],
            'max_depth': [3, 5, 7],
            'learning_rate': [0.01, 0.05, 0.1, 0.2],
            'subsample': [0.8, 1.0],
            'min_samples_split': [2, 5],
        }

        gb = GradientBoostingRegressor(random_state=42)

        search = RandomizedSearchCV(
            gb, param_grid, n_iter=15, cv=3, scoring='r2',
            random_state=42, n_jobs=-1, verbose=1
        )
        search.fit(self.X_train, self.y_train)

        best = search.best_estimator_
        y_pred = best.predict(self.X_test)

        metrics = {
            'model': 'GradientBoosting',
            'best_params': search.best_params_,
            'best_cv_score': round(search.best_score_, 4),
            'test_r2': round(r2_score(self.y_test, y_pred), 4),
            'test_rmse': round(np.sqrt(mean_squared_error(self.y_test, y_pred)), 4),
            'test_mae': round(mean_absolute_error(self.y_test, y_pred), 4),
            'duration_s': round(time.time() - start, 2)
        }

        self.results['gradient_boosting'] = metrics
        self.gb_model = best

        print(f"   ✅ Best R²: {metrics['test_r2']}")
        print(f"   📊 RMSE: {metrics['test_rmse']} | MAE: {metrics['test_mae']}")
        print(f"   ⚙️  Params: {metrics['best_params']}")

    def tune_xgboost(self):
        """Tune XGBoost with RandomizedSearchCV."""
        if not HAS_XGBOOST:
            print("\n⚠️  XGBoost not installed, skipping...")
            return

        print("\n⚡ Tuning XGBoost Regressor...")
        start = time.time()

        param_grid = {
            'n_estimators': [100, 200, 300],
            'max_depth': [3, 5, 7, 9],
            'learning_rate': [0.01, 0.05, 0.1, 0.2],
            'subsample': [0.7, 0.8, 1.0],
            'colsample_bytree': [0.7, 0.8, 1.0],
            'reg_alpha': [0, 0.1, 1.0],
            'reg_lambda': [1.0, 2.0, 5.0],
        }

        xgb = XGBRegressor(random_state=42, n_jobs=-1, verbosity=0)

        search = RandomizedSearchCV(
            xgb, param_grid, n_iter=20, cv=3, scoring='r2',
            random_state=42, n_jobs=-1, verbose=1
        )
        search.fit(self.X_train, self.y_train)

        best = search.best_estimator_
        y_pred = best.predict(self.X_test)

        metrics = {
            'model': 'XGBoost',
            'best_params': {k: (str(v) if isinstance(v, (np.integer, np.floating)) else v)
                            for k, v in search.best_params_.items()},
            'best_cv_score': round(search.best_score_, 4),
            'test_r2': round(r2_score(self.y_test, y_pred), 4),
            'test_rmse': round(np.sqrt(mean_squared_error(self.y_test, y_pred)), 4),
            'test_mae': round(mean_absolute_error(self.y_test, y_pred), 4),
            'duration_s': round(time.time() - start, 2)
        }

        self.results['xgboost'] = metrics
        self.xgb_model = best

        print(f"   ✅ Best R²: {metrics['test_r2']}")
        print(f"   📊 RMSE: {metrics['test_rmse']} | MAE: {metrics['test_mae']}")
        print(f"   ⚙️  Params: {metrics['best_params']}")

    def generate_comparison_charts(self):
        """Generate model comparison visualizations."""
        print("\n📊 Generating comparison charts...")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Model Tuning Results - AQI Prediction',
                     fontsize=18, fontweight='bold')

        models = list(self.results.keys())
        colors = ['#2ecc71', '#3498db', '#e74c3c', '#f39c12']

        # 1. R² Comparison
        ax = axes[0, 0]
        r2_scores = [self.results[m]['test_r2'] for m in models]
        bars = ax.bar(models, r2_scores, color=colors[:len(models)], edgecolor='white')
        ax.set_title('R² Score Comparison', fontsize=14)
        ax.set_ylabel('R² Score')
        ax.set_ylim(0, 1.1)
        for bar, val in zip(bars, r2_scores):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                    f'{val:.4f}', ha='center', fontweight='bold')

        # 2. RMSE Comparison
        ax = axes[0, 1]
        rmse_vals = [self.results[m]['test_rmse'] for m in models]
        bars = ax.bar(models, rmse_vals, color=colors[:len(models)], edgecolor='white')
        ax.set_title('RMSE Comparison (Lower is Better)', fontsize=14)
        ax.set_ylabel('RMSE')
        for bar, val in zip(bars, rmse_vals):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{val:.2f}', ha='center', fontweight='bold')

        # 3. Feature importance (from best model)
        ax = axes[1, 0]
        if hasattr(self, 'rf_model'):
            importances = self.rf_model.feature_importances_
            feat_imp = pd.Series(importances, index=self.X.columns)
            feat_imp.nlargest(15).sort_values().plot(kind='barh', ax=ax,
                                                      color='#2ecc71')
            ax.set_title('Top 15 Feature Importances (RF)', fontsize=14)
            ax.set_xlabel('Importance')

        # 4. Training time comparison
        ax = axes[1, 1]
        times = [self.results[m]['duration_s'] for m in models]
        bars = ax.bar(models, times, color=colors[:len(models)], edgecolor='white')
        ax.set_title('Training Duration', fontsize=14)
        ax.set_ylabel('Seconds')
        for bar, val in zip(bars, times):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{val:.1f}s', ha='center', fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.charts_dir / '08_model_tuning.png', dpi=150,
                    bbox_inches='tight')
        plt.close()
        print("   ✅ Saved: 08_model_tuning.png")

    def save_results_to_db(self):
        """Save tuning results to SQLite database."""
        print("\n🗄️  Saving tuning results to database...")

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS model_tuning_results (
                model_name TEXT PRIMARY KEY,
                best_params TEXT,
                cv_score REAL,
                test_r2 REAL,
                test_rmse REAL,
                test_mae REAL,
                duration_seconds REAL,
                feature_count INTEGER,
                train_size INTEGER,
                test_size INTEGER,
                tuned_at TEXT
            )
        """)
        cursor.execute("DELETE FROM model_tuning_results")

        now = datetime.now().isoformat()
        for name, r in self.results.items():
            cursor.execute("""
                INSERT INTO model_tuning_results VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (r['model'], json.dumps(r['best_params'], default=str),
                  r['best_cv_score'], r['test_r2'], r['test_rmse'],
                  r['test_mae'], r['duration_s'], len(self.X.columns),
                  len(self.X_train), len(self.X_test), now))

        conn.commit()
        conn.close()
        print("   ✅ Results saved to model_tuning_results table")

    def save_report(self):
        """Save text report of tuning results."""
        report = []
        report.append("=" * 60)
        report.append("  MODEL TUNING REPORT - AQI PREDICTION")
        report.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 60)
        report.append(f"\nFeatures used: {len(self.X.columns)}")
        report.append(f"Training set: {len(self.X_train):,}")
        report.append(f"Test set: {len(self.X_test):,}\n")

        best_model = None
        best_r2 = -1

        for name, r in self.results.items():
            report.append(f"\n--- {r['model']} ---")
            report.append(f"  R² Score:    {r['test_r2']}")
            report.append(f"  RMSE:        {r['test_rmse']}")
            report.append(f"  MAE:         {r['test_mae']}")
            report.append(f"  CV Score:    {r['best_cv_score']}")
            report.append(f"  Duration:    {r['duration_s']}s")
            report.append(f"  Best Params: {json.dumps(r['best_params'], default=str)}")

            if r['test_r2'] > best_r2:
                best_r2 = r['test_r2']
                best_model = r['model']

        report.append(f"\n{'='*60}")
        report.append(f"  🏆 BEST MODEL: {best_model} (R² = {best_r2})")
        report.append(f"{'='*60}")

        report_text = '\n'.join(report)
        path = self.reports_dir / 'model_tuning_report.txt'
        with open(path, 'w') as f:
            f.write(report_text)

        print(report_text)
        print(f"\n   ✅ Saved: {path}")

    def run_full_tuning(self):
        """Execute complete model tuning pipeline."""
        print(f"\n{'='*60}")
        print(f"  ⚙️  HYPERPARAMETER TUNING PIPELINE")
        print(f"  🌍 {ProjectInfo.TITLE}")
        print(f"{'='*60}")

        start = time.time()

        self.load_and_prepare_data()
        self.tune_random_forest()
        self.tune_gradient_boosting()
        self.tune_xgboost()
        self.generate_comparison_charts()
        self.save_results_to_db()
        self.save_report()

        elapsed = time.time() - start
        print(f"\n  ✅ TUNING COMPLETE - Total: {elapsed:.1f}s")


def main():
    tuner = AQIModelTuner()
    tuner.run_full_tuning()


if __name__ == '__main__':
    main()
