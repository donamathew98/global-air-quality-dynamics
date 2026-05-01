
# ============================================================
# Spark Processor - Global Air Quality Dynamics Project
# ============================================================
# Distributed data processing using Apache Spark (PySpark).
# Performs MapReduce-style transformations and aggregations
# on the air quality dataset.

# Pipeline Steps:
#   1. Load CSV data into Spark DataFrames
#   2. Data cleaning & preprocessing (MapReduce)
#   3. Aggregations by country, city, year, sector
#   4. Correlation analysis (industrial vs AQI)
#   5. Policy impact MapReduce analysis
#   6. Write results to SQLite database

# Usage:
#     python spark_processor.py
# ============================================================

import os
import sys
import time
import json
import sqlite3
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, DoubleType, BooleanType
)
from pyspark.sql.window import Window

from config import DatasetConfig, ProjectInfo


class SparkProcessor:
    """
    Distributed data processing engine using Apache Spark.
    Performs MapReduce-style operations on the air quality dataset
    and stores results in SQLite database.
    """

    def __init__(self):
        DatasetConfig.ensure_directories()
        self.data_dir = DatasetConfig.DATA_DIR
        self.output_dir = DatasetConfig.OUTPUT_DIR
        self.reports_dir = DatasetConfig.REPORTS_DIR
        self.db_path = DatasetConfig.BASE_DIR / 'air_quality_results.db'
        self.spark = None
        self.df = None

    def _init_spark(self):
        """Initialize Spark session in local mode."""
        print("\n Initializing Apache Spark...")
        self.spark = (
            SparkSession.builder
            .appName("GlobalAirQualityDynamics")
            .master("local[*]")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.showConsoleProgress", "true")
            .config("spark.driver.extraJavaOptions", "-Xss4m")
            .config("spark.log.level", "WARN")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"  Spark {self.spark.version} initialized")
        print(f"  Parallelism: {self.spark.sparkContext.defaultParallelism} cores")

    def _init_database(self):
        """Initialize SQLite database with schema."""
        print("\n Initializing SQLite database...")
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # Create tables for storing processed results
        cursor.executescript("""
            -- Country-level aggregated air quality statistics
            CREATE TABLE IF NOT EXISTS country_aqi_stats (
                country TEXT PRIMARY KEY,
                avg_aqi REAL,
                min_aqi REAL,
                max_aqi REAL,
                std_aqi REAL,
                median_pm25 REAL,
                avg_pm10 REAL,
                avg_no2 REAL,
                avg_so2 REAL,
                avg_co REAL,
                avg_o3 REAL,
                record_count INTEGER,
                regulation_score REAL,
                avg_industrial_output REAL,
                avg_green_energy_pct REAL,
                processed_at TEXT
            );

            -- City-level aggregated statistics
            CREATE TABLE IF NOT EXISTS city_aqi_stats (
                city TEXT,
                country TEXT,
                avg_aqi REAL,
                avg_pm25 REAL,
                avg_pm10 REAL,
                avg_no2 REAL,
                station_count INTEGER,
                record_count INTEGER,
                avg_respiratory_cases REAL,
                avg_hospital_admissions REAL,
                processed_at TEXT,
                PRIMARY KEY (city, country)
            );

            -- Yearly trend analysis results
            CREATE TABLE IF NOT EXISTS yearly_trends (
                year INTEGER,
                country TEXT,
                avg_aqi REAL,
                avg_pm25 REAL,
                avg_industrial_output REAL,
                avg_green_energy_pct REAL,
                avg_compliance_rate REAL,
                total_records INTEGER,
                processed_at TEXT,
                PRIMARY KEY (year, country)
            );

            -- Industry sector impact analysis
            CREATE TABLE IF NOT EXISTS sector_impact (
                sector TEXT PRIMARY KEY,
                avg_aqi REAL,
                avg_pm25 REAL,
                avg_no2 REAL,
                avg_so2 REAL,
                avg_industrial_output REAL,
                avg_compliance_rate REAL,
                record_count INTEGER,
                processed_at TEXT
            );

            -- Policy effectiveness metrics
            CREATE TABLE IF NOT EXISTS policy_effectiveness (
                country TEXT,
                has_carbon_tax INTEGER,
                avg_aqi REAL,
                avg_pm25 REAL,
                avg_compliance_rate REAL,
                avg_green_energy_pct REAL,
                record_count INTEGER,
                processed_at TEXT,
                PRIMARY KEY (country, has_carbon_tax)
            );

            -- Hourly pollution patterns (MapReduce result)
            CREATE TABLE IF NOT EXISTS hourly_patterns (
                hour INTEGER,
                avg_aqi REAL,
                avg_pm25 REAL,
                avg_no2 REAL,
                avg_co REAL,
                avg_traffic_factor REAL,
                record_count INTEGER,
                processed_at TEXT,
                PRIMARY KEY (hour)
            );

            -- Health impact correlations
            CREATE TABLE IF NOT EXISTS health_correlations (
                aqi_bracket TEXT,
                avg_respiratory_cases REAL,
                avg_cardiovascular_cases REAL,
                avg_hospital_admissions REAL,
                avg_premature_deaths REAL,
                record_count INTEGER,
                processed_at TEXT,
                PRIMARY KEY (aqi_bracket)
            );

            -- Processing metadata/log
            CREATE TABLE IF NOT EXISTS processing_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stage TEXT,
                records_processed INTEGER,
                duration_seconds REAL,
                output_table TEXT,
                spark_version TEXT,
                processed_at TEXT
            );
        """)
        conn.commit()
        conn.close()
        print(f"    Database initialized at: {self.db_path}")

    def _load_data(self):
        """Load CSV data into Spark DataFrame (Stage 1: Map - Read)."""
        print("\n STAGE 1: Loading data into Spark DataFrame...")
        csv_files = sorted(self.data_dir.glob('air_quality_part_*.csv'))

        if not csv_files:
            print(" No data files found! Run data_generator.py first.")
            sys.exit(1)

        print(f"   Found {len(csv_files)} CSV file(s)")

        # Read all CSV files into a single DataFrame
        csv_paths = [str(f) for f in csv_files]
        self.df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(csv_paths)
        )

        # Cache for performance
        self.df.cache()
        row_count = self.df.count()
        col_count = len(self.df.columns)

        print(f"    Loaded {row_count:,} records with {col_count} columns")
        print(f"    Partitions: {self.df.rdd.getNumPartitions()}")
        return row_count

    def _clean_and_preprocess(self):
        """Stage 2: Data cleaning & preprocessing (Map operation)."""
        print("\n STAGE 2: Data Cleaning & Preprocessing (Map)...")
        start = time.time()

        initial_count = self.df.count()

        # Drop rows where critical pollutant columns are null
        critical_cols = ['PM2_5', 'PM10', 'NO2', 'SO2', 'CO', 'O3', 'AQI',
                         'country', 'city', 'station_id']
        self.df = self.df.dropna(subset=critical_cols)

        # Cast types explicitly
        self.df = (
            self.df
            .withColumn('PM2_5', F.col('PM2_5').cast(DoubleType()))
            .withColumn('PM10', F.col('PM10').cast(DoubleType()))
            .withColumn('NO2', F.col('NO2').cast(DoubleType()))
            .withColumn('SO2', F.col('SO2').cast(DoubleType()))
            .withColumn('CO', F.col('CO').cast(DoubleType()))
            .withColumn('O3', F.col('O3').cast(DoubleType()))
            .withColumn('AQI', F.col('AQI').cast(IntegerType()))
            .withColumn('year', F.col('year').cast(IntegerType()))
            .withColumn('month', F.col('month').cast(IntegerType()))
            .withColumn('hour', F.col('hour').cast(IntegerType()))
            .withColumn('industrial_output_index',
                        F.col('industrial_output_index').cast(DoubleType()))
            .withColumn('regulation_stringency_score',
                        F.col('regulation_stringency_score').cast(IntegerType()))
            .withColumn('green_energy_pct',
                        F.col('green_energy_pct').cast(DoubleType()))
            .withColumn('regulatory_compliance_rate',
                        F.col('regulatory_compliance_rate').cast(DoubleType()))
            .withColumn('has_carbon_tax',
                        F.col('has_carbon_tax').cast(IntegerType()))
        )

        # Filter out invalid AQI values
        self.df = self.df.filter(
            (F.col('AQI') >= 0) & (F.col('AQI') <= 500)
        )

        # Add derived columns (Map transformation)
        self.df = (
            self.df
            .withColumn('pollution_severity',
                        F.when(F.col('AQI') <= 50, 'Low')
                         .when(F.col('AQI') <= 100, 'Moderate')
                         .when(F.col('AQI') <= 200, 'High')
                         .otherwise('Severe'))
            .withColumn('aqi_bracket',
                        F.concat(
                            (F.floor(F.col('AQI') / 50) * 50).cast(StringType()),
                            F.lit('-'),
                            ((F.floor(F.col('AQI') / 50) + 1) * 50).cast(StringType())
                        ))
        )

        self.df.cache()
        final_count = self.df.count()
        elapsed = time.time() - start

        print(f"    Cleaned: {initial_count:,} -> {final_count:,} records "
              f"({initial_count - final_count} removed)")
        print(f"     Duration: {elapsed:.1f}s")

        self._log_processing('data_cleaning', final_count, elapsed, 'N/A')
        return final_count

    def _country_aggregation(self):
        """Stage 3: Country-level aggregation (Reduce operation)."""
        print("\n STAGE 3: Country Aggregation (MapReduce)...")
        start = time.time()

        country_stats = (
            self.df.groupBy('country')
            .agg(
                F.avg('AQI').alias('avg_aqi'),
                F.min('AQI').alias('min_aqi'),
                F.max('AQI').alias('max_aqi'),
                F.stddev('AQI').alias('std_aqi'),
                F.percentile_approx('PM2_5', 0.5).alias('median_pm25'),
                F.avg('PM10').alias('avg_pm10'),
                F.avg('NO2').alias('avg_no2'),
                F.avg('SO2').alias('avg_so2'),
                F.avg('CO').alias('avg_co'),
                F.avg('O3').alias('avg_o3'),
                F.count('*').alias('record_count'),
                F.avg('regulation_stringency_score').alias('regulation_score'),
                F.avg('industrial_output_index').alias('avg_industrial_output'),
                F.avg('green_energy_pct').alias('avg_green_energy_pct'),
            )
            .orderBy('avg_aqi', ascending=False)
        )

        # Collect and write to database
        rows = country_stats.collect()
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM country_aqi_stats")
        now = datetime.now().isoformat()

        for r in rows:
            cursor.execute("""
                INSERT INTO country_aqi_stats VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (r['country'], r['avg_aqi'], r['min_aqi'], r['max_aqi'],
                  r['std_aqi'], r['median_pm25'], r['avg_pm10'], r['avg_no2'],
                  r['avg_so2'], r['avg_co'], r['avg_o3'], r['record_count'],
                  r['regulation_score'], r['avg_industrial_output'],
                  r['avg_green_energy_pct'], now))

        conn.commit()
        conn.close()
        elapsed = time.time() - start

        print(f"    Aggregated {len(rows)} countries → country_aqi_stats table")
        print(f"    Duration: {elapsed:.1f}s")

        self._log_processing('country_aggregation', len(rows), elapsed,
                             'country_aqi_stats')

    def _city_aggregation(self):
        """Stage 4: City-level aggregation (Reduce operation)."""
        print("\n  STAGE 4: City Aggregation (MapReduce)...")
        start = time.time()

        city_stats = (
            self.df.groupBy('city', 'country')
            .agg(
                F.avg('AQI').alias('avg_aqi'),
                F.avg('PM2_5').alias('avg_pm25'),
                F.avg('PM10').alias('avg_pm10'),
                F.avg('NO2').alias('avg_no2'),
                F.countDistinct('station_id').alias('station_count'),
                F.count('*').alias('record_count'),
                F.avg('respiratory_cases_per_100k').alias('avg_respiratory_cases'),
                F.avg('hospital_admissions_per_100k').alias('avg_hospital_admissions'),
            )
            .orderBy('avg_aqi', ascending=False)
        )

        rows = city_stats.collect()
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM city_aqi_stats")
        now = datetime.now().isoformat()

        for r in rows:
            cursor.execute("""
                INSERT INTO city_aqi_stats VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (r['city'], r['country'], r['avg_aqi'], r['avg_pm25'],
                  r['avg_pm10'], r['avg_no2'], r['station_count'],
                  r['record_count'], r['avg_respiratory_cases'],
                  r['avg_hospital_admissions'], now))

        conn.commit()
        conn.close()
        elapsed = time.time() - start

        print(f"    Aggregated {len(rows)} cities → city_aqi_stats table")
        print(f"    Duration: {elapsed:.1f}s")

        self._log_processing('city_aggregation', len(rows), elapsed,
                             'city_aqi_stats')

    def _yearly_trend_analysis(self):
        """Stage 5: Yearly trend analysis (MapReduce)."""
        print("\n STAGE 5: Yearly Trend Analysis (MapReduce)...")
        start = time.time()

        yearly = (
            self.df.groupBy('year', 'country')
            .agg(
                F.avg('AQI').alias('avg_aqi'),
                F.avg('PM2_5').alias('avg_pm25'),
                F.avg('industrial_output_index').alias('avg_industrial_output'),
                F.avg('green_energy_pct').alias('avg_green_energy_pct'),
                F.avg('regulatory_compliance_rate').alias('avg_compliance_rate'),
                F.count('*').alias('total_records'),
            )
            .orderBy('country', 'year')
        )

        rows = yearly.collect()
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM yearly_trends")
        now = datetime.now().isoformat()

        for r in rows:
            cursor.execute("""
                INSERT INTO yearly_trends VALUES (?,?,?,?,?,?,?,?,?)
            """, (r['year'], r['country'], r['avg_aqi'], r['avg_pm25'],
                  r['avg_industrial_output'], r['avg_green_energy_pct'],
                  r['avg_compliance_rate'], r['total_records'], now))

        conn.commit()
        conn.close()
        elapsed = time.time() - start

        print(f"    Analyzed {len(rows)} year-country combinations → yearly_trends table")
        print(f"    Duration: {elapsed:.1f}s")

        self._log_processing('yearly_trends', len(rows), elapsed,
                             'yearly_trends')

    def _sector_impact_analysis(self):
        """Stage 6: Industry sector impact (MapReduce)."""
        print("\n STAGE 6: Sector Impact Analysis (MapReduce)...")
        start = time.time()

        sector_stats = (
            self.df.groupBy('dominant_industry_sector')
            .agg(
                F.avg('AQI').alias('avg_aqi'),
                F.avg('PM2_5').alias('avg_pm25'),
                F.avg('NO2').alias('avg_no2'),
                F.avg('SO2').alias('avg_so2'),
                F.avg('industrial_output_index').alias('avg_industrial_output'),
                F.avg('regulatory_compliance_rate').alias('avg_compliance_rate'),
                F.count('*').alias('record_count'),
            )
            .orderBy('avg_aqi', ascending=False)
        )

        rows = sector_stats.collect()
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM sector_impact")
        now = datetime.now().isoformat()

        for r in rows:
            cursor.execute("""
                INSERT INTO sector_impact VALUES (?,?,?,?,?,?,?,?,?)
            """, (r['dominant_industry_sector'], r['avg_aqi'], r['avg_pm25'],
                  r['avg_no2'], r['avg_so2'], r['avg_industrial_output'],
                  r['avg_compliance_rate'], r['record_count'], now))

        conn.commit()
        conn.close()
        elapsed = time.time() - start

        print(f"    Analyzed {len(rows)} sectors → sector_impact table")
        print(f"    Duration: {elapsed:.1f}s")

        self._log_processing('sector_impact', len(rows), elapsed,
                             'sector_impact')

    def _policy_analysis(self):
        """Stage 7: Policy effectiveness MapReduce."""
        print("\n STAGE 7: Policy Effectiveness (MapReduce)...")
        start = time.time()

        policy_stats = (
            self.df.groupBy('country', 'has_carbon_tax')
            .agg(
                F.avg('AQI').alias('avg_aqi'),
                F.avg('PM2_5').alias('avg_pm25'),
                F.avg('regulatory_compliance_rate').alias('avg_compliance_rate'),
                F.avg('green_energy_pct').alias('avg_green_energy_pct'),
                F.count('*').alias('record_count'),
            )
            .orderBy('country', 'has_carbon_tax')
        )

        rows = policy_stats.collect()
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM policy_effectiveness")
        now = datetime.now().isoformat()

        for r in rows:
            cursor.execute("""
                INSERT INTO policy_effectiveness VALUES (?,?,?,?,?,?,?,?)
            """, (r['country'], r['has_carbon_tax'], r['avg_aqi'],
                  r['avg_pm25'], r['avg_compliance_rate'],
                  r['avg_green_energy_pct'], r['record_count'], now))

        conn.commit()
        conn.close()
        elapsed = time.time() - start

        print(f"    Analyzed {len(rows)} policy groups → policy_effectiveness table")
        print(f"    Duration: {elapsed:.1f}s")

        self._log_processing('policy_analysis', len(rows), elapsed,
                             'policy_effectiveness')

    def _hourly_pattern_analysis(self):
        """Stage 8: Hourly pollution patterns (MapReduce)."""
        print("\n STAGE 8: Hourly Pattern Analysis (MapReduce)...")
        start = time.time()

        hourly = (
            self.df.groupBy('hour')
            .agg(
                F.avg('AQI').alias('avg_aqi'),
                F.avg('PM2_5').alias('avg_pm25'),
                F.avg('NO2').alias('avg_no2'),
                F.avg('CO').alias('avg_co'),
                F.count('*').alias('record_count'),
            )
            .withColumn('avg_traffic_factor',
                        F.when(F.col('hour').between(7, 9), 1.3)
                         .when(F.col('hour').between(17, 19), 1.2)
                         .otherwise(0.8))
            .orderBy('hour')
        )

        rows = hourly.collect()
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM hourly_patterns")
        now = datetime.now().isoformat()

        for r in rows:
            cursor.execute("""
                INSERT INTO hourly_patterns VALUES (?,?,?,?,?,?,?,?)
            """, (r['hour'], r['avg_aqi'], r['avg_pm25'], r['avg_no2'],
                  r['avg_co'], r['avg_traffic_factor'], r['record_count'], now))

        conn.commit()
        conn.close()
        elapsed = time.time() - start

        print(f"    Analyzed {len(rows)} hourly slots → hourly_patterns table")
        print(f"    Duration: {elapsed:.1f}s")

        self._log_processing('hourly_patterns', len(rows), elapsed,
                             'hourly_patterns')

    def _health_correlation_analysis(self):
        """Stage 9: Health impact correlation (MapReduce)."""
        print("\n STAGE 9: Health Correlation Analysis (MapReduce)...")
        start = time.time()

        health = (
            self.df.groupBy('aqi_bracket')
            .agg(
                F.avg('respiratory_cases_per_100k').alias('avg_respiratory_cases'),
                F.avg('cardiovascular_cases_per_100k').alias('avg_cardiovascular_cases'),
                F.avg('hospital_admissions_per_100k').alias('avg_hospital_admissions'),
                F.avg('premature_deaths_per_million').alias('avg_premature_deaths'),
                F.count('*').alias('record_count'),
            )
            .orderBy('aqi_bracket')
        )

        rows = health.collect()
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM health_correlations")
        now = datetime.now().isoformat()

        for r in rows:
            cursor.execute("""
                INSERT INTO health_correlations VALUES (?,?,?,?,?,?,?)
            """, (r['aqi_bracket'], r['avg_respiratory_cases'],
                  r['avg_cardiovascular_cases'], r['avg_hospital_admissions'],
                  r['avg_premature_deaths'], r['record_count'], now))

        conn.commit()
        conn.close()
        elapsed = time.time() - start

        print(f"    Analyzed {len(rows)} AQI brackets → health_correlations table")
        print(f"    Duration: {elapsed:.1f}s")

        self._log_processing('health_correlations', len(rows), elapsed,
                             'health_correlations')

    def _log_processing(self, stage, records, duration, output_table):
        """Log processing step to database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO processing_log (stage, records_processed,
                duration_seconds, output_table, spark_version, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (stage, records, round(duration, 2), output_table,
              self.spark.version if self.spark else 'N/A',
              datetime.now().isoformat()))
        conn.commit()
        conn.close()

    def _print_db_summary(self):
        """Print summary of all database tables."""
        print(f"\n{'='*60}")
        print(f"    DATABASE SUMMARY")
        print(f"{'='*60}")

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        tables = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()

        for (table_name,) in tables:
            count = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"    {table_name:<30} {count:>6} rows")

        conn.close()
        print(f"\n    Database: {self.db_path}")
        db_size = os.path.getsize(self.db_path) / 1024
        print(f"    Size: {db_size:.1f} KB")
        print(f"{'='*60}")

    def run_full_processing(self):
        """Execute the complete Spark processing pipeline."""
        print(f"\n{'='*60}")
        print(f"   SPARK DISTRIBUTED PROCESSING PIPELINE")
        print(f"   {ProjectInfo.TITLE}")
        print(f"{'='*60}")

        pipeline_start = time.time()

        try:
            # Initialize
            self._init_spark()
            self._init_database()

            # Load data (Map phase)
            total_records = self._load_data()

            # Processing stages (MapReduce)
            self._clean_and_preprocess()
            self._country_aggregation()
            self._city_aggregation()
            self._yearly_trend_analysis()
            self._sector_impact_analysis()
            self._policy_analysis()
            self._hourly_pattern_analysis()
            self._health_correlation_analysis()

            # Summary
            self._print_db_summary()

            pipeline_elapsed = time.time() - pipeline_start

            print(f"\n{'='*60}")
            print(f"   SPARK PROCESSING COMPLETE")
            print(f"{'='*60}")
            print(f"   Records processed:  {total_records:,}")
            print(f"   Spark version:      {self.spark.version}")
            print(f"    Total time:         {pipeline_elapsed:.1f}s")
            print(f"    Results DB:         {self.db_path}")
            print(f"{'='*60}\n")

        finally:
            if self.spark:
                self.spark.stop()
                print("    Spark session stopped")


def main():
    """Entry point for Spark processing."""
    processor = SparkProcessor()
    processor.run_full_processing()

new
if __name__ == '__main__':
    main()
