================================================================================
  GLOBAL AIR QUALITY DYNAMICS & INDUSTRIAL POLICY
  Module: H9DISS1 - Data Intensive Scalable Systems
  README - Setup & Execution Instructions (Windows)
================================================================================

PREREQUISITES
--------------------------------------------------------------------------------
1. Python 3.10+ installed (https://www.python.org/downloads/)
   - During install, CHECK "Add Python to PATH"

2. Java JDK 11+ installed (https://adoptium.net/)
   - Set JAVA_HOME environment variable after install
   - Add %JAVA_HOME%\bin to your PATH

3. AWS account with S3 access (credentials in .env file)

VERIFY PREREQUISITES (open Command Prompt / PowerShell):
  python --version        (should show Python 3.10+)
  java -version           (should show Java 11+)


================================================================================
STEP 1: INSTALL DEPENDENCIES
================================================================================
Open Command Prompt in the project folder and run:

  pip install -r requirements.txt

This installs: pandas, numpy, pyspark, boto3, matplotlib, seaborn, scipy,
scikit-learn, xgboost, python-dotenv, tqdm, python-docx, and others.

VERIFY:
  python -c "import pandas; import pyspark; import sklearn; print('OK')"
  Expected output: OK


================================================================================
STEP 2: CONFIGURE AWS CREDENTIALS
================================================================================
Edit the .env file in the project root with your AWS credentials:

  AWS_ACCESS_KEY_ID=your_access_key_here
  AWS_SECRET_ACCESS_KEY=your_secret_key_here
  AWS_REGION=eu-north-1
  S3_BUCKET_NAME=global-air-quality-dynamics-dataset

NOTE: If you do not have AWS credentials, the pipeline will skip S3
upload/download stages automatically and continue with local processing.


================================================================================
STEP 3: SEED THE DATASET (generates ~50 MB CSV file)
================================================================================
Run:
  python seed_data.py

VERIFY:
  - File created: data\air_quality_part_001.csv
  - File size should be approximately 50 MB
  - Terminal shows "SEEDING COMPLETE - 50.x MB dataset created"
  - Check: dir data\air_quality_part_001.csv


================================================================================
STEP 4: UPLOAD TO AWS S3 (optional - requires AWS credentials)
================================================================================
Run:
  python s3_manager.py upload

VERIFY:
  - Terminal shows "UPLOAD COMPLETE" with file count
  - Log in to AWS Console > S3 > your bucket to see uploaded files
  - Or run: python s3_manager.py status

SKIP: If no AWS credentials, skip this step. Pipeline handles it gracefully.


================================================================================
STEP 5: RUN SPARK DISTRIBUTED PROCESSING
================================================================================
Run:
  python spark_processor.py

This performs 9 MapReduce stages using Apache Spark (PySpark):
  Stage 1: Load CSV into Spark DataFrame
  Stage 2: Data cleaning & preprocessing (Map)
  Stage 3: Country aggregation (MapReduce)
  Stage 4: City aggregation (MapReduce)
  Stage 5: Yearly trend analysis (MapReduce)
  Stage 6: Sector impact analysis (MapReduce)
  Stage 7: Policy effectiveness (MapReduce)
  Stage 8: Hourly patterns (MapReduce)
  Stage 9: Health correlations (MapReduce)

VERIFY:
  - Terminal shows "SPARK PROCESSING COMPLETE"
  - Database created: air_quality_results.db
  - Check DB: python -c "import sqlite3; c=sqlite3.connect('air_quality_results.db'); print([t[0] for t in c.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall()])"
  - Expected: 8+ tables listed (country_aqi_stats, city_aqi_stats, etc.)


================================================================================
STEP 6: RUN DATA ANALYSIS & VISUALISATIONS
================================================================================
Run:
  python data_analysis.py

This generates 7 publication-ready charts and statistical reports.

VERIFY:
  - Terminal shows "ALL ANALYSES COMPLETE"
  - Charts created in: output\charts\
    * 01_overview.png
    * 02_temporal.png
    * 03_geographic.png
    * 04_industrial.png
    * 05_policy.png
    * 06_health.png
    * 07_clustering.png
  - Reports created in: output\reports\
    * descriptive_statistics.csv
    * city_clusters.csv
    * analysis_summary.txt
  - Check: dir output\charts\*.png


================================================================================
STEP 7: RUN HYPERPARAMETER TUNING (ML Models)
================================================================================
Run:
  python model_tuning.py

This tunes 3 ML models for AQI prediction:
  1. Random Forest Regressor (RandomizedSearchCV, 20 iterations, 3-fold CV)
  2. Gradient Boosting Regressor (RandomizedSearchCV, 15 iterations, 3-fold CV)
  3. XGBoost Regressor (RandomizedSearchCV, 20 iterations, 3-fold CV)

NOTE: This step takes approximately 5-10 minutes depending on CPU speed.

VERIFY:
  - Terminal shows "TUNING COMPLETE" with best model and R² score
  - Chart created: output\charts\08_model_tuning.png
  - Report created: output\reports\model_tuning_report.txt
  - Results saved to: air_quality_results.db (model_tuning_results table)
  - Check: type output\reports\model_tuning_report.txt


================================================================================
STEP 8: LAUNCH WEB DASHBOARD (optional)
================================================================================
Run:
  python app.py

VERIFY:
  - Terminal shows "Running on http://127.0.0.1:5000"
  - Open browser and go to: http://localhost:5000
  - Dashboard displays: overview stats, charts, country/city tables,
    model tuning results, health impact data, and processing logs
  - Press Ctrl+C in terminal to stop the server


================================================================================
ALTERNATIVE: RUN FULL PIPELINE IN ONE COMMAND
================================================================================
To run all stages automatically (Steps 3-7):

  python main.py all

Or run individual stages:
  python main.py seed         (Step 3 only)
  python main.py generate     (generate 1.5GB+ dataset instead of seed)
  python main.py upload       (Step 4 only)
  python main.py spark        (Step 5 only)
  python main.py analyze      (Step 6 only)
  python main.py tune         (Step 7 only)


================================================================================
PROJECT FILE STRUCTURE
================================================================================
  project_root/
  ├── .env                    # AWS credentials (DO NOT share)
  ├── readme.txt              # This file
  ├── requirements.txt        # Python dependencies
  ├── config.py               # Configuration module
  ├── main.py                 # Main pipeline orchestrator
  ├── seed_data.py            # Dataset seeder (~50 MB)
  ├── data_generator.py       # Full dataset generator (1.5 GB+)
  ├── s3_manager.py           # AWS S3 upload/download manager
  ├── spark_processor.py      # PySpark MapReduce processing
  ├── data_analysis.py        # Statistical analysis & visualisation
  ├── model_tuning.py         # ML hyperparameter tuning
  ├── app.py                  # Flask web dashboard
  ├── generate_report.py      # DOCX report generator
  ├── air_quality_results.db  # SQLite database (after Step 5)
  ├── data/                   # Generated CSV datasets
  │   ├── air_quality_part_001.csv
  │   └── dataset_metadata.json
  └── output/
      ├── charts/             # Analysis visualisations (PNG)
      └── reports/            # Text/CSV reports


================================================================================
TROUBLESHOOTING
================================================================================
Problem: "pyspark not found" or Java errors
  Fix: Ensure Java 11+ is installed and JAVA_HOME is set:
    set JAVA_HOME=C:\Program Files\Java\jdk-11
    set PATH=%JAVA_HOME%\bin;%PATH%

Problem: "ModuleNotFoundError"
  Fix: pip install -r requirements.txt

Problem: Spark shows "Unable to load native-hadoop library"
  This is a WARNING only, not an error. Spark still works correctly.

Problem: Port 5000 already in use (for dashboard)
  Fix: Change port in app.py or kill the process using port 5000

Problem: AWS credentials error
  Fix: Update .env file with valid credentials, or skip S3 steps

================================================================================
TECHNOLOGIES USED
================================================================================
  - Python 3.10+          Primary language
  - Apache Spark 4.1      Distributed processing (MapReduce)
  - AWS S3                Cloud blob storage
  - SQLite                Output database
  - scikit-learn          Machine learning & tuning
  - XGBoost               Gradient boosting model
  - Pandas / NumPy        Data manipulation
  - Matplotlib / Seaborn  Visualisation
  - Flask                 Web dashboard
  - python-docx           Report generation

================================================================================
