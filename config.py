"""
============================================================
Configuration Module - Global Air Quality Dynamics Project
============================================================
Loads environment variables from .env file and provides
centralized configuration for all project modules.
============================================================
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
ENV_PATH = Path(__file__).parent / '.env'
load_dotenv(ENV_PATH)


class AWSConfig:
    """AWS S3 Configuration"""
    ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', '')
    SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    REGION = os.getenv('AWS_REGION', 'us-east-1')
    BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'global-air-quality-dynamics-dataset')
    DATA_PREFIX = os.getenv('S3_DATA_PREFIX', 'air_quality_data/')
    ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', None)
    UPLOAD_WORKERS = int(os.getenv('UPLOAD_WORKERS', '4'))
    UPLOAD_CHUNK_SIZE_MB = int(os.getenv('UPLOAD_CHUNK_SIZE_MB', '50'))

    @classmethod
    def validate(cls):
        """Validate that required AWS credentials are set."""
        errors = []
        if not cls.ACCESS_KEY_ID or cls.ACCESS_KEY_ID == 'your_aws_access_key_here':
            errors.append("AWS_ACCESS_KEY_ID is not set in .env file")
        if not cls.SECRET_ACCESS_KEY or cls.SECRET_ACCESS_KEY == 'your_aws_secret_key_here':
            errors.append("AWS_SECRET_ACCESS_KEY is not set in .env file")
        if not cls.BUCKET_NAME:
            errors.append("S3_BUCKET_NAME is not set in .env file")
        if errors:
            print("\n[ERROR] AWS Configuration Errors:")
            for err in errors:
                print(f"   * {err}")
            print(f"\n   Please update your .env file at: {ENV_PATH}")
            return False
        return True


class DatasetConfig:
    """Dataset Generation Configuration"""
    TARGET_SIZE_GB = float(os.getenv('TARGET_DATASET_SIZE_GB', '1.5'))
    TARGET_SIZE_BYTES = int(TARGET_SIZE_GB * 1024 * 1024 * 1024)

    # Data directories
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / 'data'
    OUTPUT_DIR = BASE_DIR / 'output'
    CHARTS_DIR = OUTPUT_DIR / 'charts'
    REPORTS_DIR = OUTPUT_DIR / 'reports'

    # Dataset file names
    RAW_DATA_FILE = 'global_air_quality_raw.csv'
    PROCESSED_DATA_FILE = 'global_air_quality_processed.parquet'
    METADATA_FILE = 'dataset_metadata.json'

    # Geographic coverage
    COUNTRIES_CITIES = {
        'United States': {
            'cities': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
                       'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
                       'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'Charlotte'],
            'lat_range': (25.0, 48.0), 'lon_range': (-125.0, -67.0),
            'industrial_index': 85, 'regulation_score': 78
        },
        'China': {
            'cities': ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen', 'Chengdu',
                       'Wuhan', 'Hangzhou', 'Nanjing', 'Tianjin', 'Chongqing',
                       'Shenyang', 'Harbin', 'Dalian', 'Qingdao', 'Zhengzhou'],
            'lat_range': (18.0, 53.0), 'lon_range': (73.0, 135.0),
            'industrial_index': 95, 'regulation_score': 55
        },
        'India': {
            'cities': ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Ahmedabad',
                       'Chennai', 'Kolkata', 'Pune', 'Jaipur', 'Lucknow',
                       'Kanpur', 'Nagpur', 'Patna', 'Indore', 'Bhopal'],
            'lat_range': (8.0, 35.0), 'lon_range': (68.0, 97.0),
            'industrial_index': 78, 'regulation_score': 45
        },
        'Germany': {
            'cities': ['Berlin', 'Hamburg', 'Munich', 'Cologne', 'Frankfurt',
                       'Stuttgart', 'Düsseldorf', 'Leipzig', 'Dortmund', 'Essen'],
            'lat_range': (47.0, 55.0), 'lon_range': (6.0, 15.0),
            'industrial_index': 82, 'regulation_score': 90
        },
        'United Kingdom': {
            'cities': ['London', 'Birmingham', 'Manchester', 'Leeds', 'Glasgow',
                       'Liverpool', 'Bristol', 'Sheffield', 'Edinburgh', 'Cardiff'],
            'lat_range': (50.0, 58.0), 'lon_range': (-8.0, 2.0),
            'industrial_index': 70, 'regulation_score': 88
        },
        'Brazil': {
            'cities': ['São Paulo', 'Rio de Janeiro', 'Brasília', 'Salvador', 'Fortaleza',
                       'Belo Horizonte', 'Manaus', 'Curitiba', 'Recife', 'Porto Alegre'],
            'lat_range': (-33.0, 5.0), 'lon_range': (-73.0, -35.0),
            'industrial_index': 65, 'regulation_score': 50
        },
        'Japan': {
            'cities': ['Tokyo', 'Yokohama', 'Osaka', 'Nagoya', 'Sapporo',
                       'Kobe', 'Kyoto', 'Fukuoka', 'Kawasaki', 'Hiroshima'],
            'lat_range': (24.0, 45.0), 'lon_range': (123.0, 146.0),
            'industrial_index': 88, 'regulation_score': 85
        },
        'Australia': {
            'cities': ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide',
                       'Gold Coast', 'Canberra', 'Newcastle', 'Hobart', 'Darwin'],
            'lat_range': (-44.0, -10.0), 'lon_range': (113.0, 154.0),
            'industrial_index': 55, 'regulation_score': 82
        },
        'Nigeria': {
            'cities': ['Lagos', 'Kano', 'Ibadan', 'Abuja', 'Port Harcourt',
                       'Benin City', 'Kaduna', 'Maiduguri', 'Zaria', 'Aba'],
            'lat_range': (4.0, 14.0), 'lon_range': (3.0, 15.0),
            'industrial_index': 45, 'regulation_score': 30
        },
        'South Africa': {
            'cities': ['Johannesburg', 'Cape Town', 'Durban', 'Pretoria', 'Port Elizabeth',
                       'Bloemfontein', 'East London', 'Polokwane', 'Nelspruit', 'Kimberley'],
            'lat_range': (-35.0, -22.0), 'lon_range': (17.0, 33.0),
            'industrial_index': 60, 'regulation_score': 52
        },
        'Russia': {
            'cities': ['Moscow', 'Saint Petersburg', 'Novosibirsk', 'Yekaterinburg', 'Kazan',
                       'Nizhny Novgorod', 'Chelyabinsk', 'Omsk', 'Samara', 'Rostov-on-Don'],
            'lat_range': (43.0, 70.0), 'lon_range': (28.0, 180.0),
            'industrial_index': 75, 'regulation_score': 40
        },
        'South Korea': {
            'cities': ['Seoul', 'Busan', 'Incheon', 'Daegu', 'Daejeon',
                       'Gwangju', 'Suwon', 'Ulsan', 'Changwon', 'Goyang'],
            'lat_range': (33.0, 38.5), 'lon_range': (126.0, 130.0),
            'industrial_index': 90, 'regulation_score': 75
        },
        'Mexico': {
            'cities': ['Mexico City', 'Guadalajara', 'Monterrey', 'Puebla', 'Tijuana',
                       'León', 'Ciudad Juárez', 'Zapopan', 'Mérida', 'Cancún'],
            'lat_range': (14.0, 33.0), 'lon_range': (-118.0, -87.0),
            'industrial_index': 62, 'regulation_score': 48
        },
        'Indonesia': {
            'cities': ['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Semarang',
                       'Palembang', 'Makassar', 'Tangerang', 'Depok', 'Bekasi'],
            'lat_range': (-8.0, 5.0), 'lon_range': (95.0, 141.0),
            'industrial_index': 58, 'regulation_score': 38
        },
        'France': {
            'cities': ['Paris', 'Marseille', 'Lyon', 'Toulouse', 'Nice',
                       'Nantes', 'Strasbourg', 'Montpellier', 'Bordeaux', 'Lille'],
            'lat_range': (42.0, 51.0), 'lon_range': (-5.0, 8.0),
            'industrial_index': 72, 'regulation_score': 86
        },
    }

    # Industry sectors
    INDUSTRY_SECTORS = [
        'Power Generation', 'Manufacturing', 'Transportation',
        'Mining & Extraction', 'Chemical Processing', 'Steel & Metals',
        'Cement & Construction', 'Oil Refining', 'Textile & Garment',
        'Agriculture & Farming', 'Waste Management', 'Electronics',
        'Pharmaceutical', 'Food Processing', 'Paper & Pulp'
    ]

    # Pollutant ranges (µg/m³ or ppm where applicable)
    POLLUTANT_RANGES = {
        'PM2.5': (2.0, 500.0),
        'PM10': (5.0, 800.0),
        'NO2': (1.0, 200.0),
        'SO2': (0.5, 150.0),
        'CO': (0.1, 50.0),
        'O3': (5.0, 300.0),
        'VOC': (0.01, 5.0),
        'Lead': (0.0, 1.5),
    }

    # AQI breakpoints
    AQI_CATEGORIES = {
        'Good': (0, 50),
        'Moderate': (51, 100),
        'Unhealthy for Sensitive Groups': (101, 150),
        'Unhealthy': (151, 200),
        'Very Unhealthy': (201, 300),
        'Hazardous': (301, 500)
    }

    # Time range for data generation
    START_YEAR = 2015
    END_YEAR = 2025

    @classmethod
    def ensure_directories(cls):
        """Create required directories if they don't exist."""
        for d in [cls.DATA_DIR, cls.OUTPUT_DIR, cls.CHARTS_DIR, cls.REPORTS_DIR]:
            d.mkdir(parents=True, exist_ok=True)


class ProjectInfo:
    """Project metadata"""
    TITLE = "Global Air Quality Dynamics & Industrial Policy"
    MODULE_CODE = "H9DISS1"
    VERSION = "1.0.0"
    DESCRIPTION = (
        "A comprehensive big data analytics project examining the relationship "
        "between industrial activity, environmental policy, and air quality "
        "across 15 countries using 1.5GB+ real-world-modelled datasets "
        "stored and processed via Amazon AWS S3."
    )
