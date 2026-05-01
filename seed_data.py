"""
============================================================
Data Seeder - Global Air Quality Dynamics Project
============================================================
Seeds a 50MB dataset for testing and demonstration.

Usage:
    python seed_data.py
============================================================
"""

import os
import sys
import csv
import json
import random
import time
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

from config import DatasetConfig, ProjectInfo


def seed_dataset(target_mb=50):
    """Generate seed dataset of approximately target_mb size."""
    print(f"\n{'='*60}")
    print(f"  🌱 SEEDING ~{target_mb}MB DATASET")
    print(f"{'='*60}\n")

    DatasetConfig.ensure_directories()
    np.random.seed(42)
    random.seed(42)

    target_bytes = target_mb * 1024 * 1024

    headers = [
        'record_id', 'timestamp', 'date', 'year', 'month', 'day',
        'hour', 'day_of_week', 'is_weekend',
        'station_id', 'station_name', 'country', 'city',
        'latitude', 'longitude', 'elevation_m', 'station_type',
        'PM2_5', 'PM10', 'NO2', 'SO2', 'CO', 'O3', 'VOC', 'Lead',
        'AQI', 'AQI_category',
        'temperature_c', 'humidity_pct', 'wind_speed_mps',
        'wind_direction_deg', 'pressure_hpa', 'precipitation_mm',
        'industrial_output_index', 'active_factory_count',
        'emission_standard_level', 'has_carbon_tax',
        'green_energy_pct', 'dominant_industry_sector',
        'regulatory_compliance_rate',
        'respiratory_cases_per_100k', 'cardiovascular_cases_per_100k',
        'hospital_admissions_per_100k', 'premature_deaths_per_million',
        'regulation_stringency_score', 'policy_change_flag',
        'environmental_investment_index',
        'data_quality_flag', 'sensor_calibration_status'
    ]

    locations = [
        ('United States', 'New York', 40.7128, -74.0060, 78, 10),
        ('United States', 'Los Angeles', 34.0522, -118.2437, 78, 71),
        ('United States', 'Chicago', 41.8781, -87.6298, 78, 181),
        ('United States', 'Houston', 29.7604, -95.3698, 78, 15),
        ('China', 'Beijing', 39.9042, 116.4074, 55, 43),
        ('China', 'Shanghai', 31.2304, 121.4737, 55, 4),
        ('China', 'Guangzhou', 23.1291, 113.2644, 55, 21),
        ('China', 'Shenzhen', 22.5431, 114.0579, 55, 30),
        ('India', 'Delhi', 28.7041, 77.1025, 45, 216),
        ('India', 'Mumbai', 19.0760, 72.8777, 45, 14),
        ('India', 'Bangalore', 12.9716, 77.5946, 45, 920),
        ('India', 'Kolkata', 22.5726, 88.3639, 45, 9),
        ('Germany', 'Berlin', 52.5200, 13.4050, 90, 34),
        ('Germany', 'Munich', 48.1351, 11.5820, 90, 519),
        ('Germany', 'Hamburg', 53.5511, 9.9937, 90, 6),
        ('United Kingdom', 'London', 51.5074, -0.1278, 88, 11),
        ('United Kingdom', 'Manchester', 53.4808, -2.2426, 88, 38),
        ('United Kingdom', 'Birmingham', 52.4862, -1.8904, 88, 140),
        ('Japan', 'Tokyo', 35.6762, 139.6503, 85, 40),
        ('Japan', 'Osaka', 34.6937, 135.5023, 85, 12),
        ('Brazil', 'São Paulo', -23.5505, -46.6333, 50, 760),
        ('Brazil', 'Rio de Janeiro', -22.9068, -43.1729, 50, 11),
        ('Australia', 'Sydney', -33.8688, 151.2093, 82, 58),
        ('Australia', 'Melbourne', -37.8136, 144.9631, 82, 31),
        ('Nigeria', 'Lagos', 6.5244, 3.3792, 30, 41),
        ('Nigeria', 'Abuja', 9.0579, 7.4951, 30, 840),
        ('South Korea', 'Seoul', 37.5665, 126.9780, 75, 38),
        ('South Korea', 'Busan', 35.1796, 129.0756, 75, 20),
        ('France', 'Paris', 48.8566, 2.3522, 86, 35),
        ('France', 'Lyon', 45.7640, 4.8357, 86, 175),
        ('Russia', 'Moscow', 55.7558, 37.6173, 40, 156),
        ('Russia', 'Saint Petersburg', 59.9311, 30.3609, 40, 3),
        ('South Africa', 'Johannesburg', -26.2041, 28.0473, 52, 1753),
        ('South Africa', 'Cape Town', -33.9249, 18.4241, 52, 0),
        ('Mexico', 'Mexico City', 19.4326, -99.1332, 48, 2240),
        ('Mexico', 'Guadalajara', 20.6597, -103.3496, 48, 1566),
        ('Indonesia', 'Jakarta', -6.2088, 106.8456, 38, 8),
        ('Indonesia', 'Surabaya', -7.2575, 112.7521, 38, 3),
    ]

    station_types = ['Urban Background', 'Traffic', 'Industrial', 'Suburban', 'Rural Background']
    sectors = DatasetConfig.INDUSTRY_SECTORS
    quality_flags = ['Valid', 'Suspect', 'Missing_Interpolated']
    quality_weights = [0.92, 0.05, 0.03]
    calib_flags = ['Calibrated', 'Due_Calibration', 'Uncalibrated']
    calib_weights = [0.85, 0.10, 0.05]

    # Monthly pollution factors
    monthly_factors = {
        1: 1.35, 2: 1.30, 3: 1.15, 4: 0.95, 5: 0.85, 6: 0.80,
        7: 0.75, 8: 0.78, 9: 0.88, 10: 1.00, 11: 1.20, 12: 1.40
    }
    hourly_factors = {
        0: 0.60, 1: 0.55, 2: 0.50, 3: 0.48, 4: 0.50, 5: 0.58,
        6: 0.72, 7: 0.90, 8: 1.15, 9: 1.10, 10: 1.00, 11: 0.95,
        12: 0.92, 13: 0.90, 14: 0.88, 15: 0.90, 16: 0.95, 17: 1.10,
        18: 1.20, 19: 1.15, 20: 1.05, 21: 0.92, 22: 0.80, 23: 0.70
    }

    seed_file = DatasetConfig.DATA_DIR / 'air_quality_part_001.csv'
    start_time = time.time()
    total_bytes = 0
    record_id = 0
    base_date = datetime(2015, 1, 1)

    print(f"   📝 Writing to: {seed_file}")
    print(f"   🎯 Target size: {target_mb} MB")

    with open(seed_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        day_offset = 0
        last_print = time.time()

        while total_bytes < target_bytes:
            dt_base = base_date + timedelta(days=day_offset)
            if dt_base > datetime(2025, 12, 31):
                day_offset = 0
                dt_base = base_date

            for hour in range(0, 24, 3):
                dt = dt_base.replace(hour=hour)
                year = dt.year
                month = dt.month
                day = dt.day
                dow = dt.weekday()
                is_wknd = int(dow >= 5)

                m_factor = monthly_factors[month]
                h_factor = hourly_factors[hour]
                y_trend = 1.0 - ((year - 2015) * 0.008)

                # Pick ~80% of stations each reading
                active = random.sample(locations, k=int(len(locations) * 0.8))

                for loc in active:
                    country, city, lat, lon, reg_score, elev = loc
                    record_id += 1

                    ind_index = round(random.uniform(40, 100), 1)
                    ind_mult = 0.5 + (ind_index / 100.0) * 1.2
                    reg_damp = 1.0 - (reg_score / 200.0)

                    stype = random.choice(station_types)
                    type_mult = {'Traffic': 1.3, 'Industrial': 1.5,
                                 'Urban Background': 1.0, 'Suburban': 0.8,
                                 'Rural Background': 0.5}.get(stype, 1.0)

                    combined = m_factor * h_factor * y_trend * ind_mult * reg_damp * type_mult

                    pm25 = round(max(2, random.uniform(2, 200) * combined * 0.3 +
                                 np.random.normal(0, 5)), 2)
                    pm10 = round(max(5, pm25 * random.uniform(1.3, 2.2)), 2)
                    no2 = round(max(1, random.uniform(5, 80) * combined * 0.5 +
                                np.random.normal(0, 3)), 2)
                    so2 = round(max(0.5, random.uniform(2, 60) * combined * 0.4 +
                                np.random.normal(0, 2)), 2)
                    co = round(max(0.1, random.uniform(0.5, 20) * combined * 0.3 +
                               np.random.normal(0, 1)), 2)
                    o3 = round(max(5, random.uniform(10, 120) * (2 - combined) * 0.5 +
                               np.random.normal(0, 5)), 2)
                    voc = round(max(0.01, random.uniform(0.05, 2.0) * combined * 0.4), 3)
                    lead = round(max(0, random.uniform(0, 0.5) * combined * 0.3), 3)

                    # AQI
                    if pm25 <= 12: aqi = int(pm25 * 50 / 12)
                    elif pm25 <= 35.4: aqi = int(50 + (pm25 - 12) * 50 / 23.4)
                    elif pm25 <= 55.4: aqi = int(100 + (pm25 - 35.4) * 50 / 20)
                    elif pm25 <= 150.4: aqi = int(150 + (pm25 - 55.4) * 50 / 95)
                    elif pm25 <= 250.4: aqi = int(200 + (pm25 - 150.4) * 100 / 100)
                    else: aqi = min(500, int(300 + (pm25 - 250.4) * 200 / 250))

                    aqi = max(0, min(500, aqi))

                    if aqi <= 50: aqi_cat = 'Good'
                    elif aqi <= 100: aqi_cat = 'Moderate'
                    elif aqi <= 150: aqi_cat = 'Unhealthy for Sensitive Groups'
                    elif aqi <= 200: aqi_cat = 'Unhealthy'
                    elif aqi <= 300: aqi_cat = 'Very Unhealthy'
                    else: aqi_cat = 'Hazardous'

                    # Weather
                    base_temp = 30 - (abs(lat) * 0.5) + m_factor * 5
                    diurnal = -5 * np.cos((hour - 14) * np.pi / 12)
                    temp = round(base_temp + np.random.normal(0, 3) + diurnal, 1)
                    humidity = round(min(100, max(10, 70 - temp * 0.5 + np.random.normal(0, 10))), 1)
                    wind = round(max(0, np.random.weibull(2) * 5 + np.random.normal(0, 1.5)), 1)
                    wind_dir = random.randint(0, 359)
                    pressure = round(1013.25 - (elev * 0.12) + np.random.normal(0, 5), 1)
                    precip = round(max(0, np.random.exponential(0.5)), 2)

                    # Industry
                    factory_count = int(ind_index * 2.5 + np.random.normal(0, 15))
                    emission_std = min(5, max(1, reg_score // 20 + (1 if year >= 2020 else 0)))
                    carbon_tax = 1 if reg_score > 65 else 0
                    green_pct = round(min(100, max(0, reg_score * 0.6 +
                                     (year - 2015) * 1.5 + np.random.normal(0, 5))), 1)
                    sector = random.choice(sectors)
                    compliance = round(min(100, max(20, reg_score * 0.9 +
                                      np.random.normal(0, 8))), 1)

                    # Health
                    resp = round(max(0, aqi * 0.5 + np.random.normal(0, 10)), 1)
                    cardio = round(max(0, aqi * 0.2 + np.random.normal(0, 5)), 1)
                    hospital = round(max(0, aqi * 0.08 + np.random.normal(0, 2)), 1)
                    deaths = round(max(0, aqi * 0.015 + np.random.exponential(0.5)), 2)

                    # Policy
                    policy_change = int(year in [2016, 2018, 2020, 2022, 2024] and
                                        month == 1 and day == 1 and hour == 0)
                    env_invest = round(reg_score * 0.7 + (year - 2015) * 2.5 +
                                       np.random.normal(0, 3), 1)

                    quality = random.choices(quality_flags, weights=quality_weights)[0]
                    calib = random.choices(calib_flags, weights=calib_weights)[0]

                    row = [
                        record_id, dt.strftime('%Y-%m-%d %H:%M:%S'),
                        dt.strftime('%Y-%m-%d'), year, month, day, hour, dow, is_wknd,
                        f'AQ-{(record_id % 500) + 1000:06d}',
                        f'{city}_Station_{(record_id % 5) + 1}',
                        country, city,
                        round(lat + random.uniform(-0.5, 0.5), 6),
                        round(lon + random.uniform(-0.5, 0.5), 6),
                        elev, stype,
                        pm25, pm10, no2, so2, co, o3, voc, lead,
                        aqi, aqi_cat,
                        temp, humidity, wind, wind_dir, pressure, precip,
                        ind_index, factory_count, emission_std, carbon_tax,
                        green_pct, sector, compliance,
                        resp, cardio, hospital, deaths,
                        reg_score, policy_change, env_invest, quality, calib
                    ]
                    writer.writerow(row)

                    # Estimate bytes
                    total_bytes += len(','.join(str(v) for v in row).encode()) + 1

            day_offset += 1

            # Progress update every 2 seconds
            now = time.time()
            if now - last_print > 2:
                pct = (total_bytes / target_bytes) * 100
                mb = total_bytes / (1024 * 1024)
                rate = mb / (now - start_time) if (now - start_time) > 0 else 0
                eta = ((target_bytes - total_bytes) / (1024 * 1024)) / rate if rate > 0 else 0
                print(f"   📊 {mb:.1f}/{target_mb} MB ({pct:.0f}%) | "
                      f"{record_id:,} records | {rate:.1f} MB/s | ETA: {eta:.0f}s")
                last_print = now

    elapsed = time.time() - start_time
    actual_size = os.path.getsize(seed_file)
    actual_mb = actual_size / (1024 * 1024)

    print(f"\n   ✅ Generated {record_id:,} records")
    print(f"   📁 File: {seed_file}")
    print(f"   💾 Size: {actual_mb:.1f} MB")
    print(f"   ⏱️  Time: {elapsed:.1f}s")

    # Save metadata
    metadata = {
        'project': ProjectInfo.TITLE,
        'type': 'seed_data',
        'record_count': record_id,
        'generated_at': datetime.now().isoformat(),
        'generation_time_seconds': round(elapsed, 2),
        'total_size_bytes': actual_size,
        'total_size_mb': round(actual_mb, 2),
        'countries': list(set(loc[0] for loc in locations)),
        'cities': list(set(loc[1] for loc in locations)),
        'columns': headers,
        'column_count': len(headers),
        'date_range': {'start': '2015-01-01', 'end': '2025-12-31'},
        'files': [{'file_name': 'air_quality_part_001.csv',
                   'size_bytes': actual_size, 'rows': record_id}]
    }
    meta_path = DatasetConfig.DATA_DIR / 'dataset_metadata.json'
    with open(meta_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"\n{'='*60}")
    print(f"  ✅ SEEDING COMPLETE - {actual_mb:.1f} MB dataset created")
    print(f"{'='*60}\n")
    return seed_file


if __name__ == '__main__':
    seed_dataset(target_mb=50)
