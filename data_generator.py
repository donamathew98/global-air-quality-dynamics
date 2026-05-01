"""
============================================================
Data Generator - Global Air Quality Dynamics Project
============================================================
Generates 1.5GB+ of synthetic but realistic air quality
data covering 15 countries, 150+ cities, hourly readings
over 10 years (2015-2025).

Usage:
    python data_generator.py

The script generates data in chunks to manage memory and
writes CSV files to the data/ directory. Total output will
be approximately 1.5GB (configurable via .env).
============================================================
"""

import os
import sys
import csv
import json
import time
import random
import hashlib
import math
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from config import DatasetConfig, ProjectInfo


class AirQualityDataGenerator:
    """
    Generates large-scale synthetic air quality datasets
    with realistic temporal, spatial, and industrial patterns.
    """

    def __init__(self):
        DatasetConfig.ensure_directories()
        self.target_size = DatasetConfig.TARGET_SIZE_BYTES
        self.data_dir = DatasetConfig.DATA_DIR
        self.total_bytes_written = 0
        self.total_rows = 0
        self.file_index = 0
        self.chunk_size = 500_000  # rows per chunk
        self.rows_per_file = 2_000_000  # rows per CSV file
        self.file_list = []

        # Seed for reproducibility
        np.random.seed(42)
        random.seed(42)

        # Precompute lookup data
        self._build_station_registry()
        self._build_temporal_patterns()

    def _build_station_registry(self):
        """Create a registry of monitoring stations across all cities."""
        self.stations = []
        station_id = 1000

        for country, info in DatasetConfig.COUNTRIES_CITIES.items():
            for city in info['cities']:
                # Each city has 2-5 monitoring stations
                num_stations = random.randint(2, 5)
                for s in range(num_stations):
                    lat = round(random.uniform(*info['lat_range']), 6)
                    lon = round(random.uniform(*info['lon_range']), 6)
                    station = {
                        'station_id': f"AQ-{station_id:06d}",
                        'station_name': f"{city}_Station_{s+1}",
                        'country': country,
                        'city': city,
                        'latitude': lat,
                        'longitude': lon,
                        'elevation_m': random.randint(5, 2500),
                        'station_type': random.choice([
                            'Urban Background', 'Traffic', 'Industrial',
                            'Suburban', 'Rural Background'
                        ]),
                        'industrial_index': info['industrial_index'],
                        'regulation_score': info['regulation_score'],
                        'commissioning_year': random.randint(2005, 2018)
                    }
                    self.stations.append(station)
                    station_id += 1

        print(f"📡 Built registry of {len(self.stations)} monitoring stations "
              f"across {len(DatasetConfig.COUNTRIES_CITIES)} countries")

    def _build_temporal_patterns(self):
        """Precompute seasonal and diurnal patterns."""
        # Monthly pollution multipliers (Northern Hemisphere winter = higher)
        self.monthly_factors = {
            1: 1.35, 2: 1.30, 3: 1.15, 4: 0.95, 5: 0.85, 6: 0.80,
            7: 0.75, 8: 0.78, 9: 0.88, 10: 1.00, 11: 1.20, 12: 1.40
        }

        # Hourly pollution patterns (traffic peaks at 8am and 6pm)
        self.hourly_factors = {
            0: 0.60, 1: 0.55, 2: 0.50, 3: 0.48, 4: 0.50, 5: 0.58,
            6: 0.72, 7: 0.90, 8: 1.15, 9: 1.10, 10: 1.00, 11: 0.95,
            12: 0.92, 13: 0.90, 14: 0.88, 15: 0.90, 16: 0.95, 17: 1.10,
            18: 1.20, 19: 1.15, 20: 1.05, 21: 0.92, 22: 0.80, 23: 0.70
        }

        # Year-over-year trend (slight improvement in well-regulated countries)
        self.yearly_trend = {}
        for year in range(DatasetConfig.START_YEAR, DatasetConfig.END_YEAR + 1):
            years_from_start = year - DatasetConfig.START_YEAR
            self.yearly_trend[year] = 1.0 - (years_from_start * 0.008)

        # Day of week factors
        self.dow_factors = {
            0: 1.05, 1: 1.08, 2: 1.08, 3: 1.06, 4: 1.04,
            5: 0.88, 6: 0.82
        }

    def _compute_pollutant_value(self, pollutant, base_range, station,
                                  year, month, hour, day_of_week):
        """
        Compute a realistic pollutant reading with multiple
        overlapping patterns and noise.
        """
        base_min, base_max = base_range
        base_value = random.uniform(base_min, base_max * 0.4)

        # Industrial influence
        industrial_mult = 0.5 + (station['industrial_index'] / 100.0) * 1.2

        # Regulation dampening
        reg_dampening = 1.0 - (station['regulation_score'] / 200.0)

        # Temporal patterns
        monthly_mult = self.monthly_factors.get(month, 1.0)
        # Flip seasons for Southern Hemisphere
        if station['latitude'] < 0:
            flipped_month = ((month + 5) % 12) + 1
            monthly_mult = self.monthly_factors.get(flipped_month, 1.0)

        hourly_mult = self.hourly_factors.get(hour, 1.0)
        yearly_mult = self.yearly_trend.get(year, 1.0)

        # Stronger yearly improvement if high regulation
        if station['regulation_score'] > 70:
            yearly_mult = 1.0 - ((year - DatasetConfig.START_YEAR) * 0.015)

        dow_mult = self.dow_factors.get(day_of_week, 1.0)

        # Station type influence
        type_mult = {
            'Traffic': 1.3, 'Industrial': 1.5, 'Urban Background': 1.0,
            'Suburban': 0.8, 'Rural Background': 0.5
        }.get(station['station_type'], 1.0)

        # Combine all factors
        value = (base_value * industrial_mult * reg_dampening *
                 monthly_mult * hourly_mult * yearly_mult *
                 dow_mult * type_mult)

        # Add Gaussian noise (±15%)
        noise = np.random.normal(0, value * 0.15)
        value = max(base_min, value + noise)
        value = min(base_max, value)

        return round(value, 2)

    def _compute_aqi(self, pm25, pm10, no2, so2, co, o3):
        """Compute Air Quality Index from pollutant concentrations."""
        # Simplified AQI calculation based on PM2.5 as primary pollutant
        if pm25 <= 12.0:
            aqi = (50 / 12.0) * pm25
        elif pm25 <= 35.4:
            aqi = 50 + ((100 - 50) / (35.4 - 12.0)) * (pm25 - 12.0)
        elif pm25 <= 55.4:
            aqi = 100 + ((150 - 100) / (55.4 - 35.4)) * (pm25 - 35.4)
        elif pm25 <= 150.4:
            aqi = 150 + ((200 - 150) / (150.4 - 55.4)) * (pm25 - 55.4)
        elif pm25 <= 250.4:
            aqi = 200 + ((300 - 200) / (250.4 - 150.4)) * (pm25 - 150.4)
        else:
            aqi = 300 + ((500 - 300) / (500.4 - 250.4)) * (pm25 - 250.4)

        return min(500, max(0, round(aqi)))

    def _get_aqi_category(self, aqi):
        """Get AQI category from value."""
        for category, (low, high) in DatasetConfig.AQI_CATEGORIES.items():
            if low <= aqi <= high:
                return category
        return 'Hazardous'

    def _compute_weather(self, station, month, hour):
        """Generate correlated weather data."""
        # Base temperature by latitude and month
        lat = abs(station['latitude'])
        base_temp = 30 - (lat * 0.5) + self.monthly_factors[month] * 5
        temp_variation = np.random.normal(0, 3)
        # Diurnal temperature cycle
        diurnal = -5 * math.cos((hour - 14) * math.pi / 12)
        temperature = round(base_temp + temp_variation + diurnal, 1)

        # Humidity inversely related to temperature
        humidity = round(min(100, max(10, 70 - temperature * 0.5 +
                                       np.random.normal(0, 10))), 1)

        # Wind speed
        wind_speed = round(max(0, np.random.weibull(2) * 5 +
                                np.random.normal(0, 1.5)), 1)
        wind_direction = random.randint(0, 359)

        # Atmospheric pressure
        pressure = round(1013.25 - (station['elevation_m'] * 0.12) +
                         np.random.normal(0, 5), 1)

        # Precipitation probability
        precip = round(max(0, np.random.exponential(0.5)), 2)

        return temperature, humidity, wind_speed, wind_direction, pressure, precip

    def _get_industry_data(self, station, year, month):
        """Generate industrial activity metrics."""
        base_index = station['industrial_index']

        # Seasonal industrial variation
        seasonal = self.monthly_factors.get(month, 1.0)

        # Yearly growth/decline
        growth = 1.0 + (year - DatasetConfig.START_YEAR) * 0.01

        industrial_output = round(base_index * seasonal * growth +
                                   np.random.normal(0, 5), 1)

        # Number of active factories (correlated with industrial index)
        factory_count = int(base_index * 2.5 + np.random.normal(0, 15))

        # Emission standard level (1-5, higher = stricter)
        emission_standard = min(5, max(1,
            int(station['regulation_score'] / 20) +
            (1 if year >= 2020 else 0)))

        # Carbon tax presence
        has_carbon_tax = station['regulation_score'] > 65

        # Green energy percentage
        green_energy_pct = round(min(100, max(0,
            (station['regulation_score'] * 0.6) +
            (year - DatasetConfig.START_YEAR) * 1.5 +
            np.random.normal(0, 5))), 1)

        # Industrial sector
        sector = random.choice(DatasetConfig.INDUSTRY_SECTORS)

        # Compliance rate
        compliance_rate = round(min(100, max(20,
            station['regulation_score'] * 0.9 +
            np.random.normal(0, 8))), 1)

        return (industrial_output, factory_count, emission_standard,
                has_carbon_tax, green_energy_pct, sector, compliance_rate)

    def _get_health_impact(self, aqi, population_factor=1.0):
        """Estimate health impact metrics."""
        # Respiratory cases per 100k population (correlated with AQI)
        resp_cases = round(max(0, aqi * 0.5 + np.random.normal(0, 10)) *
                           population_factor, 1)

        # Cardiovascular incidents per 100k
        cardio_cases = round(max(0, aqi * 0.2 + np.random.normal(0, 5)) *
                             population_factor, 1)

        # Hospital admissions per 100k
        hospital_admissions = round(max(0,
            (aqi * 0.08 + np.random.normal(0, 2)) * population_factor), 1)

        # Premature deaths estimate per million
        premature_deaths = round(max(0,
            aqi * 0.015 + np.random.exponential(0.5)), 2)

        return resp_cases, cardio_cases, hospital_admissions, premature_deaths

    def _get_csv_header(self):
        """Return CSV column headers."""
        return [
            # Identifiers
            'record_id', 'timestamp', 'date', 'year', 'month', 'day',
            'hour', 'day_of_week', 'is_weekend',
            # Location
            'station_id', 'station_name', 'country', 'city',
            'latitude', 'longitude', 'elevation_m', 'station_type',
            # Pollutants
            'PM2_5', 'PM10', 'NO2', 'SO2', 'CO', 'O3', 'VOC', 'Lead',
            # Air Quality Index
            'AQI', 'AQI_category',
            # Weather
            'temperature_c', 'humidity_pct', 'wind_speed_mps',
            'wind_direction_deg', 'pressure_hpa', 'precipitation_mm',
            # Industrial Metrics
            'industrial_output_index', 'active_factory_count',
            'emission_standard_level', 'has_carbon_tax',
            'green_energy_pct', 'dominant_industry_sector',
            'regulatory_compliance_rate',
            # Health Impact
            'respiratory_cases_per_100k', 'cardiovascular_cases_per_100k',
            'hospital_admissions_per_100k', 'premature_deaths_per_million',
            # Policy
            'regulation_stringency_score', 'policy_change_flag',
            'environmental_investment_index',
            # Data Quality
            'data_quality_flag', 'sensor_calibration_status'
        ]

    def _generate_record(self, record_id, station, dt):
        """Generate a single data record."""
        year = dt.year
        month = dt.month
        day = dt.day
        hour = dt.hour
        dow = dt.weekday()
        is_weekend = dow >= 5

        # Generate pollutant values
        pm25 = self._compute_pollutant_value(
            'PM2.5', DatasetConfig.POLLUTANT_RANGES['PM2.5'],
            station, year, month, hour, dow)
        pm10 = self._compute_pollutant_value(
            'PM10', DatasetConfig.POLLUTANT_RANGES['PM10'],
            station, year, month, hour, dow)
        no2 = self._compute_pollutant_value(
            'NO2', DatasetConfig.POLLUTANT_RANGES['NO2'],
            station, year, month, hour, dow)
        so2 = self._compute_pollutant_value(
            'SO2', DatasetConfig.POLLUTANT_RANGES['SO2'],
            station, year, month, hour, dow)
        co = self._compute_pollutant_value(
            'CO', DatasetConfig.POLLUTANT_RANGES['CO'],
            station, year, month, hour, dow)
        o3 = self._compute_pollutant_value(
            'O3', DatasetConfig.POLLUTANT_RANGES['O3'],
            station, year, month, hour, dow)
        voc = self._compute_pollutant_value(
            'VOC', DatasetConfig.POLLUTANT_RANGES['VOC'],
            station, year, month, hour, dow)
        lead = self._compute_pollutant_value(
            'Lead', DatasetConfig.POLLUTANT_RANGES['Lead'],
            station, year, month, hour, dow)

        # Compute AQI
        aqi = self._compute_aqi(pm25, pm10, no2, so2, co, o3)
        aqi_category = self._get_aqi_category(aqi)

        # Weather
        temp, humidity, wind, wind_dir, pressure, precip = \
            self._compute_weather(station, month, hour)

        # Industrial data
        (ind_output, factory_count, emission_std, carbon_tax,
         green_pct, sector, compliance) = \
            self._get_industry_data(station, year, month)

        # Health impact
        resp, cardio, hospital, deaths = self._get_health_impact(aqi)

        # Policy indicators
        reg_score = station['regulation_score']
        # Policy changes happen at specific years
        policy_change = (year in [2016, 2018, 2020, 2022, 2024] and
                         month == 1 and day == 1 and hour == 0)
        env_investment = round(reg_score * 0.7 +
                                (year - 2015) * 2.5 +
                                np.random.normal(0, 3), 1)

        # Data quality
        quality_flag = random.choices(
            ['Valid', 'Suspect', 'Missing_Interpolated'],
            weights=[0.92, 0.05, 0.03]
        )[0]
        calibration = random.choices(
            ['Calibrated', 'Due_Calibration', 'Uncalibrated'],
            weights=[0.85, 0.10, 0.05]
        )[0]

        return [
            record_id,
            dt.strftime('%Y-%m-%d %H:%M:%S'),
            dt.strftime('%Y-%m-%d'),
            year, month, day, hour, dow, int(is_weekend),
            station['station_id'], station['station_name'],
            station['country'], station['city'],
            station['latitude'], station['longitude'],
            station['elevation_m'], station['station_type'],
            pm25, pm10, no2, so2, co, o3, voc, lead,
            aqi, aqi_category,
            temp, humidity, wind, wind_dir, pressure, precip,
            ind_output, factory_count, emission_std,
            int(carbon_tax), green_pct, sector, compliance,
            resp, cardio, hospital, deaths,
            reg_score, int(policy_change), env_investment,
            quality_flag, calibration
        ]

    def generate(self):
        """Main generation loop - creates CSV files until target size is met."""
        print(f"\n{'='*60}")
        print(f"  🌍 GLOBAL AIR QUALITY DATA GENERATOR")
        print(f"  📦 Target size: {DatasetConfig.TARGET_SIZE_GB:.1f} GB")
        print(f"  📡 Stations: {len(self.stations)}")
        print(f"  📅 Period: {DatasetConfig.START_YEAR} - {DatasetConfig.END_YEAR}")
        print(f"{'='*60}\n")

        headers = self._get_csv_header()
        start_time = time.time()
        record_id = 0

        # Calculate the date range
        start_date = datetime(DatasetConfig.START_YEAR, 1, 1)
        end_date = datetime(DatasetConfig.END_YEAR, 12, 31, 23, 0, 0)

        # We'll iterate through time periods and stations
        # Each iteration = 1 day across all stations (every 3 hours)
        total_days = (end_date - start_date).days + 1
        hours_per_reading = 3  # Reading every 3 hours

        current_file = None
        writer = None
        current_file_rows = 0
        file_path = None

        # Progress bar based on estimated total size
        pbar = tqdm(
            total=self.target_size,
            unit='B',
            unit_scale=True,
            desc='📝 Generating data',
            bar_format='{l_bar}{bar:40}{r_bar}'
        )

        try:
            current_date = start_date
            day_count = 0

            while self.total_bytes_written < self.target_size:
                # Cycle through dates
                if current_date > end_date:
                    current_date = start_date  # Restart with different noise

                # Open new file if needed
                if current_file is None or current_file_rows >= self.rows_per_file:
                    if current_file is not None:
                        current_file.close()
                        file_size = os.path.getsize(file_path)
                        self.file_list.append({
                            'file_name': os.path.basename(file_path),
                            'file_path': str(file_path),
                            'rows': current_file_rows,
                            'size_bytes': file_size
                        })
                        print(f"\n   ✅ Completed: {os.path.basename(file_path)} "
                              f"({current_file_rows:,} rows, "
                              f"{file_size / (1024*1024):.1f} MB)")

                    self.file_index += 1
                    file_path = self.data_dir / f"air_quality_part_{self.file_index:03d}.csv"
                    current_file = open(file_path, 'w', newline='', encoding='utf-8')
                    writer = csv.writer(current_file)
                    writer.writerow(headers)
                    current_file_rows = 0

                # Generate records for this day
                chunk_records = []
                for hour in range(0, 24, hours_per_reading):
                    dt = current_date.replace(hour=hour)

                    # Select a subset of stations for this time slot
                    # (not all stations report at every time - adds realism)
                    active_stations = random.sample(
                        self.stations,
                        k=min(len(self.stations), int(len(self.stations) * 0.85))
                    )

                    for station in active_stations:
                        # Skip if station wasn't commissioned yet
                        if dt.year < station['commissioning_year']:
                            continue

                        record_id += 1
                        record = self._generate_record(record_id, station, dt)
                        chunk_records.append(record)

                # Write chunk
                if chunk_records:
                    writer.writerows(chunk_records)
                    current_file.flush()

                    # Track size
                    row_bytes = sum(len(','.join(str(v) for v in row).encode())
                                    for row in chunk_records)
                    self.total_bytes_written += row_bytes
                    self.total_rows += len(chunk_records)
                    current_file_rows += len(chunk_records)

                    pbar.update(row_bytes)

                current_date += timedelta(days=1)
                day_count += 1

                # Status update every 100 days
                if day_count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = self.total_bytes_written / elapsed if elapsed > 0 else 0
                    remaining = (self.target_size - self.total_bytes_written) / rate \
                        if rate > 0 else 0
                    pbar.set_postfix({
                        'rows': f'{self.total_rows:,.0f}',
                        'ETA': f'{remaining/60:.1f}m'
                    })

        finally:
            pbar.close()
            if current_file is not None:
                current_file.close()
                file_size = os.path.getsize(file_path)
                self.file_list.append({
                    'file_name': os.path.basename(file_path),
                    'file_path': str(file_path),
                    'rows': current_file_rows,
                    'size_bytes': file_size
                })

        elapsed = time.time() - start_time

        # Generate metadata
        self._save_metadata(elapsed)

        # Print summary
        print(f"\n{'='*60}")
        print(f"  ✅ DATA GENERATION COMPLETE")
        print(f"{'='*60}")
        print(f"  📊 Total records:     {self.total_rows:>15,}")
        print(f"  📁 Total files:       {len(self.file_list):>15}")
        total_mb = self.total_bytes_written / (1024 * 1024)
        total_gb = total_mb / 1024
        print(f"  💾 Total size:        {total_gb:>14.2f} GB")
        print(f"  ⏱️  Generation time:   {elapsed:>14.1f} seconds")
        print(f"  📂 Output directory:  {self.data_dir}")
        print(f"{'='*60}\n")

        return self.file_list

    def _save_metadata(self, elapsed_time):
        """Save dataset metadata as JSON."""
        metadata = {
            'project': ProjectInfo.TITLE,
            'version': ProjectInfo.VERSION,
            'generated_at': datetime.now().isoformat(),
            'generation_time_seconds': round(elapsed_time, 2),
            'total_records': self.total_rows,
            'total_files': len(self.file_list),
            'total_size_bytes': self.total_bytes_written,
            'total_size_gb': round(self.total_bytes_written / (1024**3), 3),
            'target_size_gb': DatasetConfig.TARGET_SIZE_GB,
            'columns': self._get_csv_header(),
            'column_count': len(self._get_csv_header()),
            'countries_covered': list(DatasetConfig.COUNTRIES_CITIES.keys()),
            'country_count': len(DatasetConfig.COUNTRIES_CITIES),
            'total_stations': len(self.stations),
            'date_range': {
                'start': f"{DatasetConfig.START_YEAR}-01-01",
                'end': f"{DatasetConfig.END_YEAR}-12-31"
            },
            'pollutants_measured': list(DatasetConfig.POLLUTANT_RANGES.keys()),
            'industry_sectors': DatasetConfig.INDUSTRY_SECTORS,
            'files': self.file_list,
            'stations': self.stations[:5],  # Sample stations
            'checksum_algorithm': 'sha256'
        }

        metadata_path = self.data_dir / DatasetConfig.METADATA_FILE
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

        print(f"   📋 Metadata saved to: {metadata_path}")


def main():
    """Entry point for data generation."""
    print("\n" + "🌍 " * 20)
    print("  GLOBAL AIR QUALITY DYNAMICS & INDUSTRIAL POLICY")
    print("  Dataset Generation Script v1.0")
    print("🌍 " * 20 + "\n")

    generator = AirQualityDataGenerator()
    files = generator.generate()

    print("📁 Generated files:")
    for f in files:
        size_mb = f['size_bytes'] / (1024 * 1024)
        print(f"   • {f['file_name']}: {f['rows']:,} rows ({size_mb:.1f} MB)")

    print("\n✅ Data generation complete!")
    print("📌 Next step: Run 's3_manager.py' to upload data to AWS S3")
    print("   Command: python s3_manager.py upload\n")


if __name__ == '__main__':
    main()
