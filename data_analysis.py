"""
============================================================
Data Analysis - Global Air Quality Dynamics Project
============================================================
Performs comprehensive analysis on the air quality dataset:
  - Descriptive statistics & distributions
  - Temporal trend analysis
  - Country/city comparisons
  - Industrial impact correlation
  - Policy effectiveness evaluation
  - Health impact assessment
  - Generates publication-ready visualizations

Usage:
    python data_analysis.py
============================================================
"""

import os
import sys
import json
import warnings
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

from config import DatasetConfig, ProjectInfo

warnings.filterwarnings('ignore')

# Set plot style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")
COLORS = ['#2ecc71', '#e74c3c', '#3498db', '#f39c12', '#9b59b6',
          '#1abc9c', '#e67e22', '#34495e', '#16a085', '#c0392b']


class AirQualityAnalyzer:
    """Comprehensive analysis engine for the air quality dataset."""

    def __init__(self, sample_size=500_000):
        DatasetConfig.ensure_directories()
        self.charts_dir = DatasetConfig.CHARTS_DIR
        self.reports_dir = DatasetConfig.REPORTS_DIR
        self.sample_size = sample_size
        self.df = None
        self.results = {}

    def load_data(self):
        """Load data from CSV files (samples for memory efficiency)."""
        data_dir = DatasetConfig.DATA_DIR
        csv_files = sorted(data_dir.glob('air_quality_part_*.csv'))

        if not csv_files:
            print(" No data files found. Run data_generator.py first!")
            sys.exit(1)

        print(f" Found {len(csv_files)} data files")

        # Read a sample from each file proportionally
        samples_per_file = max(1000, self.sample_size // len(csv_files))
        frames = []

        for f in csv_files:
            try:
                # Count rows first
                with open(f, 'r') as fh:
                    total = sum(1 for _ in fh) - 1
                skip = sorted(np.random.choice(
                    range(1, total + 1),
                    size=max(0, total - samples_per_file),
                    replace=False
                )) if total > samples_per_file else []
                chunk = pd.read_csv(f, skiprows=skip)
                frames.append(chunk)
                print(f"    {f.name}: {len(chunk):,} rows sampled from {total:,}")
            except Exception as e:
                print(f"     Error reading {f.name}: {e}")

        self.df = pd.concat(frames, ignore_index=True)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df['date'] = pd.to_datetime(self.df['date'])

        print(f"\n Loaded {len(self.df):,} records for analysis")
        print(f"   Columns: {len(self.df.columns)}")
        print(f"   Memory: {self.df.memory_usage(deep=True).sum() / 1e6:.1f} MB")
        return self

    def descriptive_statistics(self):
        """Generate descriptive statistics."""
        print("\n Computing descriptive statistics...")
        pollutants = ['PM2_5', 'PM10', 'NO2', 'SO2', 'CO', 'O3', 'VOC', 'Lead']
        stats_df = self.df[pollutants].describe().round(2)
        stats_df.to_csv(self.reports_dir / 'descriptive_statistics.csv')

        # AQI distribution
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Air Quality Overview', fontsize=18, fontweight='bold')

        # AQI histogram
        ax = axes[0, 0]
        self.df['AQI'].hist(bins=50, ax=ax, color=COLORS[0], alpha=0.7, edgecolor='white')
        ax.set_title('AQI Distribution', fontsize=14)
        ax.set_xlabel('AQI Value')
        ax.set_ylabel('Frequency')
        ax.axvline(self.df['AQI'].mean(), color='red', linestyle='--', label=f"Mean: {self.df['AQI'].mean():.0f}")
        ax.legend()

        # AQI by category
        ax = axes[0, 1]
        cat_counts = self.df['AQI_category'].value_counts()
        cat_counts.plot(kind='bar', ax=ax, color=COLORS[:len(cat_counts)], edgecolor='white')
        ax.set_title('AQI Category Distribution', fontsize=14)
        ax.set_ylabel('Count')
        ax.tick_params(axis='x', rotation=45)

        # Pollutant box plots
        ax = axes[1, 0]
        self.df[['PM2_5', 'PM10', 'NO2', 'SO2']].plot(kind='box', ax=ax)
        ax.set_title('Pollutant Distributions', fontsize=14)
        ax.set_ylabel('Concentration (ug/m^3)')

        # Correlation heatmap
        ax = axes[1, 1]
        corr = self.df[pollutants].corr()
        sns.heatmap(corr, annot=True, fmt='.2f', cmap='RdYlGn_r', ax=ax,
                    square=True, linewidths=0.5)
        ax.set_title('Pollutant Correlations', fontsize=14)

        plt.tight_layout()
        plt.savefig(self.charts_dir / '01_overview.png', dpi=150, bbox_inches='tight')
        plt.close()
        self.results['descriptive'] = stats_df.to_dict()
        print("    Saved: 01_overview.png")

    def temporal_analysis(self):
        """Analyze temporal trends."""
        print("\n Performing temporal analysis...")
        fig, axes = plt.subplots(2, 2, figsize=(18, 12))
        fig.suptitle('Temporal Air Quality Trends', fontsize=18, fontweight='bold')

        # Yearly trend
        ax = axes[0, 0]
        yearly = self.df.groupby('year')['AQI'].agg(['mean', 'std']).reset_index()
        ax.plot(yearly['year'], yearly['mean'], 'o-', color=COLORS[2], linewidth=2, markersize=8)
        ax.fill_between(yearly['year'], yearly['mean'] - yearly['std'],
                        yearly['mean'] + yearly['std'], alpha=0.2, color=COLORS[2])
        ax.set_title('Average AQI by Year', fontsize=14)
        ax.set_xlabel('Year')
        ax.set_ylabel('AQI')

        # Monthly pattern
        ax = axes[0, 1]
        monthly = self.df.groupby('month')['AQI'].mean().sort_index()
        months_map = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',
                      7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
        labels = [months_map.get(m, str(m)) for m in monthly.index]
        ax.bar(range(len(monthly)), monthly.values, color=COLORS[3], edgecolor='white')
        ax.set_xticks(range(len(monthly)))
        ax.set_xticklabels(labels, rotation=45)
        ax.set_title('Average AQI by Month', fontsize=14)
        ax.set_ylabel('AQI')

        # Hourly pattern
        ax = axes[1, 0]
        hourly = self.df.groupby('hour')['AQI'].mean()
        ax.plot(hourly.index, hourly.values, '-o', color=COLORS[4], linewidth=2)
        ax.fill_between(hourly.index, hourly.values, alpha=0.3, color=COLORS[4])
        ax.set_title('Average AQI by Hour', fontsize=14)
        ax.set_xlabel('Hour of Day')
        ax.set_ylabel('AQI')

        # Day of week
        ax = axes[1, 1]
        dow = self.df.groupby('day_of_week')['AQI'].mean()
        days = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
        ax.bar(range(7), dow.values, color=COLORS[1], edgecolor='white')
        ax.set_xticks(range(7))
        ax.set_xticklabels(days)
        ax.set_title('Average AQI by Day of Week', fontsize=14)
        ax.set_ylabel('AQI')

        plt.tight_layout()
        plt.savefig(self.charts_dir / '02_temporal.png', dpi=150, bbox_inches='tight')
        plt.close()
        print("    Saved: 02_temporal.png")

    def geographic_analysis(self):
        """Analyze geographic patterns."""
        print("\n Performing geographic analysis...")
        fig, axes = plt.subplots(1, 2, figsize=(20, 8))
        fig.suptitle('Geographic Air Quality Comparison', fontsize=18, fontweight='bold')

        # Country comparison
        ax = axes[0]
        country_aqi = self.df.groupby('country')['AQI'].mean().sort_values(ascending=True)
        bars = ax.barh(country_aqi.index, country_aqi.values, color=COLORS[0], edgecolor='white')
        ax.set_title('Average AQI by Country', fontsize=14)
        ax.set_xlabel('Average AQI')
        for bar, val in zip(bars, country_aqi.values):
            ax.text(val + 1, bar.get_y() + bar.get_height()/2, f'{val:.0f}',
                    va='center', fontsize=9)

        # Top 20 most polluted cities
        ax = axes[1]
        city_aqi = self.df.groupby('city')['AQI'].mean().nlargest(20).sort_values()
        ax.barh(city_aqi.index, city_aqi.values, color=COLORS[1], edgecolor='white')
        ax.set_title('Top 20 Most Polluted Cities', fontsize=14)
        ax.set_xlabel('Average AQI')

        plt.tight_layout()
        plt.savefig(self.charts_dir / '03_geographic.png', dpi=150, bbox_inches='tight')
        plt.close()
        print("    Saved: 03_geographic.png")

    def industrial_impact_analysis(self):
        """Analyze industrial activity impact on air quality."""
        print("\n Analyzing industrial impact...")
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Industrial Activity & Air Quality', fontsize=18, fontweight='bold')

        # Industrial output vs AQI scatter
        ax = axes[0, 0]
        sample = self.df.sample(min(5000, len(self.df)))
        ax.scatter(sample['industrial_output_index'], sample['AQI'],
                   alpha=0.3, s=10, c=COLORS[2])
        z = np.polyfit(sample['industrial_output_index'], sample['AQI'], 1)
        p = np.poly1d(z)
        x_line = np.linspace(sample['industrial_output_index'].min(),
                             sample['industrial_output_index'].max(), 100)
        ax.plot(x_line, p(x_line), 'r--', linewidth=2, label=f'Trend (slope={z[0]:.2f})')
        ax.set_title('Industrial Output vs AQI', fontsize=14)
        ax.set_xlabel('Industrial Output Index')
        ax.set_ylabel('AQI')
        ax.legend()

        # AQI by industry sector
        ax = axes[0, 1]
        sector_aqi = self.df.groupby('dominant_industry_sector')['AQI'].mean().sort_values()
        sector_aqi.plot(kind='barh', ax=ax, color=COLORS[3], edgecolor='white')
        ax.set_title('AQI by Industry Sector', fontsize=14)
        ax.set_xlabel('Average AQI')

        # Green energy vs AQI
        ax = axes[1, 0]
        ax.scatter(sample['green_energy_pct'], sample['AQI'],
                   alpha=0.3, s=10, c=COLORS[0])
        z2 = np.polyfit(sample['green_energy_pct'], sample['AQI'], 1)
        p2 = np.poly1d(z2)
        x2 = np.linspace(0, 100, 100)
        ax.plot(x2, p2(x2), 'r--', linewidth=2, label=f'Trend (slope={z2[0]:.2f})')
        ax.set_title('Green Energy % vs AQI', fontsize=14)
        ax.set_xlabel('Green Energy (%)')
        ax.set_ylabel('AQI')
        ax.legend()

        # Carbon tax impact
        ax = axes[1, 1]
        tax_groups = self.df.groupby('has_carbon_tax')['AQI'].agg(['mean', 'std'])
        labels = ['No Carbon Tax', 'Has Carbon Tax']
        bars = ax.bar(labels, tax_groups['mean'], yerr=tax_groups['std'],
                      color=[COLORS[1], COLORS[0]], edgecolor='white', capsize=5)
        ax.set_title('Carbon Tax Impact on AQI', fontsize=14)
        ax.set_ylabel('Average AQI')
        for bar, val in zip(bars, tax_groups['mean']):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                    f'{val:.0f}', ha='center', fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.charts_dir / '04_industrial.png', dpi=150, bbox_inches='tight')
        plt.close()
        print("    Saved: 04_industrial.png")

    def policy_effectiveness(self):
        """Evaluate environmental policy effectiveness."""
        print("\n Evaluating policy effectiveness...")
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Policy Effectiveness Analysis', fontsize=18, fontweight='bold')

        # Regulation score vs AQI
        ax = axes[0, 0]
        reg_aqi = self.df.groupby('regulation_stringency_score')[['AQI']].mean().reset_index()
        ax.scatter(reg_aqi['regulation_stringency_score'], reg_aqi['AQI'],
                   c=COLORS[2], s=60, alpha=0.7)
        r, p_val = stats.pearsonr(reg_aqi['regulation_stringency_score'], reg_aqi['AQI'])
        ax.set_title(f'Regulation Score vs AQI (r={r:.3f}, p={p_val:.4f})', fontsize=13)
        ax.set_xlabel('Regulation Stringency Score')
        ax.set_ylabel('Average AQI')

        # Emission standards vs pollutants
        ax = axes[0, 1]
        emit_group = self.df.groupby('emission_standard_level')[['PM2_5', 'NO2', 'SO2']].mean()
        emit_group.plot(kind='bar', ax=ax, color=COLORS[:3], edgecolor='white')
        ax.set_title('Emission Standards vs Pollutant Levels', fontsize=14)
        ax.set_xlabel('Emission Standard Level')
        ax.set_ylabel('Concentration (ug/m^3)')
        ax.legend(title='Pollutant')

        # Compliance rate distribution
        ax = axes[1, 0]
        for country in self.df['country'].unique()[:8]:
            subset = self.df[self.df['country'] == country]
            ax.hist(subset['regulatory_compliance_rate'], bins=30, alpha=0.5, label=country)
        ax.set_title('Compliance Rate Distribution by Country', fontsize=14)
        ax.set_xlabel('Compliance Rate (%)')
        ax.legend(fontsize=8)

        # Environmental investment trend
        ax = axes[1, 1]
        inv_trend = self.df.groupby(['year', 'country'])['environmental_investment_index'].mean().unstack()
        for col in inv_trend.columns[:6]:
            ax.plot(inv_trend.index, inv_trend[col], '-o', markersize=4, label=col)
        ax.set_title('Environmental Investment Trends', fontsize=14)
        ax.set_xlabel('Year')
        ax.set_ylabel('Investment Index')
        ax.legend(fontsize=8)

        plt.tight_layout()
        plt.savefig(self.charts_dir / '05_policy.png', dpi=150, bbox_inches='tight')
        plt.close()
        print("    Saved: 05_policy.png")

    def health_impact_analysis(self):
        """Analyze health impact of air pollution."""
        print("\n Analyzing health impacts...")
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Health Impact Assessment', fontsize=18, fontweight='bold')

        ax = axes[0, 0]
        sample = self.df.sample(min(5000, len(self.df)))
        scatter = ax.scatter(sample['AQI'], sample['respiratory_cases_per_100k'],
                             c=sample['regulation_stringency_score'], cmap='RdYlGn',
                             alpha=0.4, s=10)
        plt.colorbar(scatter, ax=ax, label='Regulation Score')
        ax.set_title('AQI vs Respiratory Cases', fontsize=14)
        ax.set_xlabel('AQI')
        ax.set_ylabel('Respiratory Cases per 100k')

        ax = axes[0, 1]
        health_cols = ['respiratory_cases_per_100k', 'cardiovascular_cases_per_100k',
                       'hospital_admissions_per_100k', 'premature_deaths_per_million']
        country_health = self.df.groupby('country')[health_cols].mean()
        country_health_norm = (country_health - country_health.min()) / (country_health.max() - country_health.min())
        sns.heatmap(country_health_norm, annot=True, fmt='.2f', cmap='YlOrRd',
                    ax=ax, linewidths=0.5)
        ax.set_title('Health Impact by Country (Normalized)', fontsize=14)

        ax = axes[1, 0]
        for cat in ['Good', 'Moderate', 'Unhealthy', 'Hazardous']:
            subset = self.df[self.df['AQI_category'] == cat]
            if len(subset) > 0:
                ax.hist(subset['hospital_admissions_per_100k'], bins=30,
                        alpha=0.5, label=cat)
        ax.set_title('Hospital Admissions by AQI Category', fontsize=14)
        ax.set_xlabel('Hospital Admissions per 100k')
        ax.legend()

        ax = axes[1, 1]
        yearly_health = self.df.groupby('year')[health_cols[:2]].mean()
        yearly_health.plot(ax=ax, linewidth=2, marker='o')
        ax.set_title('Health Trends Over Time', fontsize=14)
        ax.set_xlabel('Year')
        ax.set_ylabel('Cases per 100k')
        ax.legend(fontsize=8)

        plt.tight_layout()
        plt.savefig(self.charts_dir / '06_health.png', dpi=150, bbox_inches='tight')
        plt.close()
        print("    Saved: 06_health.png")

    def clustering_analysis(self):
        """Perform K-means clustering on city pollution profiles."""
        print("\n Performing clustering analysis...")
        features = ['PM2_5', 'PM10', 'NO2', 'SO2', 'CO', 'O3',
                     'industrial_output_index', 'regulation_stringency_score',
                     'green_energy_pct']
        city_profiles = self.df.groupby('city')[features].mean().dropna()
        scaler = StandardScaler()
        scaled = scaler.fit_transform(city_profiles)

        # K-means with 4 clusters
        kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
        city_profiles['cluster'] = kmeans.fit_predict(scaled)

        # PCA for visualization
        pca = PCA(n_components=2)
        coords = pca.fit_transform(scaled)
        city_profiles['PC1'] = coords[:, 0]
        city_profiles['PC2'] = coords[:, 1]

        fig, ax = plt.subplots(figsize=(14, 10))
        for cluster in range(4):
            mask = city_profiles['cluster'] == cluster
            ax.scatter(city_profiles.loc[mask, 'PC1'], city_profiles.loc[mask, 'PC2'],
                       s=100, alpha=0.7, label=f'Cluster {cluster}', c=COLORS[cluster])
            for city in city_profiles.loc[mask].index[:5]:
                ax.annotate(city, (city_profiles.loc[city, 'PC1'],
                                   city_profiles.loc[city, 'PC2']),
                            fontsize=7, alpha=0.8)

        ax.set_title('City Pollution Profile Clusters (PCA)', fontsize=16, fontweight='bold')
        ax.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]*100:.1f}% variance)')
        ax.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]*100:.1f}% variance)')
        ax.legend(title='Cluster', fontsize=11)
        plt.savefig(self.charts_dir / '07_clustering.png', dpi=150, bbox_inches='tight')
        plt.close()

        city_profiles.to_csv(self.reports_dir / 'city_clusters.csv')
        print("    Saved: 07_clustering.png, city_clusters.csv")

    def generate_summary_report(self):
        """Generate a text summary report."""
        print("\n Generating summary report...")
        report = []
        report.append("=" * 70)
        report.append(f"  {ProjectInfo.TITLE}")
        report.append(f"  Analysis Summary Report")
        report.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 70)

        report.append(f"\n1. DATASET OVERVIEW")
        report.append(f"   Total records analyzed: {len(self.df):,}")
        report.append(f"   Countries: {self.df['country'].nunique()}")
        report.append(f"   Cities: {self.df['city'].nunique()}")
        report.append(f"   Stations: {self.df['station_id'].nunique()}")
        report.append(f"   Date range: {self.df['date'].min()} to {self.df['date'].max()}")

        report.append(f"\n2. KEY FINDINGS")
        report.append(f"   Global average AQI: {self.df['AQI'].mean():.1f}")
        report.append(f"   Most polluted country: {self.df.groupby('country')['AQI'].mean().idxmax()}")
        report.append(f"   Cleanest country: {self.df.groupby('country')['AQI'].mean().idxmin()}")

        r, _ = stats.pearsonr(self.df['industrial_output_index'].dropna(),
                              self.df['AQI'].loc[self.df['industrial_output_index'].dropna().index])
        report.append(f"   Industrial output <-> AQI correlation: {r:.3f}")

        r2, _ = stats.pearsonr(self.df['regulation_stringency_score'].dropna(),
                               self.df['AQI'].loc[self.df['regulation_stringency_score'].dropna().index])
        report.append(f"   Regulation score <-> AQI correlation: {r2:.3f}")

        tax_mean = self.df.groupby('has_carbon_tax')['AQI'].mean()
        if len(tax_mean) == 2:
            reduction = ((tax_mean[0] - tax_mean[1]) / tax_mean[0]) * 100
            report.append(f"   Carbon tax AQI reduction: {reduction:.1f}%")

        report_text = '\n'.join(report)
        report_path = self.reports_dir / 'analysis_summary.txt'
        with open(report_path, 'w') as f:
            f.write(report_text)
        print(f"    Saved: {report_path}")
        print(report_text)

    def run_full_analysis(self):
        """Execute all analysis modules."""
        print(f"\n{'='*60}")
        print(f"  COMPREHENSIVE AIR QUALITY ANALYSIS")
        print(f"{'='*60}\n")

        self.load_data()
        self.descriptive_statistics()
        self.temporal_analysis()
        self.geographic_analysis()
        self.industrial_impact_analysis()
        self.policy_effectiveness()
        self.health_impact_analysis()
        self.clustering_analysis()
        self.generate_summary_report()

        print(f"\n{'='*60}")
        print(f"   ALL ANALYSES COMPLETE")
        print(f"   Charts: {self.charts_dir}")
        print(f"  Reports: {self.reports_dir}")
        print(f"{'='*60}\n")


def main():
    analyzer = AirQualityAnalyzer()
    analyzer.run_full_analysis()


if __name__ == '__main__':
    main()
