"""
Web Dashboard - Global Air Quality Dynamics Project
Serves analysis results, charts, and database data on localhost.
"""
import os, json, sqlite3, base64
from pathlib import Path
from flask import Flask, render_template_string, jsonify

from config import DatasetConfig, ProjectInfo

app = Flask(__name__)
BASE = Path(__file__).parent
DB_PATH = BASE / 'air_quality_results.db'
CHARTS_DIR = DatasetConfig.CHARTS_DIR
REPORTS_DIR = DatasetConfig.REPORTS_DIR

def query_db(sql):
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def img_b64(name):
    p = CHARTS_DIR / name
    if p.exists():
        return base64.b64encode(p.read_bytes()).decode()
    return ""

TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Global Air Quality Dynamics Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0a0e1a;--surface:#111827;--surface2:#1a2236;--border:#1e2d4a;--accent:#3b82f6;
--accent2:#8b5cf6;--green:#10b981;--red:#ef4444;--orange:#f59e0b;--text:#e2e8f0;--text2:#94a3b8}
body{font-family:'Inter',sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
.header{background:linear-gradient(135deg,#0f172a 0%,#1e1b4b 50%,#0f172a 100%);
border-bottom:1px solid var(--border);padding:24px 40px;position:sticky;top:0;z-index:100;
backdrop-filter:blur(20px)}
.header h1{font-size:24px;font-weight:700;background:linear-gradient(135deg,#60a5fa,#a78bfa,#34d399);
-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:4px}
.header p{color:var(--text2);font-size:13px}
.nav{display:flex;gap:8px;margin-top:16px;flex-wrap:wrap}
.nav a{padding:8px 18px;border-radius:8px;background:var(--surface);color:var(--text2);
text-decoration:none;font-size:13px;font-weight:500;border:1px solid var(--border);
transition:all .2s}
.nav a:hover,.nav a.active{background:var(--accent);color:#fff;border-color:var(--accent)}
.container{max-width:1400px;margin:0 auto;padding:24px 40px}
.grid{display:grid;gap:20px;margin-bottom:24px}
.grid-4{grid-template-columns:repeat(auto-fit,minmax(240px,1fr))}
.grid-2{grid-template-columns:repeat(auto-fit,minmax(500px,1fr))}
.grid-1{grid-template-columns:1fr}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;
padding:24px;transition:transform .2s,box-shadow .2s}
.card:hover{transform:translateY(-2px);box-shadow:0 8px 30px rgba(0,0,0,.3)}
.stat-card{text-align:center}
.stat-card .icon{font-size:28px;margin-bottom:8px}
.stat-card .value{font-size:32px;font-weight:800;background:linear-gradient(135deg,var(--accent),var(--accent2));
-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.stat-card .label{color:var(--text2);font-size:12px;font-weight:500;text-transform:uppercase;
letter-spacing:.5px;margin-top:4px}
.card h3{font-size:16px;font-weight:600;margin-bottom:16px;display:flex;align-items:center;gap:8px}
.card img{width:100%;border-radius:10px;border:1px solid var(--border)}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:10px 12px;background:var(--surface2);color:var(--text2);
font-weight:600;text-transform:uppercase;font-size:11px;letter-spacing:.5px;
border-bottom:2px solid var(--border)}
td{padding:10px 12px;border-bottom:1px solid var(--border)}
tr:hover td{background:rgba(59,130,246,.05)}
.badge{display:inline-block;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600}
.badge-green{background:rgba(16,185,129,.15);color:#34d399}
.badge-red{background:rgba(239,68,68,.15);color:#f87171}
.badge-blue{background:rgba(59,130,246,.15);color:#60a5fa}
.badge-orange{background:rgba(245,158,11,.15);color:#fbbf24}
.section-title{font-size:20px;font-weight:700;margin:32px 0 16px;padding-bottom:8px;
border-bottom:2px solid var(--border);display:flex;align-items:center;gap:10px}
.report-box{background:var(--surface2);border-radius:10px;padding:20px;font-family:monospace;
font-size:13px;line-height:1.8;white-space:pre-wrap;max-height:400px;overflow-y:auto;
border:1px solid var(--border)}
.bar{height:8px;border-radius:4px;background:var(--surface2);overflow:hidden;margin-top:6px}
.bar-fill{height:100%;border-radius:4px;transition:width .6s ease}
@media(max-width:768px){.container{padding:16px}.grid-2{grid-template-columns:1fr}
.header{padding:16px 20px}}
</style>
</head>
<body>
<div class="header">
<h1>🌍 Global Air Quality Dynamics & Industrial Policy</h1>
<p>Module: {{info.module}} | Big Data Analytics Dashboard | {{info.records}} records across {{info.countries}} countries</p>
<div class="nav">
<a href="#overview" class="active">Overview</a>
<a href="#charts">Charts & Analysis</a>
<a href="#countries">Countries</a>
<a href="#cities">Cities</a>
<a href="#sectors">Industry Sectors</a>
<a href="#tuning">Model Tuning</a>
<a href="#health">Health Impact</a>
<a href="#reports">Reports</a>
</div>
</div>

<div class="container">

<!-- OVERVIEW STATS -->
<div id="overview" class="section-title">📊 Dashboard Overview</div>
<div class="grid grid-4">
<div class="card stat-card"><div class="icon">📊</div>
<div class="value">{{info.records}}</div><div class="label">Total Records</div></div>
<div class="card stat-card"><div class="icon">🌍</div>
<div class="value">{{info.countries}}</div><div class="label">Countries</div></div>
<div class="card stat-card"><div class="icon">🏙️</div>
<div class="value">{{info.cities}}</div><div class="label">Cities</div></div>
<div class="card stat-card"><div class="icon">💾</div>
<div class="value">{{info.data_size}}</div><div class="label">Dataset Size</div></div>
</div>
<div class="grid grid-4">
<div class="card stat-card"><div class="icon">🔬</div>
<div class="value">{{info.avg_aqi}}</div><div class="label">Global Avg AQI</div></div>
<div class="card stat-card"><div class="icon">🏆</div>
<div class="value">{{info.best_model}}</div><div class="label">Best ML Model</div></div>
<div class="card stat-card"><div class="icon">🎯</div>
<div class="value">{{info.best_r2}}</div><div class="label">Best R² Score</div></div>
<div class="card stat-card"><div class="icon">🔥</div>
<div class="value">Spark 4.1</div><div class="label">Processing Engine</div></div>
</div>

<!-- CHARTS -->
<div id="charts" class="section-title">📈 Analysis & Visualizations</div>
<div class="grid grid-2">
{% for chart in charts %}
<div class="card"><h3>{{chart.title}}</h3>
<img src="data:image/png;base64,{{chart.data}}" alt="{{chart.title}}"></div>
{% endfor %}
</div>

<!-- COUNTRIES TABLE -->
<div id="countries" class="section-title">🌍 Country Air Quality Rankings</div>
<div class="card">
<table>
<tr><th>#</th><th>Country</th><th>Avg AQI</th><th>PM2.5</th><th>NO₂</th><th>SO₂</th>
<th>Regulation</th><th>Green Energy %</th><th>Records</th></tr>
{% for i, c in enumerate(country_stats) %}
<tr><td>{{i+1}}</td><td><strong>{{c.country}}</strong></td>
<td>{% if c.avg_aqi <= 50 %}<span class="badge badge-green">{{c.avg_aqi|round(1)}}</span>
{% elif c.avg_aqi <= 100 %}<span class="badge badge-orange">{{c.avg_aqi|round(1)}}</span>
{% else %}<span class="badge badge-red">{{c.avg_aqi|round(1)}}</span>{% endif %}</td>
<td>{{c.median_pm25|round(1)}}</td><td>{{c.avg_no2|round(1)}}</td><td>{{c.avg_so2|round(1)}}</td>
<td><div>{{c.regulation_score|round(0)|int}}/100</div>
<div class="bar"><div class="bar-fill" style="width:{{c.regulation_score}}%;background:
{% if c.regulation_score > 70 %}var(--green){% elif c.regulation_score > 50 %}var(--orange)
{% else %}var(--red){% endif %}"></div></div></td>
<td>{{c.avg_green_energy_pct|round(1)}}%</td>
<td>{{c.record_count|int}}</td></tr>
{% endfor %}
</table>
</div>

<!-- CITIES TABLE -->
<div id="cities" class="section-title">🏙️ Top 20 Most Polluted Cities</div>
<div class="card">
<table>
<tr><th>#</th><th>City</th><th>Country</th><th>Avg AQI</th><th>PM2.5</th><th>PM10</th>
<th>NO₂</th><th>Stations</th><th>Resp. Cases</th></tr>
{% for i, c in enumerate(city_stats[:20]) %}
<tr><td>{{i+1}}</td><td><strong>{{c.city}}</strong></td><td>{{c.country}}</td>
<td>{% if c.avg_aqi <= 50 %}<span class="badge badge-green">{{c.avg_aqi|round(1)}}</span>
{% elif c.avg_aqi <= 100 %}<span class="badge badge-orange">{{c.avg_aqi|round(1)}}</span>
{% else %}<span class="badge badge-red">{{c.avg_aqi|round(1)}}</span>{% endif %}</td>
<td>{{c.avg_pm25|round(1)}}</td><td>{{c.avg_pm10|round(1)}}</td><td>{{c.avg_no2|round(1)}}</td>
<td>{{c.station_count}}</td><td>{{c.avg_respiratory_cases|round(1)}}</td></tr>
{% endfor %}
</table>
</div>

<!-- SECTORS -->
<div id="sectors" class="section-title">🏭 Industry Sector Impact</div>
<div class="card">
<table>
<tr><th>Sector</th><th>Avg AQI</th><th>PM2.5</th><th>NO₂</th><th>SO₂</th>
<th>Industrial Output</th><th>Compliance %</th><th>Records</th></tr>
{% for s in sector_stats %}
<tr><td><strong>{{s.sector}}</strong></td>
<td>{% if s.avg_aqi <= 50 %}<span class="badge badge-green">{{s.avg_aqi|round(1)}}</span>
{% elif s.avg_aqi <= 100 %}<span class="badge badge-orange">{{s.avg_aqi|round(1)}}</span>
{% else %}<span class="badge badge-red">{{s.avg_aqi|round(1)}}</span>{% endif %}</td>
<td>{{s.avg_pm25|round(1)}}</td><td>{{s.avg_no2|round(1)}}</td><td>{{s.avg_so2|round(1)}}</td>
<td>{{s.avg_industrial_output|round(1)}}</td><td>{{s.avg_compliance_rate|round(1)}}%</td>
<td>{{s.record_count|int}}</td></tr>
{% endfor %}
</table>
</div>

<!-- MODEL TUNING -->
<div id="tuning" class="section-title">⚙️ Hyperparameter Tuning Results</div>
<div class="grid grid-2">
<div class="card">
<h3>🏆 Model Comparison</h3>
<table>
<tr><th>Model</th><th>R²</th><th>RMSE</th><th>MAE</th><th>CV Score</th><th>Time</th></tr>
{% for m in tuning_results %}
<tr><td><strong>{{m.model_name}}</strong></td>
<td><span class="badge badge-{% if m.test_r2 >= 0.99 %}green{% elif m.test_r2 >= 0.95 %}blue{% else %}orange{% endif %}">
{{m.test_r2}}</span></td>
<td>{{m.test_rmse}}</td><td>{{m.test_mae}}</td><td>{{m.cv_score}}</td>
<td>{{m.duration_seconds|round(1)}}s</td></tr>
{% endfor %}
</table>
</div>
<div class="card"><h3>📊 Tuning Visualization</h3>
<img src="data:image/png;base64,{{tuning_chart}}" alt="Model Tuning"></div>
</div>

<!-- HEALTH -->
<div id="health" class="section-title">🏥 Health Impact by AQI Bracket</div>
<div class="card">
<table>
<tr><th>AQI Bracket</th><th>Respiratory Cases/100k</th><th>Cardiovascular/100k</th>
<th>Hospital Admissions/100k</th><th>Premature Deaths/M</th><th>Records</th></tr>
{% for h in health_stats %}
<tr><td><strong>{{h.aqi_bracket}}</strong></td>
<td>{{h.avg_respiratory_cases|round(1)}}</td><td>{{h.avg_cardiovascular_cases|round(1)}}</td>
<td>{{h.avg_hospital_admissions|round(1)}}</td><td>{{h.avg_premature_deaths|round(2)}}</td>
<td>{{h.record_count|int}}</td></tr>
{% endfor %}
</table>
</div>

<!-- REPORTS -->
<div id="reports" class="section-title">📝 Reports</div>
<div class="grid grid-2">
<div class="card"><h3>📋 Analysis Summary</h3>
<div class="report-box">{{analysis_report}}</div></div>
<div class="card"><h3>⚙️ Model Tuning Report</h3>
<div class="report-box">{{tuning_report}}</div></div>
</div>

<!-- PROCESSING LOG -->
<div class="section-title">🔥 Spark Processing Log</div>
<div class="card">
<table>
<tr><th>Stage</th><th>Records</th><th>Duration</th><th>Output Table</th><th>Spark</th><th>Time</th></tr>
{% for l in proc_log %}
<tr><td><strong>{{l.stage}}</strong></td><td>{{l.records_processed}}</td>
<td>{{l.duration_seconds}}s</td><td><span class="badge badge-blue">{{l.output_table}}</span></td>
<td>{{l.spark_version}}</td><td>{{l.processed_at[:19]}}</td></tr>
{% endfor %}
</table>
</div>

<div style="text-align:center;padding:40px;color:var(--text2);font-size:12px">
<p>Global Air Quality Dynamics & Industrial Policy | Module {{info.module}} | 
Powered by PySpark, scikit-learn, XGBoost | Dashboard built with Flask</p>
</div>
</div>
</body>
</html>
"""

@app.route('/')
def dashboard():
    # Load DB data
    country_stats = query_db("SELECT * FROM country_aqi_stats ORDER BY avg_aqi DESC")
    city_stats = query_db("SELECT * FROM city_aqi_stats ORDER BY avg_aqi DESC")
    sector_stats = query_db("SELECT * FROM sector_impact ORDER BY avg_aqi DESC")
    health_stats = query_db("SELECT * FROM health_correlations ORDER BY aqi_bracket")
    proc_log = query_db("SELECT * FROM processing_log ORDER BY id")
    
    try:
        tuning_results = query_db("SELECT * FROM model_tuning_results ORDER BY test_r2 DESC")
    except:
        tuning_results = []

    # Charts
    chart_files = [
        ('01_overview.png', '📊 Air Quality Overview'),
        ('02_temporal.png', '📅 Temporal Trends'),
        ('03_geographic.png', '🌍 Geographic Comparison'),
        ('04_industrial.png', '🏭 Industrial Impact'),
        ('05_policy.png', '📜 Policy Effectiveness'),
        ('06_health.png', '🏥 Health Impact'),
        ('07_clustering.png', '🔬 City Clustering (PCA)'),
    ]
    charts = []
    for fname, title in chart_files:
        b = img_b64(fname)
        if b:
            charts.append({'title': title, 'data': b})

    tuning_chart = img_b64('08_model_tuning.png')

    # Reports
    analysis_report = ""
    rp = REPORTS_DIR / 'analysis_summary.txt'
    if rp.exists():
        analysis_report = rp.read_text()
    
    tuning_report = ""
    tp = REPORTS_DIR / 'model_tuning_report.txt'
    if tp.exists():
        tuning_report = tp.read_text()

    # Data file size
    data_file = DatasetConfig.DATA_DIR / 'air_quality_part_001.csv'
    data_size = f"{data_file.stat().st_size / (1024*1024):.1f} MB" if data_file.exists() else "N/A"

    # Info
    avg_aqi = round(country_stats[0]['avg_aqi'], 1) if country_stats else "N/A"
    global_avg = round(sum(c['avg_aqi'] * c['record_count'] for c in country_stats) / 
                       max(1, sum(c['record_count'] for c in country_stats)), 1) if country_stats else "N/A"
    total_records = sum(c['record_count'] for c in country_stats) if country_stats else 0
    
    best_model = tuning_results[0]['model_name'] if tuning_results else "N/A"
    best_r2 = tuning_results[0]['test_r2'] if tuning_results else "N/A"

    info = {
        'module': ProjectInfo.MODULE_CODE,
        'records': f"{total_records:,}",
        'countries': len(country_stats),
        'cities': len(city_stats),
        'data_size': data_size,
        'avg_aqi': global_avg,
        'best_model': best_model,
        'best_r2': best_r2,
    }

    return render_template_string(TEMPLATE,
        info=info, charts=charts, country_stats=country_stats,
        city_stats=city_stats, sector_stats=sector_stats,
        health_stats=health_stats, tuning_results=tuning_results,
        tuning_chart=tuning_chart, proc_log=proc_log,
        analysis_report=analysis_report, tuning_report=tuning_report,
        enumerate=enumerate)

if __name__ == '__main__':
    print("\n" + "="*60)
    print("  [GLOBE] Global Air Quality Dynamics Dashboard")
    print("  [LINK] http://localhost:5000")
    print("="*60 + "\n")
    app.run(host='0.0.0.0', port=5000, debug=False)
