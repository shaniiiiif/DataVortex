#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║          DATA VORTEX v26.2 – PROFESSIONAL EDITION (FULLY DEBUGGED)          ║
║                                                                              ║
║   🎨 AI-Grade Dashboards  |  🚀 Big Data  |  🧠 ML  |  📊 120+ Commands     ║
║                                                                              ║
║   👑 Creator : Shanif P                                                      ║
║   📧 Email   : shaniiiiif@gmail.com                                          ║
║   📍 Location: India                                                         ║
║   © 2026 Shanif P. All rights reserved.                                      ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

    v26.2 CHANGELOG (Bug Fixes):
    ✅ Added 4 missing methods: ask_offline, root_cause, export_pipeline, full_report
    ✅ Fixed plt.colormaps fallback for old matplotlib
    ✅ Fixed detect_features returning None (now returns [], [])
    ✅ Fixed KMeans n_init compatibility (sklearn < 1.2)
    ✅ Fixed filter() to actually update self.df in DuckDB mode
    ✅ Fixed head() to respect DuckDB full-data mode
    ✅ Fixed _load_dask duplicate try/except
    ✅ Added auto-feature guards in predict/rf_predict/gb_predict/cv
    ✅ Fixed arima float cast for forecast values
    ✅ Fixed NaN-safe sum in ultra_dashboard
    ✅ Fixed warnings scope (module-scoped filters)
    ✅ Fixed pivot_data mixed-type cast
    ✅ Removed unused imports (re, logging, time)
    ✅ Fixed run_pipeline/auto_pilot to disable confirm prompts
    ✅ Fixed DuckDB COPY path escaping
"""

import sys, os, gc, json, sqlite3, pickle, io, subprocess
import warnings

# ============================================================
# FIX #11: Module-scoped warning filters (was broken with-block)
# ============================================================
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning, module='matplotlib')

# ============================================================
# 🔧 CRITICAL: Patch DuckDB for Plotly 7/narwhals compatibility
# ============================================================
try:
    import duckdb
    if not hasattr(duckdb, 'DuckDBPyRelation'):
        class DuckDBPyRelation:
            """Stub class for narwhals/plotly 7 compatibility"""
            pass
        duckdb.DuckDBPyRelation = DuckDBPyRelation
except ImportError:
    pass

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from fpdf import FPDF

# ============================================================
# OPTIONAL LIBRARIES
# ============================================================
POLARS_AVAILABLE = False
POLARS_ERROR = None
try:
    import polars as pl
    try:
        _t = pl.DataFrame({'x': [1]})
        _ = _t.shape
        POLARS_AVAILABLE = True
    except Exception as e:
        POLARS_ERROR = f"Polars binary broken: {str(e)[:40]}"
except ImportError:
    POLARS_ERROR = "Polars not installed"

DUCKDB_AVAILABLE = False
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    pass

DASK_AVAILABLE = False
try:
    import dask.dataframe as dd
    DASK_AVAILABLE = True
except ImportError:
    pass

PYARROW_AVAILABLE = False
try:
    import pyarrow
    PYARROW_AVAILABLE = True
except ImportError:
    pass

PSUTIL_AVAILABLE = False
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    pass

OPENPYXL_AVAILABLE = False
try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    pass

SKLEARN_AVAILABLE = False
SKLEARN_VERSION = "0"
try:
    import sklearn
    SKLEARN_VERSION = sklearn.__version__
    from sklearn.linear_model import LinearRegression
    from sklearn.cluster import KMeans
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split, cross_val_score
    from sklearn.decomposition import PCA
    from sklearn.metrics import r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    pass

SCIPY_AVAILABLE = False
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    pass

STATSMODELS_AVAILABLE = False
try:
    from statsmodels.tsa.arima.model import ARIMA
    STATSMODELS_AVAILABLE = True
except ImportError:
    pass

RAPIDFUZZ_AVAILABLE = False
try:
    from rapidfuzz import process, fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    pass

SQLALCHEMY_AVAILABLE = False
try:
    from sqlalchemy import create_engine
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    pass

SCHEDULER_AVAILABLE = False
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    SCHEDULER_AVAILABLE = True
except ImportError:
    pass

WEASYPRINT_AVAILABLE = False
try:
    from weasyprint import HTML as WeasyHTML
    WEASYPRINT_AVAILABLE = True
except ImportError:
    pass

# ============================================================
# CONSTANTS
# ============================================================
VERSION = "26.2"
CREATOR = "Shanif P"
CREATOR_EMAIL = "shaniiiiif@gmail.com"
CREATOR_LOCATION = "India"
CREATOR_DOB = "07/01/2006"
CREATOR_ENROLMENT = "2003/70280/01366"
CREATOR_PASSWORD = os.environ.get("DV_CREATOR_PW", "shanifkichu@123")
COPYRIGHT = "© 2026 Shanif P. All rights reserved."

SMALL_DATA = 200 * 1024 * 1024
MEDIUM_DATA = 2 * 1024 * 1024 * 1024
LARGE_DATA = 20 * 1024 * 1024 * 1024

scheduler = None
if SCHEDULER_AVAILABLE:
    try:
        scheduler = BackgroundScheduler(timezone='UTC')
    except Exception:
        scheduler = None


class DataVortex:
    def __init__(self):
        self.df = None
        self._polars_df = None
        self._dask_df = None
        self.duck_con = None
        self.duck_table = None
        self._sqlite_con = None
        self._sqlite_df_id = None
        self.data_name = ""
        self.file_size = 0
        self.data_engine = "none"
        self.is_preview = False
        self.total_rows = 0
        self.chunk_size = 100000
        self.insights = []
        self.deep_report_lines = []
        self.safe_mode = True
        self.standard_cols = [
            "product", "product name", "item", "sku", "category",
            "quantity", "qty", "units", "price", "unit price",
            "date", "sale date", "order date", "customer", "customer name",
            "region", "revenue", "sales", "amount", "total", "discount",
            "cost", "profit"
        ]
        try:
            self.backup_dir = Path("backups")
            self.backup_dir.mkdir(exist_ok=True)
        except Exception:
            self.backup_dir = Path.home() / ".datavortex_backups"
            self.backup_dir.mkdir(exist_ok=True, parents=True)

    # ============================================================
    # HELPERS
    # ============================================================
    @staticmethod
    def _escape_sql_path(p):
        return str(p).replace("'", "''").replace("\\", "/")

    def _invalidate_sqlite_cache(self):
        self._sqlite_con = None
        self._sqlite_df_id = None

    def print_header(self):
        print("\n" + "═" * 78)
        print("  ╔══════════════════════════════════════════════════════════════════╗")
        print("  ║       DATA VORTEX v26.2 – PROFESSIONAL EDITION                   ║")
        print("  ║       🎨 Dashboards  🚀 Big Data  🧠 ML  📊 120+ Commands        ║")
        print("  ╚══════════════════════════════════════════════════════════════════╝")
        print("═" * 78)
        print(f"  👑 Creator : {CREATOR}")
        print(f"  📧 Email   : {CREATOR_EMAIL}")
        print(f"  📍 Location: {CREATOR_LOCATION}")
        print(f"  {COPYRIGHT}")
        print("═" * 78)
        print("  ⚙️  Engine Status:")
        print(f"     Pandas    : ✅")
        print(f"     Polars    : {'✅ Native' if POLARS_AVAILABLE else f'❌ {POLARS_ERROR}'}")
        print(f"     DuckDB    : {'✅' if DUCKDB_AVAILABLE else '❌'}")
        print(f"     Dask      : {'✅' if DASK_AVAILABLE else '❌'}")
        print(f"     PyArrow   : {'✅' if PYARROW_AVAILABLE else '❌'}")
        print(f"     psutil    : {'✅' if PSUTIL_AVAILABLE else '❌'}")
        print(f"     sklearn   : {'✅ ' + SKLEARN_VERSION if SKLEARN_AVAILABLE else '❌'}")
        print(f"     scipy     : {'✅' if SCIPY_AVAILABLE else '❌'}")
        print(f"     WeasyPrint: {'✅' if WEASYPRINT_AVAILABLE else '❌'}")
        print("═" * 78)

    def help(self):
        print("""
📊 DataVortex v26.2 – ALL COMMANDS

📥 BIG DATA:
  load_smart <file>          Auto-engine by size
  load <file>                Generic load
  load_pandas|polars|duckdb|dask|chunk <file>
  load_db <type> <db> ...    MySQL/Postgres/SQLite
  load_url <url>             CSV from URL
  nasa <apod|neo|mars>       NASA data
  save_parquet <file>        Save as Parquet
  query_duckdb <SQL>         SQL on full data
  optimize_storage <file>    CSV → Parquet

🛡️ SAFETY:
  safe_mode_on | safe_mode_off
  backup | rollback | backups_list | clean_backups
  integrity_check | quality_check | memory_check
  auto_type_fix | audit_log

🧠 NO-API KILLERS:
  ask_offline <q>            Offline Q&A
  root_cause <col> [group]   Root cause analysis
  export_pipeline [file]     Export standalone script
  full_report [col] [steps]  One-shot comprehensive report

📥 BASIC:
  show_columns | head [n] | tail [n] | info | describe | unique <col>

🧹 CLEANING:
  clean | smart_clean | fix_columns | dropna | fillna <v>
  filter <col> <op> <v>

🔄 TRANSFORM:
  pivot | melt | merge | groupby

📊 VIZ:
  plot <type> <x> [y] | plotly <type> <x> [y]
  heatmap | bi

🎨 DASHBOARDS:
  ultra_dashboard [file]     - AI-grade interactive dashboard (HTML+PDF)
  mini_dashboard [file]      - Quick PNG dashboard

🤖 ML:
  predict | rf_predict | gb_predict <target> <features>
  cluster | pca | cv | detect_features

📈 TIME SERIES:
  forecast | smooth | anomaly | arima | decompose

🔬 STATS:
  ttest | anova | chisq

🗄️ SQL:
  sql <query> | raw_sql <type> <db> <query>

📄 REPORTS:
  save <fmt> <file>          csv/excel/json/parquet
  report | auto_report | power_report | sales_report
  deep_analyze | deep_report
  ai_summary                 Mini AI English report
  enterprise_report <file>   PDF report

⚙️ PIPELINE:
  auto_pilot | run_pipeline <file> [report]
  schedule | start_scheduler | stop_scheduler | jupyter

🧠 MEMORY:
  optimize_dtypes | free_memory | sample_data [n]

🔧 OTHER:
  about | creator | clear | exit
""")

    def about(self):
        print(f"\n{'═' * 60}")
        print(f"  👑 DATA VORTEX CREATOR")
        print(f"{'═' * 60}")
        print(f"  Name     : {CREATOR}")
        print(f"  Email    : {CREATOR_EMAIL}")
        print(f"  Location : {CREATOR_LOCATION}")
        print(f"  DOB      : {CREATOR_DOB}")
        print(f"  Enrolment: {CREATOR_ENROLMENT}")
        print(f"  Version  : {VERSION}")
        print(f"  {COPYRIGHT}")
        print(f"{'═' * 60}")

    # ============================================================
    # SAFETY
    # ============================================================
    def confirm(self, msg="Are you sure?"):
        if not self.safe_mode:
            return True
        try:
            return input(f"⚠️  {msg} (yes/no): ").strip().lower() in ('yes', 'y')
        except Exception:
            return False

    def auto_backup(self):
        if self.df is None:
            return
        try:
            fname = self.backup_dir / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.pkl"
            with open(fname, 'wb') as f:
                pickle.dump({'df': self.df, 'data_name': self.data_name}, f)
            for old in sorted(self.backup_dir.glob("*.pkl"))[:-10]:
                try:
                    old.unlink()
                except Exception:
                    pass
            print(f"💾 Backup: {fname.name}")
        except Exception as e:
            print(f"⚠️  {e}")

    def rollback(self, name=None):
        backups = sorted(self.backup_dir.glob("*.pkl"))
        if not backups:
            print("❌ No backups")
            return
        target = self.backup_dir / name if name else backups[-1]
        if not target.exists():
            print(f"❌ Not found")
            return
        try:
            with open(target, 'rb') as f:
                d = pickle.load(f)
            self.df = d['df']
            self.data_name = d.get('data_name', 'restored')
            self.total_rows = len(self.df)
            self.is_preview = False
            self._invalidate_sqlite_cache()
            print(f"↩️  Rolled back: {target.name}")
        except Exception as e:
            print(f"❌ {e}")

    def backups_list(self):
        bs = sorted(self.backup_dir.glob("*.pkl"))
        if not bs:
            print("📁 No backups")
            return
        print(f"\n💾 Backups ({len(bs)}):")
        for i, b in enumerate(bs, 1):
            print(f"  {i}. {b.name} ({b.stat().st_size / 1024:.1f} KB)")

    def clean_backups(self):
        if not self.confirm("Delete ALL backups?"):
            return
        n = 0
        for b in self.backup_dir.glob("*.pkl"):
            try:
                b.unlink()
                n += 1
            except Exception:
                pass
        print(f"🗑️  {n} removed")

    def memory_check(self):
        df_mb = 0.0
        if self.df is not None and len(self.df) > 0:
            try:
                df_mb = self.df.memory_usage(deep=True).sum() / (1024 ** 2)
            except Exception:
                pass
        print(f"\n📊 Memory:")
        print(f"  Preview DF    : {df_mb:.2f} MB")
        if self.total_rows:
            print(f"  Total rows    : {self.total_rows:,}")
        print(f"  Engine        : {self.data_engine.upper()}")
        if PSUTIL_AVAILABLE:
            try:
                m = psutil.virtual_memory()
                print(f"  System RAM    : {m.used / 1024**3:.2f} / {m.total / 1024**3:.2f} GB ({m.percent:.1f}%)")
                print(f"  Available     : {m.available / 1024**3:.2f} GB")
            except Exception:
                pass

    def optimize_dtypes(self):
        if self.df is None or len(self.df) == 0:
            print("❌ No data")
            return
        try:
            before = self.df.memory_usage(deep=True).sum() / (1024 ** 2)
            for c in self.df.columns:
                try:
                    t = self.df[c].dtype
                    if t == 'int64':
                        self.df[c] = pd.to_numeric(self.df[c], downcast='integer')
                    elif t == 'float64':
                        self.df[c] = pd.to_numeric(self.df[c], downcast='float')
                    elif t == 'object':
                        n, tot = self.df[c].nunique(), len(self.df)
                        if tot > 0 and (n / tot) < 0.5:
                            self.df[c] = self.df[c].astype('category')
                except Exception:
                    pass
            after = self.df.memory_usage(deep=True).sum() / (1024 ** 2)
            self._invalidate_sqlite_cache()
            print(f"✅ Optimized: {before:.2f} MB → {after:.2f} MB")
        except Exception as e:
            print(f"❌ {e}")

    def free_memory(self):
        if self._dask_df is not None:
            del self._dask_df
            self._dask_df = None
        self._invalidate_sqlite_cache()
        gc.collect()
        print("✅ GC done")

    def sample_data(self, n=100000):
        if self.df is None or len(self.df) == 0:
            print("❌ No data")
            return
        if len(self.df) <= n:
            print(f"ℹ️  Already ≤ {n}")
            return
        self.df = self.df.sample(n=n, random_state=42).reset_index(drop=True)
        self._invalidate_sqlite_cache()
        print(f"✅ Sampled {n:,} rows")

    def integrity_check(self):
        if self.df is None:
            print("❌ No data")
            return False
        issues = []
        for c in self.df.columns:
            if self.df[c].isnull().all():
                issues.append(f"'{c}' empty")
        if self.df.columns.duplicated().any():
            issues.append("Duplicate column names")
        if len(self.df) == 0:
            issues.append("Empty DataFrame")
        if issues:
            print("🚨 Issues:")
            for i in issues:
                print(f"   - {i}")
            return False
        print("✅ Integrity OK")
        return True

    def quality_check(self):
        if self.df is None:
            print("❌ No data")
            return False
        issues = []
        for c in self.df.columns:
            mp = self.df[c].isnull().mean() * 100
            if mp > 50:
                issues.append(f"'{c}' {mp:.1f}% missing")
        dup = self.df.duplicated().sum()
        if dup > 0:
            issues.append(f"{dup:,} duplicates")
        if issues:
            print("⚠️  Quality:")
            for i in issues:
                print(f"   - {i}")
            return False
        print("✅ Quality OK")
        return True

    def auto_type_fix(self):
        if self.df is None:
            print("❌ No data")
            return
        self.auto_backup()
        n = 0
        for c in self.df.columns:
            if self.df[c].dtype == 'object':
                try:
                    conv = pd.to_datetime(self.df[c], errors='coerce')
                    if conv.notna().mean() > 0.8:
                        self.df[c] = conv
                        print(f"📅 '{c}' → datetime")
                        n += 1
                        continue
                except Exception:
                    pass
                try:
                    cl = self.df[c].astype(str).str.replace(r'[^0-9.\-]', '', regex=True)
                    conv = pd.to_numeric(cl, errors='coerce')
                    if conv.notna().mean() > 0.8:
                        self.df[c] = conv
                        print(f"🔢 '{c}' → numeric")
                        n += 1
                except Exception:
                    pass
        self._invalidate_sqlite_cache()
        print(f"✅ Auto type fix: {n}")

    def audit_log(self, action, details=""):
        try:
            with open("audit.log", 'a') as f:
                f.write(f"{datetime.now()} | {action} | {details}\n")
        except Exception:
            pass

    # ============================================================
    # ENGINE MANAGEMENT
    # ============================================================
    def _choose_engine(self, size):
        if size < SMALL_DATA:
            return "pandas"
        elif size < MEDIUM_DATA and POLARS_AVAILABLE:
            return "polars"
        elif size < LARGE_DATA and DUCKDB_AVAILABLE:
            return "duckdb"
        elif DASK_AVAILABLE:
            return "dask"
        elif DUCKDB_AVAILABLE:
            return "duckdb"
        elif POLARS_AVAILABLE:
            return "polars"
        return "pandas_chunk"

    def _clear_engine(self):
        try:
            if self.duck_con:
                self.duck_con.close()
        except Exception:
            pass
        self.duck_con = None
        self.duck_table = None
        self._polars_df = None
        self._dask_df = None
        self._invalidate_sqlite_cache()
        gc.collect()

    def _has_duckdb(self):
        return self.duck_con is not None and self.duck_table is not None

    def _has_polars(self):
        return self._polars_df is not None

    def _duck_query(self, sql):
        sql = sql.strip().rstrip(';')
        if not sql:
            return None
        try:
            return self.duck_con.execute(sql).fetchdf()
        except Exception as e:
            print(f"❌ DuckDB: {e}")
            return None

    def _safe_col(self, col):
        return str(col).replace('"', '""')

    # ============================================================
    # LOADING
    # ============================================================
    def load_smart(self, filepath):
        path = Path(filepath)
        if not path.exists() or not path.is_file():
            print(f"❌ File not found: {filepath}")
            return
        size = path.stat().st_size
        print(f"\n📁 File    : {path.name}")
        print(f"📏 Size    : {size / (1024**2):.2f} MB ({size / (1024**3):.3f} GB)")
        print(f"📄 Format  : {path.suffix.lower()}")

        engine = self._choose_engine(size)
        print(f"🎯 Engine  : {engine.upper()}")
        print("─" * 60)

        self.file_size = size
        self.is_preview = False
        self.total_rows = 0
        self._clear_engine()

        if engine == "pandas":
            actual = self._load_pandas(filepath)
        elif engine == "polars":
            actual = self._load_polars(filepath)
        elif engine == "duckdb":
            actual = self._load_duckdb(filepath)
        elif engine == "dask":
            actual = self._load_dask(filepath)
        else:
            actual = self._load_chunk(filepath)

        self.data_engine = actual or "unknown"

        if self.df is not None:
            print(f"\n✅ Loaded")
            print(f"   Preview : {len(self.df):,} rows × {len(self.df.columns)} cols")
            if self.total_rows:
                print(f"   Total   : {self.total_rows:,} rows")
            print(f"   Engine  : {self.data_engine.upper()}")
            if self.is_preview:
                print(f"   ⚠️  Preview mode – full analysis via {self.data_engine}")
            self.memory_check()

    def _load_pandas(self, filepath):
        suffix = Path(filepath).suffix.lower()
        try:
            print("⚡ Pandas load...")
            if suffix == '.csv':
                self.df = pd.read_csv(filepath)
            elif suffix == '.parquet':
                if not PYARROW_AVAILABLE:
                    print("❌ pyarrow required")
                    return None
                self.df = pd.read_parquet(filepath)
            elif suffix == '.xlsx':
                if not OPENPYXL_AVAILABLE:
                    print("❌ openpyxl required")
                    return None
                self.df = pd.read_excel(filepath, engine='openpyxl')
            elif suffix == '.json':
                self.df = pd.read_json(filepath)
            elif suffix in ('.db', '.sqlite'):
                conn = sqlite3.connect(filepath)
                t = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
                if t.empty:
                    print("❌ No tables")
                    conn.close()
                    return None
                self.df = pd.read_sql_query(f'SELECT * FROM "{t.iloc[0]["name"]}"', conn)
                conn.close()
            else:
                print(f"❌ Unsupported: {suffix}")
                return None
            self.data_name = Path(filepath).stem
            self.total_rows = len(self.df)
            self.is_preview = False
            self._invalidate_sqlite_cache()
            print(f"✅ Pandas: {len(self.df):,} rows")
            return "pandas"
        except Exception as e:
            print(f"❌ Pandas: {e}")
            return None

    def _load_polars(self, filepath):
        if not POLARS_AVAILABLE:
            if DUCKDB_AVAILABLE:
                return self._load_duckdb(filepath)
            return self._load_pandas(filepath)
        suffix = Path(filepath).suffix.lower()
        try:
            print("⚡ Polars load...")
            if suffix == '.csv':
                self._polars_df = pl.read_csv(filepath)
            elif suffix == '.parquet':
                self._polars_df = pl.read_parquet(filepath)
            else:
                return self._load_pandas(filepath)
            self.total_rows = self._polars_df.height
            self.df = self._polars_df.head(10000).to_pandas()
            self.data_name = Path(filepath).stem
            self.is_preview = True
            self._invalidate_sqlite_cache()
            print(f"✅ Polars: {self.total_rows:,} rows")
            return "polars"
        except Exception as e:
            print(f"❌ Polars: {e}")
            if DUCKDB_AVAILABLE:
                return self._load_duckdb(filepath)
            return self._load_pandas(filepath)

    def _load_duckdb(self, filepath):
        if not DUCKDB_AVAILABLE:
            return self._load_pandas(filepath)
        suffix = Path(filepath).suffix.lower()
        safe = self._escape_sql_path(filepath)
        try:
            print("⚡ DuckDB load...")
            self.duck_con = duckdb.connect(':memory:')
            if suffix == '.csv':
                self.duck_con.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{safe}')")
            elif suffix == '.parquet':
                self.duck_con.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{safe}')")
            elif suffix in ('.db', '.sqlite'):
                conn = sqlite3.connect(filepath)
                t = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
                conn.close()
                if t.empty:
                    print("❌ No tables")
                    return None
                tbl = t.iloc[0]['name']
                df_temp = pd.read_sql_query(f'SELECT * FROM "{tbl}"', sqlite3.connect(filepath))
                self.duck_con.register('tmp', df_temp)
                self.duck_con.execute("CREATE TABLE data AS SELECT * FROM tmp")
                self.duck_con.unregister('tmp')
            else:
                return self._load_pandas(filepath)
            self.duck_table = "data"
            self.total_rows = self.duck_con.execute("SELECT COUNT(*) FROM data").fetchone()[0]
            self.df = self.duck_con.execute("SELECT * FROM data LIMIT 10000").fetchdf()
            self.data_name = Path(filepath).stem
            self.is_preview = True
            self._invalidate_sqlite_cache()
            print(f"✅ DuckDB: {self.total_rows:,} rows")
            return "duckdb"
        except Exception as e:
            print(f"❌ DuckDB: {e}")
            if self.duck_con:
                try:
                    self.duck_con.close()
                except Exception:
                    pass
                self.duck_con = None
            return self._load_pandas(filepath)

    def _load_dask(self, filepath):
        """FIX #7: Removed duplicate try/except"""
        if not DASK_AVAILABLE:
            return self._load_duckdb(filepath)
        suffix = Path(filepath).suffix.lower()
        try:
            print("⚡ Dask load...")
            if suffix == '.csv':
                self._dask_df = dd.read_csv(filepath, blocksize='128MB')
            elif suffix == '.parquet':
                self._dask_df = dd.read_parquet(filepath)
            else:
                return self._load_duckdb(filepath)
            try:
                self.df = self._dask_df.head(10000)
            except Exception as e:
                print(f"⚠️  Dask head: {e}")
                self.df = self._dask_df.compute().head(10000)
            try:
                self.total_rows = int(self._dask_df.shape[0].compute())
            except Exception:
                self.total_rows = 0
            self.data_name = Path(filepath).stem
            self.is_preview = True
            self._invalidate_sqlite_cache()
            print(f"✅ Dask: {self.total_rows:,} rows")
            return "dask"
        except Exception as e:
            print(f"❌ Dask: {e}")
            return self._load_duckdb(filepath)

    def _load_chunk(self, filepath):
        if not str(filepath).lower().endswith('.csv'):
            return self._load_pandas(filepath)
        try:
            print("⏳ Chunked read...")
            chunks = []
            total = 0
            for chunk in pd.read_csv(filepath, chunksize=self.chunk_size):
                chunks.append(chunk)
                total += len(chunk)
            self.df = pd.concat(chunks, ignore_index=True)
            self.data_name = Path(filepath).stem
            self.total_rows = total
            self.is_preview = False
            self._invalidate_sqlite_cache()
            print(f"✅ Chunk: {total:,} rows")
            return "pandas_chunk"
        except Exception as e:
            print(f"❌ {e}")
            return None

    def load(self, filepath):
        self.load_smart(filepath)

    def load_fast(self, filepath):
        self._load_polars(filepath)
        self.data_engine = "polars"

    def load_chunk(self, filepath):
        self._load_chunk(filepath)
        self.data_engine = "pandas_chunk"

    def load_url(self, url):
        try:
            import requests
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                self.df = pd.read_csv(io.StringIO(r.text))
                self.data_name = url.split('/')[-1]
                self.total_rows = len(self.df)
                self.data_engine = "pandas"
                self._invalidate_sqlite_cache()
                print(f"✅ URL: {len(self.df):,} rows")
        except Exception as e:
            print(f"❌ {e}")

    def nasa(self, dataset):
        try:
            import requests
            urls = {
                "apod": "https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY&count=10",
                "neo": "https://api.nasa.gov/neo/rest/v1/neo/browse?api_key=DEMO_KEY",
                "mars": "https://api.nasa.gov/insight_weather/?api_key=DEMO_KEY&feedtype=json&ver=1.0"
            }
            if dataset not in urls:
                print("❌ apod/neo/mars")
                return
            d = requests.get(urls[dataset], timeout=15).json()
            if dataset == "apod":
                self.df = pd.DataFrame(d)
            elif dataset == "neo":
                self.df = pd.DataFrame(d['near_earth_objects'])
            else:
                self.df = pd.DataFrame([d])
            self.data_name = f"NASA_{dataset}"
            self.total_rows = len(self.df)
            self.data_engine = "pandas"
            self._invalidate_sqlite_cache()
            print(f"✅ NASA {dataset}: {len(self.df)} rows")
        except Exception as e:
            print(f"❌ {e}")

    def load_db(self, db_type, db_name, user=None, password=None,
                host="localhost", port=None, query="SELECT * FROM my_table"):
        if not SQLALCHEMY_AVAILABLE:
            print("❌ SQLAlchemy not installed")
            return
        try:
            if db_type == "mysql":
                eng = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port or 3306}/{db_name}")
            elif db_type == "postgresql":
                eng = create_engine(f"postgresql+pg8000://{user}:{password}@{host}:{port or 5432}/{db_name}")
            elif db_type == "sqlite":
                eng = create_engine(f"sqlite:///{db_name}")
            else:
                print("❌ Unsupported DB")
                return
            self.df = pd.read_sql(query, eng)
            self.data_name = db_name
            self.total_rows = len(self.df)
            self.data_engine = "pandas"
            self._invalidate_sqlite_cache()
            print(f"✅ DB: {len(self.df):,} rows")
        except Exception as e:
            print(f"❌ {e}")

    def session_save(self, fname="session.pkl"):
        if self.df is None:
            print("❌ No data")
            return
        try:
            with open(fname, 'wb') as f:
                pickle.dump({'df': self.df, 'data_name': self.data_name}, f)
            print(f"✅ Saved: {fname}")
        except Exception as e:
            print(f"❌ {e}")

    def session_load(self, fname="session.pkl"):
        if not os.path.exists(fname):
            print(f"❌ Not found")
            return
        try:
            with open(fname, 'rb') as f:
                d = pickle.load(f)
            self.df = d['df']
            self.data_name = d.get('data_name', 'session')
            self.total_rows = len(self.df)
            self._invalidate_sqlite_cache()
            print(f"✅ Loaded: {fname}")
        except Exception as e:
            print(f"❌ {e}")

    def save_parquet(self, filename="data.parquet"):
        if self.df is None:
            print("❌ No data")
            return
        if not PYARROW_AVAILABLE:
            print("❌ pyarrow not installed")
            return
        try:
            if self._has_duckdb():
                safe_out = self._escape_sql_path(filename)
                self.duck_con.execute(f"COPY {self.duck_table} TO '{safe_out}' (FORMAT PARQUET)")
                print(f"✅ Parquet: {filename}")
                return
            self.df.to_parquet(filename, compression='snappy', index=False)
            print(f"✅ Parquet: {filename}")
        except Exception as e:
            print(f"❌ {e}")

    def optimize_storage(self, filepath):
        if not str(filepath).endswith('.csv'):
            print("❌ CSV only")
            return
        if not PYARROW_AVAILABLE:
            print("❌ pyarrow needed")
            return
        try:
            out = str(filepath).replace('.csv', '.parquet')
            pd.read_csv(filepath).to_parquet(out, compression='snappy', index=False)
            print(f"✅ Optimized: {out}")
        except Exception as e:
            print(f"❌ {e}")

    # ============================================================
    # VIEW
    # ============================================================
    def show_columns(self):
        if self.df is None:
            print("❌ No data")
            return
        if self._has_duckdb():
            try:
                s = self.duck_con.execute(f"DESCRIBE {self.duck_table}").fetchdf()
                print(f"\n📋 Columns ({len(s)}):")
                for i, r in s.iterrows():
                    print(f"  {i + 1}. {r['column_name']} ({r['column_type']})")
                return
            except Exception:
                pass
        print(f"\n📋 Columns ({len(self.df.columns)}):")
        for i, c in enumerate(self.df.columns, 1):
            print(f"  {i}. {c} ({self.df[c].dtype})")

    def head(self, n=5):
        """FIX #6: DuckDB mode uses full data"""
        if self.df is None:
            print("❌ No data")
            return
        if self._has_duckdb():
            try:
                r = self._duck_query(f"SELECT * FROM {self.duck_table} LIMIT {n}")
                if r is not None:
                    print(r.to_string())
                    return
            except Exception:
                pass
        print(self.df.head(n).to_string())

    def tail(self, n=5):
        if self.df is None:
            print("❌ No data")
            return
        if self._has_duckdb():
            try:
                r = self._duck_query(f"SELECT * FROM {self.duck_table} ORDER BY 1 DESC LIMIT {n}")
                if r is not None:
                    print(r.iloc[::-1].to_string())
                    return
            except Exception:
                pass
        print(self.df.tail(n).to_string())

    def info(self):
        if self.df is None:
            print("❌ No data")
            return
        print(f"\n📊 Data Info:")
        print(f"  Name     : {self.data_name}")
        print(f"  Engine   : {self.data_engine.upper()}")
        print(f"  Size     : {self.file_size / (1024 ** 2):.2f} MB")
        if self.total_rows:
            print(f"  Total    : {self.total_rows:,}")
        print(f"  Preview  : {len(self.df):,}")

    def describe(self):
        if self.df is None:
            print("❌ No data")
            return
        if self._has_duckdb():
            try:
                print("⚡ Full-data stats (DuckDB):")
                s = self.duck_con.execute(f"DESCRIBE {self.duck_table}").fetchdf()
                num_types = ['BIGINT', 'INTEGER', 'DOUBLE', 'FLOAT', 'DECIMAL',
                             'SMALLINT', 'TINYINT', 'HUGEINT', 'REAL', 'NUMERIC']
                nc = [r['column_name'] for _, r in s.iterrows()
                      if any(t in str(r['column_type']).upper() for t in num_types)]
                if nc:
                    sels = []
                    for c in nc:
                        safe = self._safe_col(c)
                        sels += [f'COUNT("{safe}") AS "{c}_n"',
                                 f'AVG("{safe}") AS "{c}_m"',
                                 f'MIN("{safe}") AS "{c}_mn"',
                                 f'MAX("{safe}") AS "{c}_mx"',
                                 f'STDDEV("{safe}") AS "{c}_s"']
                    r = self._duck_query(f"SELECT {', '.join(sels)} FROM {self.duck_table}")
                    if r is not None:
                        d = {}
                        for c in nc:
                            d[c] = {'count': r[f"{c}_n"].iloc[0],
                                    'mean': r[f"{c}_m"].iloc[0],
                                    'std': r[f"{c}_s"].iloc[0],
                                    'min': r[f"{c}_mn"].iloc[0],
                                    'max': r[f"{c}_mx"].iloc[0]}
                        print(pd.DataFrame(d).to_string())
                        return
            except Exception as e:
                print(f"⚠️  {e}")
        print("📊 Stats (preview):")
        print(self.df.describe().to_string())

    def unique(self, col):
        if self.df is None or col not in self.df.columns:
            print(f"❌ '{col}' not found")
            return
        if self._has_duckdb():
            try:
                safe = self._safe_col(col)
                r = self._duck_query(f'SELECT DISTINCT "{safe}" FROM {self.duck_table} LIMIT 1000')
                if r is not None:
                    print(r.iloc[:, 0].tolist())
                    return
            except Exception:
                pass
        print(self.df[col].unique())

    # ============================================================
    # CLEANING
    # ============================================================
    def clean(self):
        if self.df is None or not self.confirm("Clean data?"):
            return
        self.auto_backup()
        before = len(self.df)
        self.df = self.df.drop_duplicates()
        for c in self.df.columns:
            if self.df[c].dtype in ['int64', 'float64']:
                self.df[c] = self.df[c].fillna(self.df[c].median())
            else:
                m = self.df[c].mode()
                self.df[c] = self.df[c].fillna(m[0] if not m.empty else 'Unknown')
        self._invalidate_sqlite_cache()
        print(f"✅ Cleaned: {before:,} → {len(self.df):,}")

    def _std_cols(self, df):
        if not RAPIDFUZZ_AVAILABLE:
            return df, []
        changes = []
        new = {}
        for c in df.columns:
            cl = c.strip().lower()
            m = process.extractOne(cl, self.standard_cols, scorer=fuzz.ratio)
            if m and m[1] >= 85 and m[0] != cl:
                new[c] = m[0].title()
                changes.append(f"'{c}' → '{m[0].title()}'")
            else:
                new[c] = c
        if new:
            df = df.rename(columns=new)
        return df, changes

    def fix_columns(self):
        if self.df is None:
            print("❌ No data")
            return
        df, changes = self._std_cols(self.df)
        if changes:
            self.df = df
            self._invalidate_sqlite_cache()
            print("🔧 Fixed:")
            for c in changes:
                print(f"   {c}")
        else:
            print("✅ OK")

    def smart_clean(self, auto_fix=True):
        if self.df is None or not self.confirm("Smart clean?"):
            return
        self.auto_backup()
        df = self.df.copy()
        df, col_changes = self._std_cols(df)
        if col_changes:
            print("🔤 Columns:")
            for c in col_changes:
                print(f"   {c}")
        before = len(df)
        df = df.drop_duplicates()
        if len(df) < before:
            print(f"🗑️  {before - len(df):,} duplicates")
        for c in df.columns:
            if df[c].dtype == 'object':
                sample = df[c].dropna().astype(str).str.strip()
                if len(sample) > 0 and sample.str.match(r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}').mean() > 0.8:
                    try:
                        df[c] = pd.to_datetime(df[c], errors='coerce')
                        print(f"📅 '{c}' → date")
                        continue
                    except Exception:
                        pass
                try:
                    num = pd.to_numeric(df[c], errors='coerce')
                    ratio = num.notna().mean()
                except Exception:
                    ratio = 0
                if ratio < 0.8:
                    cl = df[c].astype(str).str.replace(r'[^0-9.\-]', '', regex=True)
                    num = pd.to_numeric(cl, errors='coerce')
                    ratio = num.notna().mean()
                if ratio > 0.8:
                    df[c] = num
                    print(f"🔢 '{c}' → numeric")
                else:
                    df[c] = df[c].astype(str).str.strip().str.lower()
                    if df[c].nunique() < 50 and RAPIDFUZZ_AVAILABLE:
                        vals = df[c].dropna().unique()
                        for v in vals:
                            m = process.extract(v, vals, scorer=fuzz.ratio, limit=2)
                            if len(m) > 1 and m[1][1] > 85:
                                df[c] = df[c].replace(v, m[0][0])
        for c in df.columns:
            if df[c].isnull().sum() > 0:
                if pd.api.types.is_numeric_dtype(df[c]):
                    df[c] = df[c].fillna(df[c].median())
                elif pd.api.types.is_datetime64_any_dtype(df[c]):
                    df[c] = df[c].ffill().bfill()
                else:
                    m = df[c].mode()
                    df[c] = df[c].fillna(m[0] if not m.empty else 'Unknown')
        if auto_fix:
            for c in df.select_dtypes(include=[np.number]).columns:
                q1, q3 = df[c].quantile(0.25), df[c].quantile(0.75)
                iqr = q3 - q1
                lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                out = ((df[c] < lo) | (df[c] > hi)).sum()
                if out > 0:
                    df[c] = df[c].clip(lo, hi)
                    print(f"⚠️  '{c}': {out:,} outliers clipped")
        self.df = df
        self.total_rows = len(df)
        self._invalidate_sqlite_cache()
        print(f"\n✅ Done: {len(self.df):,} rows")

    def dropna(self):
        if self.df is None or not self.confirm("Drop nulls?"):
            return
        self.auto_backup()
        b = len(self.df)
        self.df = self.df.dropna()
        self.total_rows = len(self.df)
        self._invalidate_sqlite_cache()
        print(f"✅ {b:,} → {len(self.df):,}")

    def fillna(self, val):
        if self.df is None:
            print("❌ No data")
            return
        self.auto_backup()
        self.df = self.df.fillna(val)
        self._invalidate_sqlite_cache()
        print(f"✅ Filled: {val}")

    def filter(self, col, op, val):
        """FIX #5: DuckDB mode actually updates self.df"""
        if self.df is None or col not in self.df.columns:
            print(f"❌ '{col}' not found")
            return
        if self._has_duckdb():
            try:
                sql_op = '=' if op == '==' else op
                safe = self._safe_col(col)
                try:
                    fv = float(val)
                    where = f'"{safe}" {sql_op} {fv}'
                except ValueError:
                    sv = str(val).replace("'", "''")
                    where = f"\"{safe}\" {sql_op} '{sv}'"
                self.df = self.duck_con.execute(
                    f"SELECT * FROM {self.duck_table} WHERE {where} LIMIT 100000"
                ).fetchdf()
                cnt = self.duck_con.execute(
                    f"SELECT COUNT(*) FROM {self.duck_table} WHERE {where}"
                ).fetchone()[0]
                self._invalidate_sqlite_cache()
                print(f"⚡ Filter: {cnt:,} rows match (preview: {len(self.df):,})")
                return
            except Exception as e:
                print(f"⚠️  DuckDB filter: {e}")
        try:
            if op == '>':
                self.df = self.df[self.df[col] > float(val)]
            elif op == '<':
                self.df = self.df[self.df[col] < float(val)]
            elif op == '>=':
                self.df = self.df[self.df[col] >= float(val)]
            elif op == '<=':
                self.df = self.df[self.df[col] <= float(val)]
            elif op == '==':
                self.df = self.df[self.df[col] == val]
            elif op == '!=':
                self.df = self.df[self.df[col] != val]
            self._invalidate_sqlite_cache()
            print(f"✅ {len(self.df):,} rows")
        except Exception as e:
            print(f"❌ {e}")

    # ============================================================
    # TRANSFORM
    # ============================================================
    def pivot_data(self, idx, cols, vals, agg='sum'):
        """FIX #13: Cast vals to numeric for sum/mean"""
        if self.df is None:
            print("❌ No data")
            return
        try:
            df = self.df.copy()
            if agg in ('sum', 'mean', 'avg', 'std', 'median') and vals in df.columns:
                df[vals] = pd.to_numeric(df[vals], errors='coerce')
            self.df = pd.pivot_table(df, index=idx, columns=cols,
                                     values=vals, aggfunc=agg).reset_index()
            self._invalidate_sqlite_cache()
            print(f"✅ Pivot: {self.df.shape}")
        except Exception as e:
            print(f"❌ {e}")

    def melt_data(self, idv, valv):
        if self.df is None:
            print("❌ No data")
            return
        try:
            self.df = pd.melt(self.df, id_vars=idv.split(','), value_vars=valv.split(','))
            self._invalidate_sqlite_cache()
            print(f"✅ Melt: {self.df.shape}")
        except Exception as e:
            print(f"❌ {e}")

    def merge_data(self, lc, rf, rc, how='inner'):
        if self.df is None or not os.path.exists(rf):
            print("❌ Data/file missing")
            return
        try:
            r = pd.read_csv(rf)
            self.df = self.df.merge(r, left_on=lc, right_on=rc, how=how)
            self._invalidate_sqlite_cache()
            print(f"✅ Merged: {self.df.shape}")
        except Exception as e:
            print(f"❌ {e}")

    def groupby(self, col, agg='sum'):
        if self.df is None or col not in self.df.columns:
            print(f"❌ '{col}' not found")
            return
        if self._has_duckdb():
            try:
                safe_col = self._safe_col(col)
                schema = self.duck_con.execute(f"DESCRIBE {self.duck_table}").fetchdf()
                num_types = ['BIGINT', 'INTEGER', 'DOUBLE', 'FLOAT', 'DECIMAL',
                             'SMALLINT', 'TINYINT', 'HUGEINT', 'REAL', 'NUMERIC']
                nc = [r['column_name'] for _, r in schema.iterrows()
                      if any(t in str(r['column_type']).upper() for t in num_types)]
                if agg.lower() == 'count':
                    sql = f'SELECT "{safe_col}", COUNT(*) AS count FROM {self.duck_table} GROUP BY "{safe_col}" ORDER BY count DESC'
                elif nc:
                    fn = {'sum': 'SUM', 'mean': 'AVG', 'avg': 'AVG',
                          'min': 'MIN', 'max': 'MAX'}.get(agg.lower(), 'SUM')
                    sels = ', '.join([f'{fn}("{self._safe_col(c)}") AS "{c}"' for c in nc])
                    sql = f'SELECT "{safe_col}", {sels} FROM {self.duck_table} GROUP BY "{safe_col}" ORDER BY 1'
                else:
                    print("❌ No numeric cols")
                    return
                r = self._duck_query(sql)
                if r is not None:
                    print(f"⚡ GroupBy: {col} → {agg}")
                    print(r.to_string())
                return
            except Exception as e:
                print(f"⚠️  {e}")
        try:
            nc = self.df.select_dtypes(include=[np.number]).columns.tolist()
            if agg.lower() == 'count':
                r = self.df.groupby(col).size().reset_index(name='count')
            else:
                r = self.df.groupby(col)[nc].agg(agg)
            print(r.to_string())
        except Exception as e:
            print(f"❌ {e}")

    # ============================================================
    # VISUALIZATION
    # ============================================================
    def plot(self, ptype, x, y=None):
        if self.df is None or x not in self.df.columns:
            print(f"❌ '{x}' not found")
            return
        try:
            total = len(self.df)
            sample = self.df.sample(min(10000, total))
            sampled = len(sample) < total
            plt.figure(figsize=(10, 6))
            if ptype == 'line':
                plt.plot(sample[x], sample[y] if y else sample[x])
            elif ptype == 'bar':
                plt.bar(sample[x], sample[y] if y else sample[x])
            elif ptype == 'scatter':
                plt.scatter(sample[x], sample[y] if y else sample[x])
            elif ptype == 'hist':
                plt.hist(sample[x], bins=20)
            else:
                print("❌ line/bar/scatter/hist")
                return
            title = f"{ptype}: {x}"
            if sampled:
                title += f"  (sampled {len(sample):,} of {total:,})"
            plt.title(title)
            fn = f"plot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            plt.tight_layout()
            plt.savefig(fn, dpi=100)
            plt.close()
            print(f"✅ Saved: {fn}")
            if sampled:
                print(f"   ⚠️  Note: only {len(sample):,} of {total:,} rows plotted")
        except Exception as e:
            print(f"❌ {e}")

    def plotly_viz(self, ptype, x, y=None):
        try:
            import plotly.express as px
        except ImportError:
            print("❌ plotly not installed")
            return
        try:
            if ptype == 'scatter':
                fig = px.scatter(self.df, x=x, y=y)
            elif ptype == 'line':
                fig = px.line(self.df, x=x, y=y)
            elif ptype == 'bar':
                fig = px.bar(self.df, x=x, y=y)
            else:
                print("❌ scatter/line/bar")
                return
            fn = f"plotly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            fig.write_html(fn)
            print(f"✅ Saved: {fn}")
        except Exception as e:
            print(f"❌ {e}")

    def heatmap(self):
        if self.df is None:
            print("❌ No data")
            return
        num = self.df.select_dtypes(include=[np.number])
        if num.empty:
            print("❌ No numeric")
            return
        try:
            plt.figure(figsize=(10, 8))
            sns.heatmap(num.corr(), annot=True, cmap='coolwarm', fmt='.2f')
            fn = f"heatmap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            plt.tight_layout()
            plt.savefig(fn, dpi=100)
            plt.close()
            print(f"✅ Saved: {fn}")
        except Exception as e:
            print(f"❌ {e}")

    def bi(self):
        try:
            import plotly.express as px
        except ImportError:
            print("❌ plotly not installed")
            return
        try:
            sample = self.df.sample(min(10000, len(self.df)))
            nc = sample.select_dtypes(include=[np.number]).columns.tolist()
            cc = sample.select_dtypes(include=['object']).columns.tolist()
            html = "<html><body style='background:#0a0e1a;color:#fff;font-family:Arial'><h1>DataVortex BI</h1>"
            if len(nc) >= 2:
                html += px.scatter(sample, x=nc[0], y=nc[1]).to_html(full_html=False, include_plotlyjs='cdn')
            if cc and nc:
                html += px.bar(sample, x=cc[0], y=nc[0]).to_html(full_html=False, include_plotlyjs='cdn')
            html += "</body></html>"
            fn = f"BI_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            with open(fn, 'w') as f:
                f.write(html)
            print(f"✅ Saved: {fn}")
        except Exception as e:
            print(f"❌ {e}")

    # ============================================================
    # ULTRA DASHBOARD
    # ============================================================
    def ultra_dashboard(self, filename="ultra_dashboard.pdf"):
        if self.df is None:
            print("❌ No data")
            return
        try:
            import plotly.graph_objects as go
            import plotly.io as pio
            print("🎨 Generating ULTRA dashboard (Plotly)...")
            if self._ultra_dashboard_plotly(filename):
                return
        except ImportError:
            print("ℹ️  Plotly not installed — using matplotlib fallback")
        except Exception as e:
            print(f"⚠️  Plotly error: {e}")
            print("   Falling back to matplotlib...")
        print("🎨 Generating dashboard (Matplotlib)...")
        png_file = str(filename).replace('.pdf', '.png')
        self.mini_dashboard(png_file)

    def _ultra_dashboard_plotly(self, filename):
        try:
            import plotly.graph_objects as go
            import plotly.io as pio

            BG = '#0a0e1a'; CARD = '#141b2d'; CYAN = '#00d4ff'
            PURPLE = '#8b5cf6'; PINK = '#ec4899'; GREEN = '#10b981'
            ORANGE = '#f59e0b'; RED = '#ef4444'
            TEXT = '#e2e8f0'; MUTED = '#94a3b8'
            COLORS = [CYAN, PURPLE, PINK, GREEN, ORANGE, RED, '#06b6d4', '#a855f7']

            df = self.df
            nums = df.select_dtypes(include=[np.number]).columns.tolist()
            cats = df.select_dtypes(include=['object', 'category']).columns.tolist()
            dates = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]

            if not nums:
                print("❌ Need numeric columns")
                return False

            # FIX #10: NaN-safe sum
            primary_num = max(nums, key=lambda c: abs(df[c].sum())
                              if pd.notna(df[c].sum()) else 0)
            raw_sum = df[primary_num].sum()
            total_val = float(raw_sum) if pd.notna(raw_sum) else 0.0
            cfg = {'displayModeBar': False, 'responsive': True}
            charts = {}

            fig = go.Figure(go.Indicator(
                mode="gauge+number", value=total_val,
                number={'font': {'size': 38, 'color': CYAN}},
                gauge={
                    'axis': {'range': [0, max(total_val * 1.2, 1)], 'tickcolor': MUTED},
                    'bar': {'color': CYAN, 'thickness': 0.35},
                    'bgcolor': CARD, 'borderwidth': 2, 'bordercolor': CYAN,
                    'steps': [
                        {'range': [0, total_val * 0.5], 'color': 'rgba(0,212,255,0.1)'},
                        {'range': [total_val * 0.5, total_val], 'color': 'rgba(0,212,255,0.2)'},
                    ],
                    'threshold': {'line': {'color': PINK, 'width': 4},
                                  'thickness': 0.8, 'value': total_val * 0.95}
                },
                title={'text': f"<b>{primary_num.upper()}</b><br><span style='font-size:11px;color:{MUTED}'>TOTAL</span>",
                       'font': {'size': 13, 'color': TEXT}}
            ))
            fig.update_layout(paper_bgcolor=BG, plot_bgcolor=BG,
                            height=280, margin=dict(l=30, r=30, t=50, b=20),
                            font={'family': 'Arial', 'color': TEXT})
            charts['gauge'] = pio.to_html(fig, full_html=False, include_plotlyjs=False, config=cfg)

            if cats:
                pc = df[cats[0]].value_counts().head(8)
                fig = go.Figure(data=[go.Pie(
                    labels=pc.index, values=pc.values, hole=0.55,
                    marker=dict(colors=COLORS[:len(pc)], line=dict(color=BG, width=3)),
                    textinfo='label+percent', textfont=dict(size=10, color='white')
                )])
                fig.update_layout(paper_bgcolor=BG, plot_bgcolor=BG,
                                height=280, margin=dict(l=20, r=20, t=50, b=20),
                                font={'family': 'Arial', 'color': TEXT},
                                title={'text': f"<b>{cats[0].upper()}</b>",
                                       'font': {'size': 13, 'color': TEXT}})
                charts['pie'] = pio.to_html(fig, full_html=False, include_plotlyjs=False, config=cfg)

            if cats:
                cr = df.groupby(cats[0])[primary_num].sum().sort_values(ascending=True).tail(10)
                fig = go.Figure(go.Bar(
                    x=cr.values, y=[str(x)[:18] for x in cr.index], orientation='h',
                    marker=dict(color=cr.values,
                                colorscale=[[0, PURPLE], [0.5, CYAN], [1, GREEN]],
                                line=dict(color=BG, width=2)),
                    text=[f'{v:,.0f}' for v in cr.values],
                    textposition='outside', textfont=dict(color=TEXT, size=9)
                ))
                fig.update_layout(paper_bgcolor=BG, plot_bgcolor=BG,
                                height=300, margin=dict(l=10, r=60, t=50, b=30),
                                font={'family': 'Arial', 'color': TEXT},
                                title={'text': f"<b>TOP {cats[0].upper()}</b>",
                                       'font': {'size': 13, 'color': TEXT}},
                                xaxis=dict(gridcolor='rgba(148,163,184,0.15)',
                                          tickfont=dict(color=MUTED, size=9)),
                                yaxis=dict(gridcolor='rgba(148,163,184,0.15)',
                                          tickfont=dict(color=TEXT, size=10)))
                charts['bar'] = pio.to_html(fig, full_html=False, include_plotlyjs=False, config=cfg)

            fig = go.Figure(go.Histogram(
                x=df[primary_num].dropna(), nbinsx=30,
                marker=dict(color=CYAN, line=dict(color=BG, width=1))
            ))
            fig.update_layout(paper_bgcolor=BG, plot_bgcolor=BG,
                            height=280, margin=dict(l=30, r=20, t=50, b=35),
                            font={'family': 'Arial', 'color': TEXT},
                            title={'text': f"<b>{primary_num.upper()} DISTRIBUTION</b>",
                                   'font': {'size': 13, 'color': TEXT}},
                            xaxis=dict(gridcolor='rgba(148,163,184,0.15)',
                                      tickfont=dict(color=MUTED, size=9)),
                            yaxis=dict(gridcolor='rgba(148,163,184,0.15)',
                                      tickfont=dict(color=MUTED, size=9)))
            charts['hist'] = pio.to_html(fig, full_html=False, include_plotlyjs=False, config=cfg)

            cc = nums[:6]
            corr = df[cc].corr()
            fig = go.Figure(data=go.Heatmap(
                z=corr.values, x=[c[:12] for c in cc], y=[c[:12] for c in cc],
                colorscale=[[0, RED], [0.5, CARD], [1, GREEN]], zmid=0,
                text=[[f'{v:.2f}' for v in row] for row in corr.values],
                texttemplate='%{text}', textfont=dict(size=10, color='white')
            ))
            fig.update_layout(paper_bgcolor=BG, plot_bgcolor=BG,
                            height=300, margin=dict(l=20, r=20, t=50, b=40),
                            font={'family': 'Arial', 'color': TEXT},
                            title={'text': "<b>CORRELATION</b>",
                                   'font': {'size': 13, 'color': TEXT}},
                            xaxis=dict(tickfont=dict(color=MUTED, size=9)),
                            yaxis=dict(tickfont=dict(color=MUTED, size=9)))
            charts['heat'] = pio.to_html(fig, full_html=False, include_plotlyjs=False, config=cfg)

            if dates:
                try:
                    dfc = df.copy()
                    dfc[dates[0]] = pd.to_datetime(dfc[dates[0]], errors='coerce')
                    dfc = dfc.dropna(subset=[dates[0]])
                    if len(dfc) > 0:
                        monthly = dfc.groupby(dfc[dates[0]].dt.to_period('M'))[primary_num].sum()
                        monthly.index = monthly.index.astype(str)
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=list(monthly.index), y=list(monthly.values),
                            mode='lines+markers',
                            line=dict(color=CYAN, width=3, shape='spline'),
                            marker=dict(size=7, color=CYAN, line=dict(color='white', width=2)),
                            fill='tozeroy', fillcolor='rgba(0,212,255,0.15)'
                        ))
                        fig.update_layout(paper_bgcolor=BG, plot_bgcolor=BG,
                                        height=280, margin=dict(l=20, r=20, t=50, b=55),
                                        font={'family': 'Arial', 'color': TEXT},
                                        title={'text': f"<b>{primary_num.upper()} TREND</b>",
                                               'font': {'size': 13, 'color': TEXT}},
                                        xaxis=dict(gridcolor='rgba(148,163,184,0.15)',
                                                  tickfont=dict(color=MUTED, size=9)),
                                        yaxis=dict(gridcolor='rgba(148,163,184,0.15)',
                                                  tickfont=dict(color=MUTED, size=9)))
                        charts['line'] = pio.to_html(fig, full_html=False, include_plotlyjs=False, config=cfg)
                except Exception:
                    pass

            if len(nums) >= 2:
                fig = go.Figure(go.Scatter(
                    x=df[nums[0]].head(3000), y=df[nums[1]].head(3000),
                    mode='markers',
                    marker=dict(size=5, color=df[primary_num].head(3000),
                                colorscale=[[0, PURPLE], [1, CYAN]], opacity=0.6)
                ))
                fig.update_layout(paper_bgcolor=BG, plot_bgcolor=BG,
                                height=280, margin=dict(l=40, r=20, t=50, b=40),
                                font={'family': 'Arial', 'color': TEXT},
                                title={'text': f"<b>{nums[0].upper()} vs {nums[1].upper()}</b>",
                                       'font': {'size': 13, 'color': TEXT}},
                                xaxis=dict(gridcolor='rgba(148,163,184,0.15)',
                                          tickfont=dict(color=MUTED, size=9)),
                                yaxis=dict(gridcolor='rgba(148,163,184,0.15)',
                                          tickfont=dict(color=MUTED, size=9)))
                charts['scatter'] = pio.to_html(fig, full_html=False, include_plotlyjs=False, config=cfg)

            k1 = f"₹{total_val:,.0f}" if total_val > 100000 else f"{total_val:,.2f}"
            k2 = f"{len(df):,}"
            k3 = f"{df[primary_num].mean():,.2f}"
            k4 = f"{df[primary_num].median():,.2f}"
            k5 = f"{df[primary_num].max():,.0f}"
            k6 = f"{len(cats)}"

            html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>DataVortex Ultra Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',Arial,sans-serif;background:linear-gradient(135deg,#0a0e1a 0%,#141b2d 50%,#1a1f35 100%);color:#e2e8f0;padding:20px;min-height:100vh}}
.header{{background:linear-gradient(90deg,rgba(0,212,255,0.15),rgba(139,92,246,0.15));border:2px solid rgba(0,212,255,0.3);border-radius:16px;padding:24px 32px;margin-bottom:20px;position:relative;overflow:hidden}}
.header::before{{content:'';position:absolute;top:-50%;right:-50%;width:200%;height:200%;background:radial-gradient(circle,rgba(0,212,255,0.15) 0%,transparent 70%);animation:rot 10s linear infinite}}
@keyframes rot{{from{{transform:rotate(0deg)}}to{{transform:rotate(360deg)}}}}
.hc{{position:relative;z-index:1}}
h1{{font-size:30px;font-weight:800;background:linear-gradient(90deg,#00d4ff,#8b5cf6,#ec4899);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:6px}}
.sub{{color:#94a3b8;font-size:13px}}
.meta{{display:flex;gap:20px;margin-top:10px;flex-wrap:wrap}}
.meta span{{color:#00d4ff;font-size:11px;font-weight:600}}
.kpis{{display:grid;grid-template-columns:repeat(6,1fr);gap:14px;margin-bottom:18px}}
.kpi{{background:linear-gradient(145deg,#141b2d,#0f1425);border:1px solid rgba(0,212,255,0.2);border-radius:12px;padding:18px 14px;position:relative;overflow:hidden}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,#00d4ff,#8b5cf6,#ec4899)}}
.kl{{font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:1.2px;font-weight:600;margin-bottom:8px}}
.kv{{font-size:20px;font-weight:800;color:#00d4ff;margin-bottom:3px}}
.ks{{font-size:9px;color:#64748b}}
.row{{display:grid;gap:14px;margin-bottom:14px}}
.r2{{grid-template-columns:1fr 1fr}}
.r12{{grid-template-columns:1fr 2fr}}
.card{{background:linear-gradient(145deg,#141b2d,#0f1425);border:1px solid rgba(0,212,255,0.15);border-radius:12px;overflow:hidden;position:relative}}
.card::after{{content:'';position:absolute;top:0;left:0;width:100%;height:2px;background:linear-gradient(90deg,#00d4ff,#8b5cf6);opacity:0.7}}
.foot{{text-align:center;padding:18px;margin-top:20px;color:#64748b;font-size:11px;border-top:1px solid rgba(0,212,255,0.15)}}
.foot b{{color:#00d4ff}}
@media print{{body{{padding:10px}}@page{{size:A4 landscape;margin:0}}.header::before{{animation:none}}}}
</style></head><body>

<div class="header"><div class="hc">
<h1>⚡ DATA VORTEX ULTRA DASHBOARD</h1>
<p class="sub">Enterprise analytics for <strong>{self.data_name}</strong></p>
<div class="meta">
<span>📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}</span>
<span>📊 {len(df):,} records</span>
<span>🎯 {len(df.columns)} columns</span>
<span>⚙️ {self.data_engine.upper()}</span>
</div></div></div>

<div class="kpis">
<div class="kpi"><div class="kl">{primary_num[:14]}</div><div class="kv">{k1}</div><div class="ks">Total</div></div>
<div class="kpi"><div class="kl">Records</div><div class="kv">{k2}</div><div class="ks">Points</div></div>
<div class="kpi"><div class="kl">Average</div><div class="kv">{k3}</div><div class="ks">Mean</div></div>
<div class="kpi"><div class="kl">Median</div><div class="kv">{k4}</div><div class="ks">Middle</div></div>
<div class="kpi"><div class="kl">Maximum</div><div class="kv">{k5}</div><div class="ks">Peak</div></div>
<div class="kpi"><div class="kl">Categories</div><div class="kv">{k6}</div><div class="ks">Groups</div></div>
</div>

<div class="row r2">
<div class="card">{charts.get('gauge', '')}</div>
<div class="card">{charts.get('pie', '')}</div>
</div>

<div class="row r12">
<div class="card">{charts.get('hist', '')}</div>
<div class="card">{charts.get('bar', '')}</div>
</div>

<div class="row r2">
<div class="card">{charts.get('line', charts.get('heat', ''))}</div>
<div class="card">{charts.get('scatter', '')}</div>
</div>

<div class="row r2">
<div class="card">{charts.get('heat', '')}</div>
<div class="card">{charts.get('bar', '')}</div>
</div>

<div class="foot">
Generated by <b>DataVortex v26.2</b> | <b>{CREATOR}</b> | 📧 {CREATOR_EMAIL} | {COPYRIGHT}
</div>
</body></html>"""

            html_file = str(filename).replace('.pdf', '.html')
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"✅ HTML: {html_file}")

            if WEASYPRINT_AVAILABLE:
                try:
                    WeasyHTML(string=html, base_url=os.getcwd()).write_pdf(filename)
                    print(f"✅ PDF: {filename}")
                except Exception as e:
                    print(f"⚠️  PDF: {e}")
            print(f"\n📸 Open: termux-open {html_file}")
            return True
        except Exception as e:
            print(f"❌ Plotly dashboard: {e}")
            return False

    # ============================================================
    # MINI DASHBOARD
    # ============================================================
    def mini_dashboard(self, filename="mini_dashboard.png"):
        if self.df is None:
            print("❌ No data")
            return
        try:
            print("🎨 Generating mini dashboard...")
            BG = '#0a0e1a'; CARD = '#141b2d'; CYAN = '#00d4ff'
            PURPLE = '#8b5cf6'; PINK = '#ec4899'; GREEN = '#10b981'
            ORANGE = '#f59e0b'; RED = '#ef4444'
            TEXT = '#e2e8f0'; MUTED = '#94a3b8'
            COLORS = [CYAN, PURPLE, PINK, GREEN, ORANGE, RED, '#06b6d4', '#a855f7']

            df = self.df
            if len(df) > 50000:
                df = df.sample(50000, random_state=42)
            nums = df.select_dtypes(include=[np.number]).columns.tolist()
            cats = df.select_dtypes(include=['object', 'category']).columns.tolist()
            if not nums:
                print("❌ Need numeric columns")
                return
            primary_num = max(nums, key=lambda c: abs(df[c].sum())
                              if pd.notna(df[c].sum()) else 0)

            fig = plt.figure(figsize=(22, 14), facecolor=BG)
            fig.text(0.5, 0.975, '⚡ DATA VORTEX ULTRA DASHBOARD',
                    ha='center', va='top', fontsize=32, fontweight='bold', color=CYAN)
            fig.text(0.5, 0.945, f'Enterprise Analytics  |  {self.data_name}',
                    ha='center', va='top', fontsize=13, color=MUTED)
            fig.text(0.5, 0.925,
                    f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}  |  '
                    f'{len(df):,} records  |  {len(df.columns)} columns',
                    ha='center', va='top', fontsize=10, color=CYAN)

            kpis = [
                (primary_num.upper()[:14], f"{df[primary_num].sum():,.0f}", "TOTAL", CYAN),
                ("RECORDS", f"{len(df):,}", "DATA", PURPLE),
                ("AVERAGE", f"{df[primary_num].mean():,.2f}", "MEAN", PINK),
                ("MEDIAN", f"{df[primary_num].median():,.2f}", "MIDDLE", GREEN),
                ("MAXIMUM", f"{df[primary_num].max():,.0f}", "PEAK", ORANGE),
                ("CATEGORIES", f"{len(cats)}", "GROUPS", RED),
            ]
            for i, (lbl, val, sub, color) in enumerate(kpis):
                ax = fig.add_axes([0.02 + i * 0.163, 0.80, 0.15, 0.10])
                ax.set_facecolor(CARD); ax.set_xticks([]); ax.set_yticks([])
                for sp in ax.spines.values():
                    sp.set_color(color); sp.set_linewidth(2)
                ax.plot([0, 1], [1, 1], color=color, linewidth=3, transform=ax.transAxes)
                ax.text(0.5, 0.78, lbl, ha='center', va='center',
                        fontsize=9, color=MUTED, fontweight='bold')
                ax.text(0.5, 0.42, val, ha='center', va='center',
                        fontsize=16, color=color, fontweight='bold')
                ax.text(0.5, 0.12, sub, ha='center', va='center',
                        fontsize=8, color=MUTED, style='italic')

            if cats:
                ax1 = fig.add_axes([0.02, 0.52, 0.22, 0.25]); ax1.set_facecolor(BG)
                cc = df[cats[0]].value_counts().head(6)
                wedges, _, _ = ax1.pie(
                    cc.values, labels=None, autopct='%1.1f%%',
                    colors=COLORS[:len(cc)], startangle=90, pctdistance=0.75,
                    wedgeprops=dict(width=0.45, edgecolor=BG, linewidth=3),
                    textprops=dict(color='white', fontsize=10, weight='bold'))
                ax1.text(0, 0, f"{len(cc)}\nCAT", ha='center', va='center',
                        fontsize=11, color=CYAN, fontweight='bold')
                ax1.set_title(f'{cats[0].upper()}', color=CYAN, fontsize=13,
                             fontweight='bold', pad=12)
                ax1.legend(wedges, [str(x)[:14] for x in cc.index],
                          loc='center left', bbox_to_anchor=(0.95, 0.5),
                          fontsize=9, labelcolor=TEXT, framealpha=0)

            if cats:
                ax2 = fig.add_axes([0.30, 0.52, 0.32, 0.25]); ax2.set_facecolor(BG)
                top = df.groupby(cats[0])[primary_num].sum().nlargest(8).sort_values()
                colors_bar = [COLORS[i % len(COLORS)] for i in range(len(top))]
                bars = ax2.barh(range(len(top)), top.values, color=colors_bar,
                               height=0.7, edgecolor='white', linewidth=1.5)
                ax2.set_yticks(range(len(top)))
                ax2.set_yticklabels([str(x)[:18] for x in top.index],
                                   color=TEXT, fontsize=10, fontweight='bold')
                ax2.set_title(f'TOP {cats[0].upper()}', color=CYAN, fontsize=13,
                             fontweight='bold', pad=12)
                ax2.tick_params(axis='x', colors=MUTED, labelsize=9)
                ax2.grid(True, alpha=0.2, axis='x', color='#334455', linestyle='--')
                for sp in ax2.spines.values():
                    sp.set_color('#334455')
                for b, v in zip(bars, top.values):
                    ax2.text(v, b.get_y() + b.get_height()/2, f' {v:,.0f}',
                            va='center', ha='left', color=TEXT,
                            fontsize=9, fontweight='bold')

            ax3 = fig.add_axes([0.66, 0.52, 0.32, 0.25]); ax3.set_facecolor(BG)
            n, bins, patches = ax3.hist(df[primary_num].dropna(), bins=25,
                                        color=CYAN, edgecolor='white',
                                        linewidth=1, alpha=0.85)
            # FIX #2: version-safe colormap
            try:
                cmap = plt.colormaps['cool']
            except (AttributeError, KeyError):
                cmap = plt.cm.get_cmap('cool')
            for i, p in enumerate(patches):
                p.set_facecolor(cmap(i / max(len(patches) - 1, 1)))
            ax3.set_title(f'{primary_num.upper()} DISTRIBUTION', color=CYAN,
                         fontsize=13, fontweight='bold', pad=12)
            ax3.tick_params(colors=MUTED, labelsize=9)
            ax3.grid(True, alpha=0.2, color='#334455', linestyle='--')
            for sp in ax3.spines.values():
                sp.set_color('#334455')

            ax4 = fig.add_axes([0.02, 0.24, 0.32, 0.23]); ax4.set_facecolor(BG)
            date_col = next((c for c in df.columns
                            if 'date' in c.lower() or 'time' in c.lower()), None)
            if date_col:
                try:
                    dfc = df.copy()
                    dfc[date_col] = pd.to_datetime(dfc[date_col], errors='coerce')
                    dfc = dfc.dropna(subset=[date_col])
                    if len(dfc) > 0:
                        monthly = dfc.groupby(dfc[date_col].dt.to_period('M'))[primary_num].sum()
                        monthly.index = monthly.index.astype(str)
                        ax4.plot(range(len(monthly)), monthly.values,
                                color=CYAN, linewidth=3, marker='o', markersize=8,
                                markerfacecolor='white', markeredgecolor=CYAN,
                                markeredgewidth=2)
                        ax4.fill_between(range(len(monthly)), monthly.values,
                                        alpha=0.25, color=CYAN)
                        step = max(1, len(monthly) // 8)
                        ax4.set_xticks(range(0, len(monthly), step))
                        ax4.set_xticklabels([monthly.index[i][-7:]
                                            for i in range(0, len(monthly), step)],
                                           rotation=45, ha='right', color=MUTED, fontsize=9)
                        ax4.set_title(f'{primary_num.upper()} TREND', color=CYAN,
                                     fontsize=13, fontweight='bold', pad=12)
                        ax4.tick_params(axis='y', colors=MUTED, labelsize=9)
                        ax4.grid(True, alpha=0.2, color='#334455', linestyle='--')
                        for sp in ax4.spines.values():
                            sp.set_color('#334455')
                except Exception:
                    pass

            ax5 = fig.add_axes([0.38, 0.24, 0.28, 0.23]); ax5.set_facecolor(BG)
            cc = nums[:6]
            corr = df[cc].corr()
            ax5.imshow(corr.values, cmap='RdYlGn', vmin=-1, vmax=1, aspect='auto')
            ax5.set_xticks(range(len(cc))); ax5.set_yticks(range(len(cc)))
            ax5.set_xticklabels([c[:12] for c in cc], color=MUTED, fontsize=9,
                               rotation=45, ha='right')
            ax5.set_yticklabels([c[:12] for c in cc], color=MUTED, fontsize=9)
            for i in range(len(cc)):
                for j in range(len(cc)):
                    val = corr.values[i, j]
                    ax5.text(j, i, f'{val:.2f}', ha='center', va='center',
                            color='white' if abs(val) > 0.5 else 'black',
                            fontsize=8, fontweight='bold')
            ax5.set_title('CORRELATION', color=CYAN, fontsize=13,
                         fontweight='bold', pad=12)
            for sp in ax5.spines.values():
                sp.set_color('#334455')

            if len(nums) >= 2:
                ax6 = fig.add_axes([0.70, 0.24, 0.28, 0.23]); ax6.set_facecolor(BG)
                sample = df.sample(min(3000, len(df)))
                ax6.scatter(sample[nums[0]], sample[nums[1]],
                           c=sample[primary_num], cmap='cool', s=25, alpha=0.6,
                           edgecolors='white', linewidth=0.5)
                ax6.set_xlabel(nums[0][:18], color=MUTED, fontsize=10)
                ax6.set_ylabel(nums[1][:18], color=MUTED, fontsize=10)
                ax6.set_title(f'{nums[0].upper()} vs {nums[1].upper()}',
                             color=CYAN, fontsize=13, fontweight='bold', pad=12)
                ax6.tick_params(colors=MUTED, labelsize=9)
                ax6.grid(True, alpha=0.2, color='#334455', linestyle='--')
                for sp in ax6.spines.values():
                    sp.set_color('#334455')

            fig.text(0.5, 0.005,
                    f'⚡ DataVortex v26.2  |  👑 {CREATOR}  |  '
                    f'📧 {CREATOR_EMAIL}  |  {COPYRIGHT}',
                    ha='center', va='bottom', fontsize=10, color=MUTED, style='italic')

            plt.savefig(filename, dpi=120, bbox_inches='tight',
                       facecolor=BG, edgecolor='none')
            plt.close()
            print(f"✅ Saved: {filename}")
            print(f"📸 Open: termux-open {filename}")
        except Exception as e:
            print(f"❌ Mini dashboard: {e}")

    # ============================================================
    # ML
    # ============================================================
    def _resolve_features(self, target, features):
        """FIX #8: Central feature resolution with guards"""
        if features == 'auto':
            fcols, _ = self.detect_features(target)
            if not fcols:
                print("❌ No numeric features found. Specify manually: col1,col2")
                return None
        else:
            fcols = [f.strip() for f in features.split(',') if f.strip()]
            if not fcols:
                print("❌ No features provided")
                return None
        return fcols

    def predict(self, target, features):
        if not SKLEARN_AVAILABLE:
            print("❌ sklearn not installed")
            return
        if self.df is None or target not in self.df.columns:
            print(f"❌ '{target}' not found")
            return
        try:
            fcols = self._resolve_features(target, features)
            if fcols is None:
                return
            clean = self.df[fcols + [target]].dropna()
            if len(clean) < 10:
                print("❌ Not enough clean rows (need 10+)")
                return
            X, y = clean[fcols].values, clean[target].values
            Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42)
            m = LinearRegression()
            m.fit(Xtr, ytr)
            yp = m.predict(Xte)
            print(f"✅ LinearRegression R²={r2_score(yte, yp):.4f}")
            print(f"   Coefficients: {dict(zip(fcols, m.coef_))}")
        except Exception as e:
            print(f"❌ {e}")

    def rf_predict(self, target, features):
        if not SKLEARN_AVAILABLE:
            print("❌ sklearn not installed")
            return
        if self.df is None or target not in self.df.columns:
            print(f"❌ '{target}' not found")
            return
        try:
            fcols = self._resolve_features(target, features)
            if fcols is None:
                return
            clean = self.df[fcols + [target]].dropna()
            if len(clean) < 10:
                print("❌ Not enough clean rows")
                return
            X, y = clean[fcols].values, clean[target].values
            Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42)
            m = RandomForestRegressor(n_estimators=100, random_state=42)
            m.fit(Xtr, ytr)
            yp = m.predict(Xte)
            print(f"✅ RandomForest R²={r2_score(yte, yp):.4f}")
            print(f"   Feature Importance: {dict(zip(fcols, m.feature_importances_))}")
        except Exception as e:
            print(f"❌ {e}")

    def gb_predict(self, target, features):
        if not SKLEARN_AVAILABLE:
            print("❌ sklearn not installed")
            return
        if self.df is None or target not in self.df.columns:
            print(f"❌ '{target}' not found")
            return
        try:
            fcols = self._resolve_features(target, features)
            if fcols is None:
                return
            clean = self.df[fcols + [target]].dropna()
            if len(clean) < 10:
                print("❌ Not enough clean rows")
                return
            X, y = clean[fcols].values, clean[target].values
            Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42)
            m = GradientBoostingRegressor(n_estimators=100, random_state=42)
            m.fit(Xtr, ytr)
            yp = m.predict(Xte)
            print(f"✅ GradientBoosting R²={r2_score(yte, yp):.4f}")
        except Exception as e:
            print(f"❌ {e}")

    def cluster(self, features):
        """FIX #4: version-safe KMeans n_init"""
        if not SKLEARN_AVAILABLE:
            print("❌ sklearn not installed")
            return
        if self.df is None:
            print("❌ No data")
            return
        try:
            if features == 'auto':
                fcols = self.df.select_dtypes(include=[np.number]).columns.tolist()
                if not fcols:
                    print("❌ No numeric features")
                    return
            else:
                fcols = [f.strip() for f in features.split(',') if f.strip()]
                if not fcols:
                    print("❌ No features provided")
                    return
            X = self.df[fcols].fillna(0)
            if len(X) < 3:
                print("❌ Need 3+ rows")
                return
            Xs = StandardScaler().fit_transform(X)
            k = min(3, len(X))
            try:
                m = KMeans(n_clusters=k, random_state=42, n_init='auto')
            except TypeError:
                m = KMeans(n_clusters=k, random_state=42, n_init=10)
            self.df['cluster'] = m.fit_predict(Xs)
            self._invalidate_sqlite_cache()
            print(self.df.groupby('cluster').size())
        except Exception as e:
            print(f"❌ {e}")

    def pca(self, features):
        if not SKLEARN_AVAILABLE:
            print("❌ sklearn not installed")
            return
        if self.df is None:
            print("❌ No data")
            return
        try:
            if features == 'auto':
                fcols = self.df.select_dtypes(include=[np.number]).columns.tolist()
            else:
                fcols = [f.strip() for f in features.split(',') if f.strip()]
            if not fcols:
                print("❌ No features")
                return
            X = self.df[fcols].fillna(0)
            Xs = StandardScaler().fit_transform(X)
            p = PCA(n_components=min(2, len(fcols)))
            p.fit(Xs)
            print(f"✅ PCA: {p.explained_variance_ratio_}")
        except Exception as e:
            print(f"❌ {e}")

    def cv(self, target, features):
        if not SKLEARN_AVAILABLE:
            print("❌ sklearn not installed")
            return
        if self.df is None or target not in self.df.columns:
            print(f"❌ '{target}' not found")
            return
        try:
            fcols = self._resolve_features(target, features)
            if fcols is None:
                return
            clean = self.df[fcols + [target]].dropna()
            if len(clean) < 10:
                print("❌ Need 10+ clean rows")
                return
            X = clean[fcols].values
            y = clean[target].values
            scores = cross_val_score(LinearRegression(), X, y, cv=min(5, len(clean)))
            print(f"✅ CV: {scores}, mean={scores.mean():.4f}")
        except Exception as e:
            print(f"❌ {e}")

    def detect_features(self, target=None):
        """FIX #3: Returns empty lists, not None"""
        if self.df is None:
            print("⚠️  No data loaded")
            return [], []
        nums = self.df.select_dtypes(include=[np.number]).columns.tolist()
        cats = self.df.select_dtypes(include=['object']).columns.tolist()
        if target and target in nums:
            nums.remove(target)
        print(f"Numeric: {nums}")
        print(f"Categorical: {cats}")
        return nums, cats

    # ============================================================
    # TIME SERIES
    # ============================================================
    def forecast(self, col, steps=10):
        if not SKLEARN_AVAILABLE:
            print("❌ sklearn not installed")
            return
        if self.df is None or col not in self.df.columns:
            print(f"❌ '{col}' not found")
            return
        try:
            dc = next((c for c in self.df.columns
                      if 'date' in c.lower() or 'time' in c.lower()), None)
            if dc:
                self.df[dc] = pd.to_datetime(self.df[dc], errors='coerce')
                self.df = self.df.dropna(subset=[dc])
                self.df['_ord'] = self.df[dc].map(pd.Timestamp.toordinal)
                X = self.df[['_ord']].fillna(0)
            else:
                self.df['_i'] = range(len(self.df))
                X = self.df[['_i']].fillna(0)
            y = self.df[col].fillna(0).values
            if len(y) < 10:
                print("❌ Need 10+ points")
                return
            m = GradientBoostingRegressor(n_estimators=100, random_state=42)
            m.fit(X.iloc[:len(y)], y)
            if dc:
                fd = pd.date_range(start=self.df[dc].iloc[-1] + pd.Timedelta(days=1), periods=steps)
                fX = pd.DataFrame({'_ord': fd.map(pd.Timestamp.toordinal)})
            else:
                fX = pd.DataFrame({'_i': range(len(y), len(y) + steps)})
            pred = m.predict(fX)
            print(f"\n📈 Forecast '{col}':")
            for i, v in enumerate(pred, 1):
                print(f"   Step {i}: {v:,.2f}")
            plt.figure(figsize=(12, 6))
            plt.plot(range(len(y)), y, label='Historical')
            plt.plot(range(len(y), len(y) + steps), pred, color='red', label='Forecast')
            plt.legend(); plt.grid(True, alpha=0.3)
            fn = f"forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            plt.savefig(fn, dpi=100); plt.close()
            print(f"✅ Saved: {fn}")
        except Exception as e:
            print(f"❌ {e}")

    def smooth(self, col, steps=10):
        if self.df is None or col not in self.df.columns:
            print(f"❌ '{col}' not found")
            return
        s = self.df[col].dropna().values
        if len(s) < 10:
            print("❌ Need 10+")
            return
        window = min(5, len(s) - 1)
        seed = list(s[-window:])
        pred = []
        for _ in range(steps):
            nxt = sum(seed[-window:]) / window
            pred.append(nxt)
            seed.append(nxt)
        print(f"📈 Moving Avg '{col}' (window={window}):")
        for i, v in enumerate(pred, 1):
            print(f"   Step {i}: {v:,.2f}")

    def anomaly(self, col):
        if self.df is None or col not in self.df.columns:
            print(f"❌ '{col}' not found")
            return
        if not np.issubdtype(self.df[col].dtype, np.number):
            print("❌ Numeric only")
            return
        mean, std = self.df[col].mean(), self.df[col].std()
        if std == 0:
            print(f"🚨 '{col}' has zero variance — no anomalies")
            return
        thresh = 2.5 * std
        anom = self.df[abs(self.df[col] - mean) > thresh]
        print(f"🚨 Anomalies in '{col}': {len(anom)}")

    def arima(self, col, steps=10):
        """FIX #9: float cast for forecast values"""
        if not STATSMODELS_AVAILABLE:
            print("❌ statsmodels not installed")
            return
        if self.df is None or col not in self.df.columns:
            print(f"❌ '{col}' not found")
            return
        try:
            s = self.df[col].dropna()
            if len(s) < 20:
                print("❌ Need 20+ points")
                return
            m = ARIMA(s, order=(1, 1, 1)).fit()
            pred = m.forecast(steps=steps)
            print(f"📈 ARIMA '{col}':")
            for i, v in enumerate(pred, 1):
                print(f"   Step {i}: {float(v):,.2f}")
        except Exception as e:
            print(f"❌ {e}")

    def decompose(self, col):
        if not STATSMODELS_AVAILABLE:
            print("❌ statsmodels not installed")
            return
        if self.df is None or col not in self.df.columns:
            print(f"❌ '{col}' not found")
            return
        try:
            from statsmodels.tsa.seasonal import seasonal_decompose
            s = self.df[col].dropna()
            if len(s) < 24:
                print("❌ Need 24+ points for decomposition")
                return
            d = seasonal_decompose(s, model='additive', period=12)
            plt.figure(figsize=(12, 8))
            plt.subplot(411); plt.plot(d.observed); plt.title('Observed')
            plt.subplot(412); plt.plot(d.trend); plt.title('Trend')
            plt.subplot(413); plt.plot(d.seasonal); plt.title('Seasonal')
            plt.subplot(414); plt.plot(d.resid); plt.title('Residual')
            fn = f"decompose_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            plt.tight_layout(); plt.savefig(fn); plt.close()
            print(f"✅ Saved: {fn}")
        except Exception as e:
            print(f"❌ {e}")

    # ============================================================
    # STATS
    # ============================================================
    def ttest(self, c1, c2=None, gc=None, g1=None, g2=None):
        if not SCIPY_AVAILABLE:
            print("❌ scipy not installed")
            return
        try:
            if gc and g1 and g2:
                a = self.df[self.df[gc] == g1][c1].dropna()
                b = self.df[self.df[gc] == g2][c1].dropna()
                if len(a) < 2 or len(b) < 2:
                    print("❌ Groups too small")
                    return
                s, p = stats.ttest_ind(a, b)
                print(f"✅ Indep t: t={s:.4f}, p={p:.4f}")
            elif c2:
                a = self.df[c1].dropna()
                b = self.df[c2].dropna()
                if len(a) != len(b):
                    print("❌ Paired samples must have equal length")
                    return
                s, p = stats.ttest_rel(a, b)
                print(f"✅ Paired t: t={s:.4f}, p={p:.4f}")
            else:
                print("❌ Need 2 columns or grouping info")
        except Exception as e:
            print(f"❌ {e}")

    def anova(self, vc, gc):
        if not SCIPY_AVAILABLE:
            print("❌ scipy not installed")
            return
        try:
            groups = [g[vc].dropna().values for _, g in self.df.groupby(gc)
                      if len(g[vc].dropna()) > 0]
            if len(groups) < 2:
                print("❌ Need 2+ groups")
                return
            s, p = stats.f_oneway(*groups)
            print(f"✅ ANOVA: F={s:.4f}, p={p:.4f}")
        except Exception as e:
            print(f"❌ {e}")

    def chisq(self, c1, c2):
        if not SCIPY_AVAILABLE:
            print("❌ scipy not installed")
            return
        try:
            ct = pd.crosstab(self.df[c1], self.df[c2])
            if ct.size == 0:
                print("❌ Empty contingency table")
                return
            s, p, d, _ = stats.chi2_contingency(ct)
            print(f"✅ Chi²: {s:.4f}, p={p:.4f}, dof={d}")
        except Exception as e:
            print(f"❌ {e}")

    # ============================================================
    # SQL
    # ============================================================
    def sql(self, query):
        if self.df is None:
            print("❌ No data")
            return
        try:
            cur_id = id(self.df)
            if self._sqlite_con is None or self._sqlite_df_id != cur_id:
                if self._sqlite_con is not None:
                    try:
                        self._sqlite_con.close()
                    except Exception:
                        pass
                self._sqlite_con = sqlite3.connect(':memory:')
                self.df.to_sql('df', self._sqlite_con, index=False, if_exists='replace')
                self._sqlite_df_id = cur_id
            print(pd.read_sql_query(query, self._sqlite_con).to_string())
        except Exception as e:
            print(f"❌ {e}")

    def query_duckdb(self, sql):
        sql = sql.strip().rstrip(';')
        if not sql:
            print("❌ Empty")
            return
        if not self._has_duckdb():
            if self.df is not None and DUCKDB_AVAILABLE:
                self.duck_con = duckdb.connect(':memory:')
                self.duck_con.register('data', self.df)
                self.duck_table = "data"
            else:
                print("❌ No data")
                return
        try:
            r = self.duck_con.execute(sql).fetchdf()
            print(r.to_string() if not r.empty else "ℹ️  Empty")
        except Exception as e:
            print(f"❌ {e}")

    def raw_sql(self, dtype, db, query, user=None, pwd=None, host="localhost", port=None):
        if not SQLALCHEMY_AVAILABLE:
            print("❌ SQLAlchemy not installed")
            return
        try:
            if dtype == 'mysql':
                eng = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:{port or 3306}/{db}")
            elif dtype == 'postgresql':
                eng = create_engine(f"postgresql+pg8000://{user}:{pwd}@{host}:{port or 5432}/{db}")
            elif dtype == 'sqlite':
                eng = create_engine(f"sqlite:///{db}")
            else:
                print("❌ Unsupported")
                return
            print(pd.read_sql_query(query, eng).to_string())
        except Exception as e:
            print(f"❌ {e}")

    # ============================================================
    # EXPORT
    # ============================================================
    def save(self, fmt, filename):
        if self.df is None:
            print("❌ No data")
            return
        try:
            parent = Path(filename).parent
            if str(parent) != ".":
                parent.mkdir(parents=True, exist_ok=True)
            fmt = fmt.lower()
            if self._has_duckdb() and fmt in ('csv', 'parquet'):
                safe_out = self._escape_sql_path(filename)
                if fmt == 'csv':
                    self.duck_con.execute(
                        f"COPY {self.duck_table} TO '{safe_out}' (HEADER true, DELIMITER ',')"
                    )
                else:
                    if not PYARROW_AVAILABLE:
                        print("❌ pyarrow needed")
                        return
                    self.duck_con.execute(
                        f"COPY {self.duck_table} TO '{safe_out}' (FORMAT PARQUET)"
                    )
                print(f"✅ Full saved: {filename}")
                return
            if fmt in ('excel', 'xlsx'):
                if not OPENPYXL_AVAILABLE:
                    print("❌ openpyxl required")
                    return
                self.df.to_excel(filename, index=False, engine='openpyxl')
            elif fmt == 'csv':
                self.df.to_csv(filename, index=False)
            elif fmt == 'json':
                self.df.to_json(filename, orient='records', indent=2)
            elif fmt == 'parquet':
                if not PYARROW_AVAILABLE:
                    print("❌ pyarrow needed")
                    return
                self.df.to_parquet(filename, index=False)
            else:
                print("❌ csv/excel/xlsx/json/parquet only")
                return
            print(f"✅ Saved: {filename}")
        except Exception as e:
            print(f"❌ {e}")

    def report(self, fname="report.pdf"):
        if self.df is None:
            print("❌ No data")
            return
        if not self.insights:
            self.auto_analyze()
        try:
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 16)
            pdf.cell(0, 10, f"DataVortex Report - {self.data_name}", ln=True, align="C")
            pdf.set_font("Helvetica", "", 10)
            pdf.cell(0, 8, f"Generated: {datetime.now()}", ln=True, align="C")
            pdf.cell(0, 8, f"Creator: {CREATOR} | {CREATOR_EMAIL}", ln=True, align="C")
            pdf.ln(10)
            pdf.set_font("Helvetica", "B", 12)
            pdf.cell(0, 8, "Insights", ln=True)
            pdf.set_font("Helvetica", "", 10)
            for ins in self.insights:
                pdf.multi_cell(0, 6, str(ins).encode('ascii', errors='replace').decode())
            pdf.set_font("Helvetica", "I", 8)
            pdf.ln(5)
            pdf.cell(0, 6, COPYRIGHT, ln=True, align="C")
            pdf.output(fname)
            print(f"✅ Report: {fname}")
        except Exception as e:
            print(f"❌ {e}")

    def auto_report(self, fname="auto_report.pdf"):
        self.auto_analyze()
        self.report(fname)

    def power_report(self, fname="power_report.pdf"):
        self.auto_analyze()
        self.report(fname)

    def sales_report(self, fname="sales_report.pdf"):
        if self.df is None:
            print("❌ No data")
            return
        rc = next((c for c in self.df.columns
                   if any(k in c.lower() for k in ['revenue', 'sales', 'amount', 'total'])), None)
        ins = []
        if rc:
            ins.append(f"Total Revenue: {self.df[rc].sum():,.2f}")
        try:
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 16)
            pdf.cell(0, 10, "Sales Report", ln=True, align="C")
            pdf.set_font("Helvetica", "", 10)
            pdf.ln(10)
            for i in ins:
                pdf.multi_cell(0, 6, i)
            pdf.set_font("Helvetica", "I", 8)
            pdf.cell(0, 6, COPYRIGHT, ln=True, align="C")
            pdf.output(fname)
            print(f"✅ Sales report: {fname}")
        except Exception as e:
            print(f"❌ {e}")

    def deep_analyze(self):
        if self.df is None:
            print("❌ No data")
            return
        rep = ["🔍 DEEP ANALYSIS", "=" * 60]
        if self._has_duckdb():
            try:
                total = self.duck_con.execute(f"SELECT COUNT(*) FROM {self.duck_table}").fetchone()[0]
                rep.append(f"Total rows (FULL): {total:,}")
                schema = self.duck_con.execute(f"DESCRIBE {self.duck_table}").fetchdf()
                rep.append(f"Columns: {len(schema)}")
                miss_cols = {}
                for _, r in schema.iterrows():
                    safe = self._safe_col(r['column_name'])
                    cnt = self.duck_con.execute(
                        f'SELECT COUNT(*) FROM {self.duck_table} WHERE "{safe}" IS NULL'
                    ).fetchone()[0]
                    if cnt > 0:
                        miss_cols[r['column_name']] = cnt
                if miss_cols:
                    rep.append("\n⚠️  Missing:")
                    for c, n in miss_cols.items():
                        rep.append(f"   {c}: {n:,}")
                else:
                    rep.append("\n✅ No missing")
                score = 100 - min(30, len(miss_cols) * 3)
                rep.append(f"\n🎯 Quality: {score}/100")
            except Exception as e:
                rep.append(f"⚠️  {e}")
        else:
            df = self.df
            rep.append(f"Rows: {len(df):,}, Cols: {len(df.columns)}")
            miss = df.isnull().sum()
            mc = miss[miss > 0]
            if len(mc) > 0:
                rep.append("\n⚠️  Missing:")
                for c, n in mc.items():
                    rep.append(f"   {c}: {n:,}")
            else:
                rep.append("\n✅ No missing")
            score = 100 - min(30, len(mc) * 3)
            rep.append(f"\n🎯 Quality: {score}/100")
        self.deep_report_lines = rep
        for line in rep:
            print(line)

    def deep_report(self, fname="deep_report.pdf"):
        if not self.deep_report_lines:
            self.deep_analyze()
        try:
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 16)
            pdf.cell(0, 10, "Deep Analysis Report", ln=True, align="C")
            pdf.set_font("Helvetica", "", 10)
            pdf.cell(0, 8, f"Creator: {CREATOR}", ln=True, align="C")
            pdf.ln(5)
            for line in self.deep_report_lines:
                pdf.multi_cell(0, 6, str(line).encode('ascii', errors='replace').decode())
            pdf.set_font("Helvetica", "I", 8)
            pdf.cell(0, 6, COPYRIGHT, ln=True, align="C")
            pdf.output(fname)
            print(f"✅ Deep report: {fname}")
        except Exception as e:
            print(f"❌ {e}")

    def kpi(self):
        if self.df is None:
            print("❌ No data")
            return
        print("\n📊 KPIs:")
        if self._has_duckdb():
            try:
                schema = self.duck_con.execute(f"DESCRIBE {self.duck_table}").fetchdf()
                for _, r in schema.iterrows():
                    cname = r['column_name']
                    ctype = str(r['column_type']).upper()
                    if any(t in ctype for t in ['BIGINT', 'INTEGER', 'DOUBLE', 'FLOAT', 'DECIMAL', 'REAL', 'NUMERIC']):
                        safe = self._safe_col(cname)
                        try:
                            st = self.duck_con.execute(
                                f'SELECT SUM("{safe}") AS t, AVG("{safe}") AS a, '
                                f'MIN("{safe}") AS mn, MAX("{safe}") AS mx FROM {self.duck_table}'
                            ).fetchdf().iloc[0]
                            print(f"   {cname}: Total={st['t']:,.2f}, Avg={st['a']:,.2f}, "
                                  f"Min={st['mn']:,.2f}, Max={st['mx']:,.2f}")
                        except Exception:
                            pass
                return
            except Exception:
                pass
        for c in self.df.select_dtypes(include=[np.number]).columns:
            print(f"   {c}: Total={self.df[c].sum():,.2f}, Avg={self.df[c].mean():,.2f}, "
                  f"Min={self.df[c].min():,.2f}, Max={self.df[c].max():,.2f}")

    def auto_analyze(self):
        if self.df is None:
            print("❌ No data")
            return
        print("\n🤖 AUTO-ANALYSIS:")
        self.insights = []
        if self._has_duckdb():
            try:
                total = self.duck_con.execute(f"SELECT COUNT(*) FROM {self.duck_table}").fetchone()[0]
                self.insights.append(f"Total rows: {total:,}")
                schema = self.duck_con.execute(f"DESCRIBE {self.duck_table}").fetchdf()
                for _, r in schema.iterrows():
                    cname = r['column_name']
                    ctype = str(r['column_type']).upper()
                    if any(t in ctype for t in ['BIGINT', 'INTEGER', 'DOUBLE', 'FLOAT', 'DECIMAL', 'REAL', 'NUMERIC']):
                        safe = self._safe_col(cname)
                        try:
                            st = self.duck_con.execute(
                                f'SELECT AVG("{safe}") AS m, MIN("{safe}") AS mn, MAX("{safe}") AS mx '
                                f'FROM {self.duck_table}').fetchdf().iloc[0]
                            self.insights.append(f"{cname}: mean={st['m']:,.2f}, "
                                                f"min={st['mn']:,.2f}, max={st['mx']:,.2f}")
                        except Exception:
                            pass
            except Exception:
                pass
        else:
            df = self.df
            self.insights.append(f"Rows: {len(df):,}, Columns: {len(df.columns)}")
            for c in df.select_dtypes(include=[np.number]).columns:
                self.insights.append(f"{c}: mean={df[c].mean():,.2f}, "
                                    f"min={df[c].min():,.2f}, max={df[c].max():,.2f}")
        for ins in self.insights:
            print(f"🔹 {ins}")

    def ai_summary(self):
        if self.df is None:
            print("❌ No data")
            return
        print("\n" + "═" * 78)
        print("  🤖 DATA VORTEX – AI REPORT")
        print("═" * 78)
        use_full = self._has_duckdb()
        nums_db, cats_db, col_names = [], [], []
        total, cols = 0, 0
        if use_full:
            try:
                total = self.duck_con.execute(f"SELECT COUNT(*) FROM {self.duck_table}").fetchone()[0]
                cq = self.duck_con.execute(f"DESCRIBE {self.duck_table}").fetchdf()
                cols = len(cq)
                col_names = [r['column_name'] for _, r in cq.iterrows()]
                nt = ['BIGINT', 'INTEGER', 'DOUBLE', 'FLOAT', 'DECIMAL', 'SMALLINT',
                      'TINYINT', 'HUGEINT', 'REAL', 'NUMERIC']
                nums_db = [r['column_name'] for _, r in cq.iterrows()
                           if any(t in str(r['column_type']).upper() for t in nt)]
                cats_db = [r['column_name'] for _, r in cq.iterrows()
                           if 'CHAR' in str(r['column_type']).upper() or
                              'VARCHAR' in str(r['column_type']).upper() or
                              'TEXT' in str(r['column_type']).upper()]
            except Exception:
                use_full = False
        if not use_full:
            df = self.df
            total = len(df)
            cols = len(df.columns)
            col_names = list(df.columns)
            nums_db = df.select_dtypes(include=[np.number]).columns.tolist()
            cats_db = df.select_dtypes(include=['object', 'category']).columns.tolist()

        print(f"\n📋 1. OVERVIEW")
        print("─" * 78)
        print(f"   File      : {self.data_name}")
        print(f"   Rows      : {total:,}")
        print(f"   Columns   : {cols}")
        print(f"   Engine    : {self.data_engine.upper()}")

        print(f"\n📊 2. STRUCTURE")
        print("─" * 78)
        print(f"   Numeric     : {len(nums_db)}")
        print(f"   Categorical : {len(cats_db)}")

        print(f"\n🚨 3. QUALITY")
        print("─" * 78)
        missing_cols = {}
        dup_count = 0
        if use_full:
            try:
                for c in col_names:
                    safe = self._safe_col(c)
                    cnt = self.duck_con.execute(
                        f'SELECT COUNT(*) FROM {self.duck_table} WHERE "{safe}" IS NULL'
                    ).fetchone()[0]
                    if cnt > 0:
                        missing_cols[c] = cnt
                if missing_cols:
                    print(f"   ⚠️  Missing in {len(missing_cols)} cols:")
                    for c, n in list(missing_cols.items())[:10]:
                        pct = n / total * 100
                        print(f"      • {c}: {n:,} ({pct:.2f}%)")
                else:
                    print(f"   ✅ No missing")
                distinct = self.duck_con.execute(
                    f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {self.duck_table})"
                ).fetchone()[0]
                dup_count = total - distinct
                print(f"   {'⚠️  Duplicates: ' + f'{dup_count:,}' if dup_count > 0 else '✅ No duplicates'}")
            except Exception as e:
                print(f"   ⚠️  {e}")

        print(f"\n🔢 4. NUMERIC ANALYSIS")
        print("─" * 78)
        if use_full and nums_db:
            for c in nums_db[:5]:
                try:
                    safe = self._safe_col(c)
                    r = self.duck_con.execute(
                        f'SELECT MIN("{safe}") AS mn, MAX("{safe}") AS mx, '
                        f'AVG("{safe}") AS avg, MEDIAN("{safe}") AS med FROM {self.duck_table}'
                    ).fetchdf().iloc[0]
                    print(f"\n   📌 {c}:")
                    print(f"      Min={r['mn']:,.2f}, Max={r['mx']:,.2f}")
                    print(f"      Mean={r['avg']:,.2f}, Median={r['med']:,.2f}")
                except Exception:
                    pass

        print(f"\n🏷️  5. CATEGORICAL")
        print("─" * 78)
        if use_full and cats_db:
            for c in cats_db[:5]:
                try:
                    safe = self._safe_col(c)
                    uniq = self.duck_con.execute(
                        f'SELECT COUNT(DISTINCT "{safe}") FROM {self.duck_table}'
                    ).fetchone()[0]
                    top = self.duck_con.execute(
                        f'SELECT "{safe}", COUNT(*) AS cnt FROM {self.duck_table} '
                        f'GROUP BY "{safe}" ORDER BY cnt DESC LIMIT 3'
                    ).fetchdf()
                    print(f"\n   📌 {c}: ({uniq:,} unique)")
                    for _, row in top.iterrows():
                        pct = row['cnt'] / total * 100
                        bar = "█" * int(pct / 3)
                        print(f"      {bar} {str(row[c])[:20]}: {row['cnt']:,} ({pct:.1f}%)")
                except Exception:
                    pass

        print(f"\n" + "═" * 78)
        print(f"  🏁 VERDICT")
        print("═" * 78)
        score = 100
        if len(missing_cols) > 0:
            score -= min(30, len(missing_cols) * 3)
        if dup_count > 0:
            score -= min(20, dup_count / max(total, 1) * 100)
        score = max(0, int(score))
        print(f"\n   Quality Score: {score}/100")
        icon = "🌟 EXCELLENT" if score >= 90 else "✅ GOOD" if score >= 75 else "🟡 FAIR"
        print(f"   {icon}")
        print(f"\n   👑 {CREATOR} | 📧 {CREATOR_EMAIL}")
        print(f"   {COPYRIGHT}")
        print("═" * 78)

    def enterprise_report(self, filename="enterprise_report.pdf"):
        if self.df is None:
            print("❌ No data")
            return
        try:
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 20)
            pdf.cell(0, 12, "ENTERPRISE DATA REPORT", ln=True, align="C")
            pdf.set_font("Helvetica", "", 11)
            pdf.cell(0, 8, f"Dataset: {self.data_name}", ln=True, align="C")
            pdf.cell(0, 8, f"Generated: {datetime.now()}", ln=True, align="C")
            pdf.ln(10)
            if self._has_duckdb():
                total = self.duck_con.execute(f"SELECT COUNT(*) FROM {self.duck_table}").fetchone()[0]
                cols = len(self.duck_con.execute(f"DESCRIBE {self.duck_table}").fetchdf())
            else:
                total = len(self.df)
                cols = len(self.df.columns)
            pdf.set_font("Helvetica", "B", 14)
            pdf.cell(0, 10, "OVERVIEW", ln=True)
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(0, 6, f"Rows: {total:,}\nColumns: {cols}\nEngine: {self.data_engine.upper()}")
            pdf.set_font("Helvetica", "I", 8)
            pdf.ln(5)
            pdf.cell(0, 6, COPYRIGHT, ln=True, align="C")
            pdf.output(filename)
            print(f"✅ Enterprise report: {filename}")
        except Exception as e:
            print(f"❌ {e}")

    # ============================================================
    # NO-API KILLERS (FIX #1: previously missing methods)
    # ============================================================
    def ask_offline(self, question):
        """Offline natural-language questions"""
        if self.df is None:
            print("❌ No data loaded")
            return
        q = question.lower()
        print(f"\n🤖 Offline Answer for: '{question}'")
        print("─" * 60)
        cols = self.df.columns.tolist()
        nums = self.df.select_dtypes(include=[np.number]).columns.tolist()
        cats = self.df.select_dtypes(include=['object', 'category']).columns.tolist()

        if any(k in q for k in ['how many', 'count', 'rows', 'records']):
            print(f"📊 Total rows: {len(self.df):,}")
            print(f"📊 Total columns: {len(cols)}")

        elif 'average' in q or 'mean' in q:
            target = next((c for c in nums if c.lower() in q), None)
            if target:
                print(f"📈 Mean of '{target}': {self.df[target].mean():,.2f}")
            else:
                for c in nums[:5]:
                    print(f"   {c}: mean={self.df[c].mean():,.2f}")

        elif 'max' in q or 'maximum' in q or 'highest' in q:
            target = next((c for c in nums if c.lower() in q), None)
            if target:
                print(f"📈 Max of '{target}': {self.df[target].max():,.2f}")
            else:
                print("❌ Specify a column")

        elif 'min' in q or 'minimum' in q or 'lowest' in q:
            target = next((c for c in nums if c.lower() in q), None)
            if target:
                print(f"📉 Min of '{target}': {self.df[target].min():,.2f}")

        elif 'top' in q:
            target = next((c for c in nums if c.lower() in q), None)
            grp = cats[0] if cats else None
            if target and grp:
                print(self.df.groupby(grp)[target].sum().nlargest(5))
            elif target:
                print(self.df.nlargest(5, target)[[target]])

        elif 'missing' in q or 'null' in q or 'empty' in q:
            miss = self.df.isnull().sum()
            miss = miss[miss > 0]
            if len(miss) == 0:
                print("✅ No missing values")
            else:
                print(miss.to_string())

        elif 'unique' in q or 'distinct' in q:
            target = next((c for c in cols if c.lower() in q), None)
            if target:
                print(f"Unique in '{target}': {self.df[target].nunique():,}")
                print(self.df[target].unique()[:20])

        elif any(k in q for k in ['summary', 'info', 'stats', 'describe']):
            print(self.df.describe().to_string())
            print(f"\nColumns: {cols}")

        elif 'quality' in q or 'health' in q:
            score = 100 - int((self.df.isnull().sum().sum() / max(self.df.size, 1)) * 100)
            print(f"🎯 Quality Score: {score}/100")

        elif 'correlat' in q:
            if len(nums) >= 2:
                print(self.df[nums].corr().to_string())

        else:
            target = next((c for c in cols if c.lower() in q), None)
            if target:
                print(f"📌 Column '{target}' info:")
                print(f"   Type: {self.df[target].dtype}")
                print(f"   Unique: {self.df[target].nunique():,}")
                print(f"   Missing: {self.df[target].isnull().sum():,}")
                if pd.api.types.is_numeric_dtype(self.df[target]):
                    print(f"   Mean: {self.df[target].mean():,.2f}")
                    print(f"   Min: {self.df[target].min():,.2f}")
                    print(f"   Max: {self.df[target].max():,.2f}")
            else:
                print("💡 Try: 'how many rows', 'average of <col>', "
                      "'top 5 <col>', 'missing', 'summary', 'unique <col>'")

    def root_cause(self, target_col, compare_col=None):
        """Find root causes / drivers of a numeric column"""
        if self.df is None or target_col not in self.df.columns:
            print(f"❌ '{target_col}' not found")
            return
        if not np.issubdtype(self.df[target_col].dtype, np.number):
            print(f"❌ '{target_col}' must be numeric")
            return

        print(f"\n🔍 ROOT CAUSE ANALYSIS: {target_col}")
        print("═" * 70)

        nums = [c for c in self.df.select_dtypes(include=[np.number]).columns
                if c != target_col]
        cats = self.df.select_dtypes(include=['object', 'category']).columns.tolist()
        corr = None

        if nums:
            print("\n📊 Correlated Numeric Columns:")
            corr = self.df[nums + [target_col]].corr()[target_col].drop(target_col)
            corr_abs = corr.abs().sort_values(ascending=False)
            for c, v in corr_abs.head(5).items():
                arrow = "⬆️" if corr[c] > 0 else "⬇️"
                print(f"   {arrow} {c}: |r| = {v:.4f}")

        if cats and compare_col is None:
            compare_col = cats[0]

        if compare_col and compare_col in self.df.columns:
            print(f"\n🏷️  Variance by '{compare_col}':")
            grp = self.df.groupby(compare_col)[target_col].agg(['mean', 'count'])
            grp = grp[grp['count'] >= 5].sort_values('mean', ascending=False)
            overall_mean = self.df[target_col].mean()
            print(f"   Overall mean: {overall_mean:,.2f}")
            for idx, row in grp.head(10).iterrows():
                diff = row['mean'] - overall_mean
                pct = (diff / overall_mean * 100) if overall_mean != 0 else 0
                marker = "🟢" if pct > 5 else "🔴" if pct < -5 else "⚪"
                print(f"   {marker} {str(idx)[:25]}: {row['mean']:,.2f} "
                      f"({pct:+.1f}%, n={int(row['count'])})")

        print("\n" + "─" * 70)
        print("🎯 VERDICT:")
        if corr is not None and len(corr) > 0:
            top_corr = corr.abs().idxmax()
            top_val = corr.abs().max()
            if top_val > 0.7:
                print(f"   💪 '{top_corr}' is the STRONGEST driver of '{target_col}'")
            elif top_val > 0.4:
                print(f"   📊 '{top_corr}' is a MODERATE driver of '{target_col}'")
            else:
                print(f"   ⚠️  No strong numeric driver found")

    def export_pipeline(self, filename="pipeline_api.py"):
        """Export a standalone Python pipeline script"""
        if self.df is None:
            print("❌ No data")
            return
        cols = list(self.df.columns)
        nums = self.df.select_dtypes(include=[np.number]).columns.tolist()
        cats = self.df.select_dtypes(include=['object']).columns.tolist()

        script = f'''#!/usr/bin/env python3
"""
Auto-generated pipeline by DataVortex v26.2
Creator: {CREATOR} | {CREATOR_EMAIL}
Generated: {datetime.now()}
"""

import pandas as pd
import numpy as np
import sys

def load_data(path):
    """Load and preprocess data"""
    df = pd.read_csv(path)
    print(f"✅ Loaded {{len(df):,}} rows × {{len(df.columns)}} cols")
    return df

def clean_data(df):
    """Standard cleaning pipeline"""
    before = len(df)
    df = df.drop_duplicates()
    print(f"🗑️  Removed {{before - len(df):,}} duplicates")
    for c in df.columns:
        if df[c].isnull().sum() > 0:
            if pd.api.types.is_numeric_dtype(df[c]):
                df[c] = df[c].fillna(df[c].median())
            else:
                m = df[c].mode()
                df[c] = df[c].fillna(m[0] if not m.empty else 'Unknown')
    return df

def analyze(df):
    """Basic analysis"""
    print("\\n📊 ANALYSIS")
    print("─" * 60)
    print(f"Rows: {{len(df):,}}")
    print(f"Columns: {{list(df.columns)}}")
    print(f"\\nNumeric columns: {nums}")
    print(f"Categorical columns: {cats}")
    print("\\nStats:")
    print(df.describe().to_string())
    return df

def main():
    if len(sys.argv) < 2:
        print("Usage: python pipeline_api.py <data.csv>")
        sys.exit(1)
    df = load_data(sys.argv[1])
    df = clean_data(df)
    df = analyze(df)
    df.to_csv("cleaned_output.csv", index=False)
    print(f"\\n✅ Saved: cleaned_output.csv")

if __name__ == "__main__":
    main()
'''
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(script)
            print(f"✅ Pipeline exported: {filename}")
            print(f"   Run: python {filename} <data.csv>")
        except Exception as e:
            print(f"❌ {e}")

    def full_report(self, target=None, steps=10):
        """One-shot comprehensive report"""
        if self.df is None:
            print("❌ No data")
            return
        print("\n" + "╔" + "═" * 76 + "╗")
        print("║" + " " * 20 + "🔬 FULL COMPREHENSIVE REPORT" + " " * 28 + "║")
        print("╚" + "═" * 76 + "╝")

        self.deep_analyze()
        print()
        self.auto_analyze()
        print()
        self.kpi()
        print()

        nums = self.df.select_dtypes(include=[np.number]).columns.tolist()
        if nums and target is None:
            target = nums[0]
        if target and target in self.df.columns:
            print()
            try:
                self.forecast(target, steps)
            except Exception as e:
                print(f"⚠️  Forecast skipped: {e}")

        print(f"\n✅ Full report complete | {CREATOR} | {CREATOR_EMAIL}")

    # ============================================================
    # PIPELINE
    # ============================================================
    def auto_pilot(self):
        """FIX #15: Disable confirm prompts during auto-pilot"""
        if self.df is None:
            print("❌ No data")
            return
        print("⚡ AUTO-PILOT...")
        old_safe = self.safe_mode
        self.safe_mode = False
        try:
            self.smart_clean()
            self.deep_analyze()
            self.auto_analyze()
            self.ai_summary()
            print("\n✅ Auto-Pilot complete!")
        finally:
            self.safe_mode = old_safe

    def run_pipeline(self, data_file, report_file=None):
        """FIX #15: Disable confirm prompts during pipeline"""
        old_safe = self.safe_mode
        self.safe_mode = False
        try:
            self.load_smart(data_file)
            self.smart_clean()
            self.deep_analyze()
            self.auto_analyze()
            self.ai_summary()
            if report_file:
                self.enterprise_report(report_file)
            print("\n✅ Pipeline complete")
        finally:
            self.safe_mode = old_safe

    def schedule_job(self, command, time_str):
        if not SCHEDULER_AVAILABLE or scheduler is None:
            print("❌ Scheduler unavailable")
            return
        try:
            h, m = time_str.split(":")
            scheduler.add_job(lambda: print(f"Run: {command}"),
                             'cron', hour=int(h), minute=int(m))
            print(f"✅ Scheduled: {command} @ {time_str}")
        except Exception as e:
            print(f"❌ {e}")

    def start_scheduler(self):
        if scheduler:
            try:
                scheduler.start()
                print("✅ Scheduler ON")
            except Exception as e:
                print(f"❌ {e}")

    def stop_scheduler(self):
        if scheduler:
            try:
                scheduler.shutdown(wait=False)
                print("✅ Scheduler OFF")
            except Exception:
                pass

    def jupyter(self):
        try:
            subprocess.Popen(["jupyter", "lab"])
            print("✅ JupyterLab launched")
        except Exception:
            print("❌ pip install jupyterlab")

    def web(self, port=5000):
        print(f"💡 pip install flask")

    def clear(self):
        if not self.confirm("Clear all data?"):
            return
        self.auto_backup()
        self._clear_engine()
        self.df = None
        self.insights = []
        self.deep_report_lines = []
        print("✅ Cleared")

    def cleanup(self):
        try:
            if scheduler and getattr(scheduler, 'running', False):
                scheduler.shutdown(wait=False)
        except Exception:
            pass
        try:
            if self._sqlite_con:
                self._sqlite_con.close()
        except Exception:
            pass
        self._clear_engine()
        gc.collect()

    def creator_menu(self):
        print(f"\n--- 👑 CREATOR MENU ---")
        print(f"Welcome, {CREATOR}!")
        print(f"1. View backups")
        print(f"2. About")
        print(f"0. Back")
        try:
            ch = input("Choice: ").strip()
            if ch == '1':
                self.backups_list()
            elif ch == '2':
                self.about()
        except Exception:
            pass

    # ============================================================
    # MAIN RUN LOOP
    # ============================================================
    def run(self):
        self.print_header()

        if os.getenv('DV_SKIP_AUTH') != '1':
            try:
                if input("\n🔑 Activate Creator Power? (y/n): ").strip().lower() == 'y':
                    if input("Password: ").strip() == CREATOR_PASSWORD:
                        print(f"✅ Welcome, {CREATOR}!")
                    else:
                        print("❌ Wrong password")
            except Exception:
                pass

        while True:
            try:
                cmd = input("\nDataVortex> ").strip()
                if not cmd:
                    continue
                p = cmd.split()
                a = p[0].lower()
                args = p[1:]

                # LOAD
                if a == "load_smart" and args:
                    self.load_smart(" ".join(args))
                elif a == "load" and args:
                    self.load_smart(" ".join(args))
                elif a == "load_pandas" and args:
                    self._load_pandas(" ".join(args))
                    self.data_engine = "pandas"
                elif a == "load_polars" and args:
                    self._load_polars(" ".join(args))
                    self.data_engine = "polars"
                elif a == "load_duckdb" and args:
                    self._load_duckdb(" ".join(args))
                    self.data_engine = "duckdb"
                elif a == "load_dask" and args:
                    self._load_dask(" ".join(args))
                    self.data_engine = "dask"
                elif a == "load_chunk" and args:
                    self._load_chunk(" ".join(args))
                    self.data_engine = "pandas_chunk"
                elif a == "load_fast" and args:
                    self._load_polars(" ".join(args))
                elif a == "load_url" and args:
                    self.load_url(args[0])
                elif a == "nasa" and args:
                    self.nasa(args[0])
                elif a == "load_db" and len(args) >= 2:
                    self.load_db(
                        args[0], args[1],
                        args[2] if len(args) > 2 else None,
                        args[3] if len(args) > 3 else None,
                        args[4] if len(args) > 4 else "localhost",
                        int(args[5]) if len(args) > 5 else None,
                        " ".join(args[6:]) if len(args) > 6 else f"SELECT * FROM {args[1]}"
                    )
                elif a == "session_save":
                    self.session_save(args[0] if args else "session.pkl")
                elif a == "session_load":
                    self.session_load(args[0] if args else "session.pkl")
                elif a == "save_parquet":
                    self.save_parquet(args[0] if args else "data.parquet")
                elif a == "optimize_storage" and args:
                    self.optimize_storage(args[0])
                elif a == "query_duckdb" and args:
                    self.query_duckdb(" ".join(args))

                # SAFETY
                elif a == "safe_mode_on":
                    self.safe_mode = True
                    print("🛡️  ON")
                elif a == "safe_mode_off":
                    self.safe_mode = False
                    print("⚠️  OFF")
                elif a == "backup":
                    self.auto_backup()
                elif a == "rollback":
                    self.rollback(args[0] if args else None)
                elif a == "backups_list":
                    self.backups_list()
                elif a == "clean_backups":
                    self.clean_backups()
                elif a == "integrity_check":
                    self.integrity_check()
                elif a == "quality_check":
                    self.quality_check()
                elif a == "memory_check":
                    self.memory_check()
                elif a == "auto_type_fix":
                    self.auto_type_fix()
                elif a == "audit_log":
                    if os.path.exists("audit.log"):
                        print(open("audit.log").read())
                    else:
                        print("No log")
                elif a == "optimize_dtypes":
                    self.optimize_dtypes()
                elif a == "free_memory":
                    self.free_memory()
                elif a == "sample_data":
                    self.sample_data(int(args[0]) if args else 100000)

                # KILLERS
                elif a == "ask_offline" and args:
                    self.ask_offline(" ".join(args))
                elif a == "root_cause" and args:
                    self.root_cause(args[0], args[1] if len(args) > 1 else None)
                elif a == "export_pipeline":
                    self.export_pipeline(args[0] if args else "pipeline_api.py")
                elif a == "full_report":
                    self.full_report(args[0] if args else None,
                                     int(args[1]) if len(args) > 1 else 10)

                # BASIC
                elif a == "show_columns":
                    self.show_columns()
                elif a == "head":
                    self.head(int(args[0]) if args else 5)
                elif a == "tail":
                    self.tail(int(args[0]) if args else 5)
                elif a == "info":
                    self.info()
                elif a == "describe":
                    self.describe()
                elif a == "unique" and args:
                    self.unique(args[0])

                # CLEANING
                elif a == "clean":
                    self.clean()
                elif a == "smart_clean":
                    self.smart_clean()
                elif a == "fix_columns":
                    self.fix_columns()
                elif a == "dropna":
                    self.dropna()
                elif a == "fillna" and args:
                    self.fillna(args[0])
                elif a == "filter" and len(args) >= 3:
                    self.filter(args[0], args[1], args[2])

                # TRANSFORM
                elif a == "pivot" and len(args) >= 3:
                    self.pivot_data(args[0], args[1], args[2],
                                    args[3] if len(args) > 3 else 'sum')
                elif a == "melt" and len(args) >= 2:
                    self.melt_data(args[0], args[1])
                elif a == "merge" and len(args) >= 3:
                    self.merge_data(args[0], args[1], args[2],
                                    args[3] if len(args) > 3 else 'inner')
                elif a == "groupby" and args:
                    self.groupby(args[0], args[1] if len(args) > 1 else 'sum')

                # VIZ
                elif a == "plot" and len(args) >= 2:
                    self.plot(args[0], args[1], args[2] if len(args) > 2 else None)
                elif a == "plotly" and len(args) >= 2:
                    self.plotly_viz(args[0], args[1], args[2] if len(args) > 2 else None)
                elif a == "heatmap":
                    self.heatmap()
                elif a == "bi":
                    self.bi()
                elif a == "ultra_dashboard":
                    self.ultra_dashboard(args[0] if args else "ultra_dashboard.pdf")
                elif a == "mini_dashboard":
                    self.mini_dashboard(args[0] if args else "mini_dashboard.png")
                elif a == "dashboard":
                    self.ultra_dashboard(args[0] if args else "dashboard.pdf")

                # ML
                elif a == "predict" and len(args) >= 2:
                    self.predict(args[0], " ".join(args[1:]))
                elif a == "rf_predict" and len(args) >= 2:
                    self.rf_predict(args[0], " ".join(args[1:]))
                elif a == "gb_predict" and len(args) >= 2:
                    self.gb_predict(args[0], " ".join(args[1:]))
                elif a == "cluster" and args:
                    self.cluster(" ".join(args))
                elif a == "pca" and args:
                    self.pca(" ".join(args))
                elif a == "cv" and len(args) >= 2:
                    self.cv(args[0], " ".join(args[1:]))
                elif a == "detect_features":
                    self.detect_features(args[0] if args else None)

                # TIME SERIES
                elif a == "forecast" and args:
                    self.forecast(args[0], int(args[1]) if len(args) > 1 else 10)
                elif a == "smooth" and args:
                    self.smooth(args[0], int(args[1]) if len(args) > 1 else 10)
                elif a == "anomaly" and args:
                    self.anomaly(args[0])
                elif a == "arima" and args:
                    self.arima(args[0], int(args[1]) if len(args) > 1 else 10)
                elif a == "decompose" and args:
                    self.decompose(args[0])

                # STATS
                elif a == "ttest" and args:
                    if len(args) >= 4:
                        self.ttest(args[0], gc=args[1], g1=args[2], g2=args[3])
                    elif len(args) >= 2:
                        self.ttest(args[0], c2=args[1])
                    else:
                        self.ttest(args[0])
                elif a == "anova" and len(args) >= 2:
                    self.anova(args[0], args[1])
                elif a == "chisq" and len(args) >= 2:
                    self.chisq(args[0], args[1])

                # SQL
                elif a == "sql" and args:
                    self.sql(" ".join(args))
                elif a == "raw_sql" and len(args) >= 3:
                    self.raw_sql(args[0], args[1], " ".join(args[2:]))

                # EXPORT
                elif a == "save" and len(args) >= 2:
                    self.save(args[0], args[1])
                elif a == "report":
                    self.report(args[0] if args else "report.pdf")
                elif a == "auto_report":
                    self.auto_report(args[0] if args else "auto_report.pdf")
                elif a == "power_report":
                    self.power_report(args[0] if args else "power_report.pdf")
                elif a == "sales_report":
                    self.sales_report(args[0] if args else "sales_report.pdf")
                elif a == "deep_analyze":
                    self.deep_analyze()
                elif a == "deep_report":
                    self.deep_report(args[0] if args else "deep_report.pdf")
                elif a == "ai_summary" or a == "ai":
                    self.ai_summary()
                elif a == "enterprise_report":
                    self.enterprise_report(args[0] if args else "enterprise_report.pdf")
                elif a == "kpi":
                    self.kpi()

                # PIPELINE
                elif a == "auto_pilot":
                    self.auto_pilot()
                elif a == "run_pipeline" and args:
                    self.run_pipeline(args[0],
                                     args[1] if len(args) > 1 else None)
                elif a == "schedule" and len(args) >= 2:
                    self.schedule_job(" ".join(args[:-1]), args[-1])
                elif a == "start_scheduler":
                    self.start_scheduler()
                elif a == "stop_scheduler":
                    self.stop_scheduler()
                elif a == "jupyter":
                    self.jupyter()
                elif a == "web":
                    self.web(int(args[0]) if args else 5000)

                # OTHER
                elif a == "help":
                    self.help()
                elif a == "about":
                    self.about()
                elif a == "creator":
                    self.creator_menu()
                elif a == "clear":
                    self.clear()
                elif a == "exit":
                    if self.confirm("Exit DataVortex?"):
                        print(f"\n👋 നന്ദി! - {CREATOR}")
                        print(f"📧 {CREATOR_EMAIL}")
                        print(f"📍 {CREATOR_LOCATION}")
                        print(f"{COPYRIGHT}")
                        self.cleanup()
                        break
                else:
                    print("❌ Unknown. Type 'help'")

            except KeyboardInterrupt:
                print("\n⚠️  Type 'exit'")
            except Exception as e:
                print(f"❌ {type(e).__name__}: {e}")


if __name__ == "__main__":
    DataVortex().run()
