"""
E-commerce ETL Pipeline
Extracts, cleans, validates and analyses e-commerce data from PostgreSQL.

Author: Babasaheb Patil
Version: 1.0
"""

import pandas as pd
import psycopg2
from psycopg2 import pool
import logging
import sys
import os
from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


# =============================
# CONFIGURATION
# =============================

@dataclass
class Config:
    """Holds all pipeline configuration — pulled from .env so credentials never touch the code."""
    DB_NAME: str = os.getenv("DB_NAME", "ecomdb")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres")
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: str = os.getenv("DB_PORT", "5433")
    EXPORT_DIR: str = "exports"
    LOG_FILE: str = "etl_log.log"
    LOG_LEVEL: int = logging.INFO


# =============================
# LOGGING SETUP
# =============================

def setup_logging(config: Config) -> logging.Logger:
    """
    Sets up logging to both a file and the console.
    File keeps a permanent record; console gives live feedback while running.
    """
    os.makedirs(config.EXPORT_DIR, exist_ok=True)

    logger = logging.getLogger(__name__)
    logger.setLevel(config.LOG_LEVEL)

    file_handler = logging.FileHandler(config.LOG_FILE)
    file_handler.setLevel(config.LOG_LEVEL)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(config.LOG_LEVEL)

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# =============================
# DATABASE CONNECTION
# =============================

class DatabaseManager:
    """
    Manages a pool of database connections instead of opening/closing
    one per query — significantly faster for repeated DB access.
    """

    _connection_pool: Optional[pool.SimpleConnectionPool] = None

    @classmethod
    def initialize_pool(cls, config: Config, logger: logging.Logger) -> bool:
        try:
            cls._connection_pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=5,
                database=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                host=config.DB_HOST,
                port=config.DB_PORT
            )
            logger.info("Database connection pool initialized")
            return True
        except psycopg2.OperationalError as e:
            logger.error(f"Could not connect to database: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during pool setup: {e}")
            return False

    @classmethod
    def get_connection(cls, logger: logging.Logger) -> Optional[psycopg2.extensions.connection]:
        if cls._connection_pool is None:
            logger.error("Connection pool not initialized")
            return None
        try:
            return cls._connection_pool.getconn()
        except pool.PoolError as e:
            logger.error(f"Failed to get connection from pool: {e}")
            return None

    @classmethod
    def return_connection(cls, conn: psycopg2.extensions.connection,
                          logger: logging.Logger) -> None:
        if cls._connection_pool is None:
            return
        try:
            cls._connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")

    @classmethod
    def close_pool(cls, logger: logging.Logger) -> None:
        if cls._connection_pool:
            cls._connection_pool.closeall()
            logger.info("Database connection pool closed")


# =============================
# DATA FETCHING
# =============================

def fetch_clean_data(logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Pulls data from PostgreSQL into a DataFrame. Returns None if anything goes wrong."""
    conn = DatabaseManager.get_connection(logger)
    if not conn:
        return None

    query = "SELECT * FROM clean_ecommerce;"

    try:
        df = pd.read_sql(query, conn)

        if df.empty:
            logger.warning("Query returned no rows — check source table")
        else:
            logger.info(f"Data fetched: {len(df)} rows, {len(df.columns)} columns")

        return df

    except psycopg2.DatabaseError as e:
        logger.error(f"Database error while fetching: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected fetch error: {e}")
        return None
    finally:
        # always return connection even if fetch failed
        if conn:
            DatabaseManager.return_connection(conn, logger)


# =============================
# DATA CLEANING
# =============================

def clean_data(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Cleans the raw data:
    - Standardises column names
    - Removes duplicate rows
    - Fills missing values (carefully — revenue and quantity are excluded
      because filling them with 0 would silently corrupt KPI totals)
    """
    if df.empty:
        logger.warning("Received empty DataFrame — skipping cleaning")
        return df

    logger.info("Starting data cleaning")
    initial_rows = len(df)

    # lowercase + underscores so column references don't break on casing
    df.columns = df.columns.str.lower().str.replace(" ", "_", regex=False)

    dupes = df.duplicated().sum()
    df = df.drop_duplicates()
    logger.info(f"Removed {dupes} duplicate rows")

    # fill missing numeric values with 0 — but NOT revenue or quantity
    # those columns feed directly into KPIs; a missing value should stay
    # visible rather than being quietly zeroed out
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    safe_to_fill = [c for c in numeric_cols if c not in ("revenue", "quantity")]
    for col in safe_to_fill:
        df[col] = df[col].fillna(0)

    # categorical nulls become "Unknown" so groupby/aggregation doesn't drop rows
    categorical_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    for col in categorical_cols:
        df[col] = df[col].fillna("Unknown")

    logger.info(f"Cleaning done: {initial_rows} → {len(df)} rows")
    return df


# =============================
# DATA VALIDATION
# =============================

def validate_data(df: pd.DataFrame, logger: logging.Logger) -> Dict[str, Any]:
    """
    Generates a data quality snapshot — row/column counts, nulls,
    duplicates, and a check for negative revenue which would indicate
    returns or bad data that could skew KPIs downward.
    """
    logger.info("Running data validation")

    report = {
        "total_rows": df.shape[0],
        "total_columns": df.shape[1],
        "missing_values": df.isnull().sum().to_dict(),
        "duplicate_rows": int(df.duplicated().sum()),
        "data_types": df.dtypes.astype(str).to_dict(),
        "column_names": df.columns.tolist()
    }

    # warn on columns with more than 10% missing
    missing_pct = df.isnull().sum() / len(df) * 100
    high_missing = missing_pct[missing_pct > 10]
    if not high_missing.empty:
        logger.warning(f"High missing value rate detected: {high_missing.to_dict()}")

    # negative revenue = returns or data entry errors — flag them separately
    # rather than letting them silently drag down total revenue
    if "revenue" in df.columns:
        negative_revenue_count = int((df["revenue"] < 0).sum())
        report["negative_revenue_rows"] = negative_revenue_count
        if negative_revenue_count > 0:
            logger.warning(
                f"{negative_revenue_count} rows have negative revenue "
                f"— likely returns or data errors, review before reporting"
            )

    logger.info("Validation complete")
    return report


# =============================
# KPI CALCULATION
# =============================

def calculate_kpis(df: pd.DataFrame, logger: logging.Logger) -> Dict[str, Any]:
    """
    Calculates five core business KPIs.
    Includes guards for empty data and missing columns
    so the pipeline doesn't crash on bad input.
    """
    kpis = {}

    required_cols = ["revenue", "invoiceno", "customerid", "country"]
    missing_cols = [c for c in required_cols if c not in df.columns]

    if missing_cols:
        logger.warning(f"Skipping KPIs — missing columns: {missing_cols}")
        return kpis

    if df.empty:
        logger.warning("Empty DataFrame — no KPIs to calculate")
        return kpis

    try:
        # calculate total_revenue once and reuse below
        # avoids calling the same aggregation multiple times on large datasets
        total_revenue = pd.to_numeric(df["revenue"], errors='coerce').sum()
        num_orders = df["invoiceno"].nunique()
        num_customers = df["customerid"].nunique()

        # can't calculate AOV without at least one order
        if num_orders == 0:
            logger.warning("No orders in dataset — AOV cannot be calculated")
            return kpis

        kpis["Total Revenue"] = round(total_revenue, 2)
        kpis["Total Orders"] = int(num_orders)
        kpis["Total Customers"] = int(num_customers)

        # num_orders is confirmed > 0 above so this division is safe
        kpis["Average Order Value"] = round(total_revenue / num_orders, 2)

        # value_counts().idxmax() raises ValueError on an empty series
        # so we check length before calling it
        if len(df) > 0:
            kpis["Top Country"] = df["country"].value_counts().idxmax()
        else:
            kpis["Top Country"] = "N/A"

        logger.info(f"{len(kpis)} KPIs calculated successfully")
        return kpis

    except (ValueError, TypeError) as e:
        logger.error(f"KPI calculation error: {e}")
        return kpis
    except Exception as e:
        logger.error(f"Unexpected error in KPI calculation: {e}")
        return kpis


# =============================
# SAVING OUTPUTS
# =============================

def save_clean_data(df: pd.DataFrame, config: Config, logger: logging.Logger) -> bool:
    """Saves cleaned DataFrame as a timestamped CSV so previous runs aren't overwritten."""
    try:
        os.makedirs(config.EXPORT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(config.EXPORT_DIR, f"cleaned_data_{timestamp}.csv")
        df.to_csv(filename, index=False)
        logger.info(f"Cleaned data saved: {filename} ({len(df)} rows)")
        return True
    except IOError as e:
        logger.error(f"IO error saving data: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving data: {e}")
        return False


def save_report(report: Dict[str, Any], kpis: Dict[str, Any],
                config: Config, logger: logging.Logger) -> bool:
    """Writes the validation report and KPIs to a timestamped text file."""
    try:
        os.makedirs(config.EXPORT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(config.EXPORT_DIR, f"data_report_{timestamp}.txt")

        with open(report_file, "w") as f:
            f.write("=" * 60 + "\n")
            f.write("DATA QUALITY REPORT\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write("SUMMARY\n")
            f.write("-" * 60 + "\n")
            f.write(f"Total Rows:     {report['total_rows']}\n")
            f.write(f"Total Columns:  {report['total_columns']}\n")
            f.write(f"Duplicate Rows: {report['duplicate_rows']}\n")
            f.write(f"Columns: {', '.join(report['column_names'])}\n\n")

            f.write("MISSING VALUES\n")
            f.write("-" * 60 + "\n")
            missing = report["missing_values"]
            if any(missing.values()):
                for col, count in missing.items():
                    if count > 0:
                        pct = count / report['total_rows'] * 100
                        f.write(f"  {col}: {count} ({pct:.1f}%)\n")
            else:
                f.write("  No missing values\n")

            # include negative revenue warning in report if flagged
            if report.get("negative_revenue_rows", 0) > 0:
                f.write(f"\nWARNING: {report['negative_revenue_rows']} rows with negative revenue detected\n")

            f.write("\n")
            f.write("=" * 60 + "\n")
            f.write("BUSINESS KPIs\n")
            f.write("=" * 60 + "\n\n")
            if kpis:
                for key, value in kpis.items():
                    f.write(f"  {key}: {value}\n")
            else:
                f.write("  No KPIs calculated\n")

        logger.info(f"Report saved: {report_file}")
        return True

    except IOError as e:
        logger.error(f"IO error saving report: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving report: {e}")
        return False


# =============================
# MAIN PIPELINE
# =============================

def run_pipeline() -> bool:
    config = Config()
    logger = setup_logging(config)
    logger.info("ETL PIPELINE STARTED")

    try:
        if not DatabaseManager.initialize_pool(config, logger):
            logger.critical("Cannot start pipeline — database connection failed")
            return False

        df = fetch_clean_data(logger)
        if df is None:
            return False

        df = clean_data(df, logger)
        report = validate_data(df, logger)
        kpis = calculate_kpis(df, logger)

        save_clean_data(df, config, logger)
        save_report(report, kpis, config, logger)

        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        return True

    except Exception as e:
        logger.error(f"Pipeline failed unexpectedly: {e}")
        return False
    finally:
        DatabaseManager.close_pool(logger)


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
