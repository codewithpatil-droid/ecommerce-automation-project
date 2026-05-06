"""
E-commerce ETL Pipeline
A production-ready data pipeline for extracting, cleaning, validating, 
and analyzing e-commerce data from PostgreSQL.

Author: [Your Name]
Version: 1.0
"""

"""
E-commerce ETL Pipeline
A production-ready data pipeline for extracting, cleaning, validating, 
and analyzing e-commerce data from PostgreSQL.

Author: [Your Name]
Version: 1.0
"""

import pandas as pd
import psycopg2
from psycopg2 import pool
import logging
import sys
import os
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# =============================
# CONFIGURATION
# =============================

@dataclass
class Config:
    """Configuration management for the ETL pipeline."""
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
    Configure logging for the pipeline.
    
    Args:
        config: Configuration object
        
    Returns:
        Configured logger instance
    """
    # Create exports directory if it doesn't exist
    os.makedirs(config.EXPORT_DIR, exist_ok=True)
    
    # Configure logging
    logger = logging.getLogger(__name__)
    logger.setLevel(config.LOG_LEVEL)
    
    # File handler
    file_handler = logging.FileHandler(config.LOG_FILE)
    file_handler.setLevel(config.LOG_LEVEL)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(config.LOG_LEVEL)
    
    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# =============================
# DATABASE CONNECTION
# =============================

class DatabaseManager:
    """Manages database connections and pooling."""
    
    _connection_pool: Optional[pool.SimpleConnectionPool] = None
    
    @classmethod
    def initialize_pool(cls, config: Config, logger: logging.Logger) -> bool:
        """
        Initialize the connection pool.
        
        Args:
            config: Configuration object
            logger: Logger instance
            
        Returns:
            True if pool initialized successfully, False otherwise
        """
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
            logger.error(f"Failed to initialize connection pool: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error initializing connection pool: {e}")
            return False
    
    @classmethod
    def get_connection(cls, logger: logging.Logger) -> Optional[psycopg2.extensions.connection]:
        """
        Get a connection from the pool.
        
        Args:
            logger: Logger instance
            
        Returns:
            Database connection or None if failed
        """
        if cls._connection_pool is None:
            logger.error("Connection pool not initialized")
            return None
        
        try:
            conn = cls._connection_pool.getconn()
            logger.debug("Connection retrieved from pool")
            return conn
        except pool.PoolError as e:
            logger.error(f"Failed to get connection from pool: {e}")
            return None
    
    @classmethod
    def return_connection(cls, conn: psycopg2.extensions.connection, 
                         logger: logging.Logger) -> None:
        """
        Return a connection to the pool.
        
        Args:
            conn: Database connection to return
            logger: Logger instance
        """
        if cls._connection_pool is None:
            logger.error("Connection pool not initialized")
            return
        
        try:
            cls._connection_pool.putconn(conn)
            logger.debug("Connection returned to pool")
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")
    
    @classmethod
    def close_pool(cls, logger: logging.Logger) -> None:
        """
        Close all connections in the pool.
        
        Args:
            logger: Logger instance
        """
        if cls._connection_pool:
            cls._connection_pool.closeall()
            logger.info("Database connection pool closed")


# =============================
# DATA FETCHING
# =============================

def fetch_clean_data(logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    Fetch data from the database.
    
    Args:
        logger: Logger instance
        
    Returns:
        DataFrame with fetched data or None if failed
    """
    conn = DatabaseManager.get_connection(logger)
    if not conn:
        logger.error("Could not fetch data: no database connection")
        return None
    
    query = "SELECT * FROM clean_ecommerce;"
    
    try:
        df = pd.read_sql(query, conn)
        
        if df.empty:
            logger.warning("Fetched data is empty")
        else:
            logger.info(f"Data fetched successfully: {len(df)} rows, {len(df.columns)} columns")
        
        return df
        
    except psycopg2.DatabaseError as e:
        logger.error(f"Database error fetching data: {e}")
        return None
    except psycopg2.OperationalError as e:
        logger.error(f"Connection error fetching data: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching data: {e}")
        return None
    finally:
        if conn:
            DatabaseManager.return_connection(conn, logger)


# =============================
# DATA CLEANING
# =============================

def clean_data(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Clean and standardize the data.
    
    Args:
        df: Input DataFrame
        logger: Logger instance
        
    Returns:
        Cleaned DataFrame
    """
    if df.empty:
        logger.warning("Cannot clean empty DataFrame")
        return df
    
    logger.info("Starting data cleaning")
    
    initial_rows = len(df)
    
    # Standardize column names: lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(" ", "_", regex=False)
    logger.debug("Column names standardized")
    
    # Remove duplicate rows
    initial_duplicates = df.duplicated().sum()
    df = df.drop_duplicates()
    logger.info(f"Removed {initial_duplicates} duplicate rows")
    
    # Fill missing numeric values with 0 (excluding revenue and quantity — they affect KPIs)
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    safe_fill = [c for c in numeric_cols if c not in ("revenue", "quantity")]
    if safe_fill:
        for col in safe_fill:
            df[col] = df[col].fillna(0)
        logger.debug(f"Filled missing values in {len(safe_fill)} numeric columns")
        
    # Fill missing categorical values with "Unknown"
    categorical_cols = df.select_dtypes(include=['object','string']).columns.tolist()
    if categorical_cols:
        for col in categorical_cols:
            df[col] = df[col].fillna("Unknown")
        logger.debug(f"Filled missing values in {len(categorical_cols)} categorical columns")
    
    rows_after_cleaning = len(df)
    logger.info(
        f"Data cleaning completed: {initial_rows} → {rows_after_cleaning} rows "
        f"({initial_rows - rows_after_cleaning} rows removed)"
    )
    
    return df


# =============================
# DATA VALIDATION
# =============================

def validate_data(df: pd.DataFrame, logger: logging.Logger) -> Dict[str, Any]:
    """
    Validate data quality and generate a report.
    
    Args:
        df: DataFrame to validate
        logger: Logger instance
        
    Returns:
        Validation report dictionary
    """
    logger.info("Starting data validation")
    
    report = {
        "total_rows": df.shape[0],
        "total_columns": df.shape[1],
        "missing_values": df.isnull().sum().to_dict(),
        "duplicate_rows": df.duplicated().sum(),
        "data_types": df.dtypes.to_dict(),
        "column_names": df.columns.tolist()
    }
    
    # Calculate missing percentage for warnings
    missing_pct = (df.isnull().sum() / len(df) * 100)
    high_missing = missing_pct[missing_pct > 10]
    
    if not high_missing.empty:
        logger.warning(f"High missing values detected: {high_missing.to_dict()}")
    
    logger.info("Data validation completed")
    return report


# =============================
# KPI CALCULATION
# =============================

def calculate_kpis(df: pd.DataFrame, logger: logging.Logger) -> Dict[str, Any]:
    """
    Calculate business KPIs from the data.
    
    Args:
        df: Input DataFrame
        logger: Logger instance
        
    Returns:
        Dictionary of calculated KPIs
    """
    kpis = {}
    
    # Check if required columns exist
    required_cols = ["revenue", "invoiceno", "customerid", "country"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        logger.warning(f"Cannot calculate KPIs. Missing columns: {missing_cols}")
        return kpis
    
    if df.empty:
        logger.warning("Cannot calculate KPIs: DataFrame is empty")
        return kpis
    
    try:
        # BUG FIX #1: Calculate once and reuse to avoid redundant operations
        total_revenue = pd.to_numeric(df["revenue"], errors='coerce').sum()
        num_orders = df["invoiceno"].nunique()
        num_customers = df["customerid"].nunique()
        
        # BUG FIX #2: Check for zero division
        if num_orders == 0:
            logger.warning("No orders found in data")
            return kpis
        
        kpis["Total Revenue"] = round(total_revenue, 2)
        kpis["Total Orders"] = int(num_orders)
        kpis["Total Customers"] = int(num_customers)
        
        # BUG FIX #3: Safe division with zero check
        kpis["Average Order Value"] = round(total_revenue / num_orders, 2)
        
        # BUG FIX #4: Handle potential errors with idxmax()
        if len(df) > 0:
            top_country = df["country"].value_counts().idxmax()
            kpis["Top Country"] = top_country
        else:
            kpis["Top Country"] = "N/A"
        
        logger.info(f"KPI calculation completed: {len(kpis)} KPIs calculated")
        return kpis
        
    except (ValueError, TypeError) as e:
        logger.error(f"Error calculating KPIs: {e}")
        return kpis
    except Exception as e:
        logger.error(f"Unexpected error calculating KPIs: {e}")
        return kpis


# =============================
# DATA PERSISTENCE
# =============================

def save_clean_data(df: pd.DataFrame, config: Config, logger: logging.Logger) -> bool:
    """
    Save cleaned data to CSV file.
    
    Args:
        df: DataFrame to save
        config: Configuration object
        logger: Logger instance
        
    Returns:
        True if successful, False otherwise
    """
    try:
        os.makedirs(config.EXPORT_DIR, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(config.EXPORT_DIR, f"cleaned_data_{timestamp}.csv")
        
        df.to_csv(filename, index=False)
        logger.info(f"Cleaned data saved: {filename} ({len(df)} rows)")
        return True
        
    except IOError as e:
        logger.error(f"IO error saving cleaned data: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving cleaned data: {e}")
        return False


def save_report(report: Dict[str, Any], kpis: Dict[str, Any], 
                config: Config, logger: logging.Logger) -> bool:
    """
    Save validation report and KPIs to file.
    
    Args:
        report: Validation report dictionary
        kpis: KPIs dictionary
        config: Configuration object
        logger: Logger instance
        
    Returns:
        True if successful, False otherwise
    """
    try:
        os.makedirs(config.EXPORT_DIR, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(config.EXPORT_DIR, f"data_report_{timestamp}.txt")
        
        with open(report_file, "w") as f:
            # Data Quality Report
            f.write("=" * 60 + "\n")
            f.write("DATA QUALITY REPORT\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("SUMMARY\n")
            f.write("-" * 60 + "\n")
            f.write(f"Total Rows: {report['total_rows']}\n")
            f.write(f"Total Columns: {report['total_columns']}\n")
            f.write(f"Duplicate Rows: {report['duplicate_rows']}\n")
            f.write(f"Columns: {', '.join(report['column_names'])}\n\n")
            
            # Missing Values
            f.write("MISSING VALUES\n")
            f.write("-" * 60 + "\n")
            missing_values = report["missing_values"]
            if any(missing_values.values()):
                for col, count in missing_values.items():
                    if count > 0:
                        pct = (count / report['total_rows'] * 100)
                        f.write(f"{col}: {count} ({pct:.2f}%)\n")
            else:
                f.write("No missing values found\n")
            f.write("\n")
            
            # Data Types
            f.write("DATA TYPES\n")
            f.write("-" * 60 + "\n")
            for col, dtype in report["data_types"].items():
                f.write(f"{col}: {dtype}\n")
            f.write("\n")
            
            # Business KPIs
            f.write("=" * 60 + "\n")
            f.write("BUSINESS KPIs\n")
            f.write("=" * 60 + "\n\n")
            
            if kpis:
                for key, value in kpis.items():
                    f.write(f"{key}: {value}\n")
            else:
                f.write("No KPIs calculated\n")
        
        logger.info(f"Report saved successfully: {report_file}")
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
            logger.critical("Failed to initialize connection pool")
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
        logger.error(f"Pipeline failed: {e}")
        return False
    finally:
        DatabaseManager.close_pool(logger)


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)