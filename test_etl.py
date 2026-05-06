"""
Unit Tests for E-commerce ETL Pipeline
Run with: pytest test_etl.py -v
"""

"""
Unit Tests for E-commerce ETL Pipeline
Run with: pytest test_etl.py -v
"""

import pandas as pd
import logging
import pytest
from etl_pipeline import clean_data, validate_data, calculate_kpis

# Silent logger so test output stays clean
logger = logging.getLogger()


# =============================
# HELPERS
# =============================

def make_sample_df():
    """Returns a clean, realistic sample DataFrame for testing."""
    return pd.DataFrame({
        "revenue":    [100.0, 200.0, 300.0, 400.0],
        "invoiceno":  ["A1",  "A2",  "A3",  "A4"],
        "customerid": [1,     2,     3,     4],
        "country":    ["UK",  "UK",  "US",  "UK"]
    })


# =============================
# clean_data() TESTS
# =============================

class TestCleanData:

    def test_removes_duplicate_rows(self):
        """Duplicate rows should be dropped, keeping one."""
        df = pd.DataFrame({
            "revenue":    [100.0, 100.0, 200.0],
            "invoiceno":  ["A1",  "A1",  "A2"],
            "customerid": [1,     1,     2],
            "country":    ["UK",  "UK",  "US"]
        })
        result = clean_data(df, logger)
        assert len(result) == 2

    def test_fills_missing_numeric_with_zero(self):
        """Non-sensitive numeric columns should have nulls filled with 0."""
        df = pd.DataFrame({
            "unit_price": [5.0, None, 8.0],
            "discount":   [0.1, None, 0.2]
        })
        result = clean_data(df, logger)
        assert result["unit_price"].isnull().sum() == 0
        assert result["discount"].isnull().sum() == 0

    def test_does_not_fill_revenue_and_quantity(self):
        """revenue and quantity are excluded from auto-fill to protect KPI accuracy."""
        df = pd.DataFrame({
            "revenue":  [100.0, None, 300.0],
            "quantity": [1.0,   None, 3.0]
        })
        result = clean_data(df, logger)
        assert result["revenue"].isnull().sum() == 1
        assert result["quantity"].isnull().sum() == 1

    def test_fills_missing_categorical_with_unknown(self):
        """Missing string values should be filled with 'Unknown'."""
        df = pd.DataFrame({"country": ["UK", None, "US"]})
        result = clean_data(df, logger)
        assert result["country"].iloc[1] == "Unknown"

    def test_standardizes_column_names(self):
        """Column names should be lowercase with underscores."""
        df = pd.DataFrame({"Total Revenue": [100], "Customer ID": [1]})
        result = clean_data(df, logger)
        assert "total_revenue" in result.columns
        assert "customer_id" in result.columns

    def test_empty_dataframe_returns_empty(self):
        """Empty DataFrame should be returned as-is without crashing."""
        df = pd.DataFrame()
        result = clean_data(df, logger)
        assert result.empty

    def test_no_duplicates_unchanged_row_count(self):
        """DataFrame with no duplicates should keep all rows."""
        df = make_sample_df()
        result = clean_data(df, logger)
        assert len(result) == 4

    def test_all_duplicates_keeps_one_row(self):
        """If every row is a duplicate, only one should remain."""
        df = pd.DataFrame({
            "revenue":    [100.0, 100.0, 100.0],
            "invoiceno":  ["A1",  "A1",  "A1"],
            "customerid": [1,     1,     1],
            "country":    ["UK",  "UK",  "UK"]
        })
        result = clean_data(df, logger)
        assert len(result) == 1


# =============================
# validate_data() TESTS
# =============================

class TestValidateData:

    def test_returns_correct_row_count(self):
        """Report should reflect the actual number of rows."""
        df = make_sample_df()
        report = validate_data(df, logger)
        assert report["total_rows"] == 4

    def test_returns_correct_column_count(self):
        """Report should reflect the actual number of columns."""
        df = make_sample_df()
        report = validate_data(df, logger)
        assert report["total_columns"] == 4

    def test_detects_missing_values(self):
        """Missing values should be counted correctly per column."""
        df = pd.DataFrame({
            "revenue":    [100.0, None, 300.0],
            "invoiceno":  ["A1",  "A2", "A3"],
            "customerid": [1,     2,    3],
            "country":    ["UK",  "US", None]
        })
        report = validate_data(df, logger)
        assert report["missing_values"]["revenue"] == 1
        assert report["missing_values"]["country"] == 1

    def test_detects_duplicate_rows(self):
        """Duplicate row count should appear in the report."""
        df = pd.DataFrame({
            "revenue":    [100.0, 100.0],
            "invoiceno":  ["A1",  "A1"],
            "customerid": [1,     1],
            "country":    ["UK",  "UK"]
        })
        report = validate_data(df, logger)
        assert report["duplicate_rows"] == 1

    def test_report_has_required_keys(self):
        """Report dictionary must contain all expected keys."""
        df = make_sample_df()
        report = validate_data(df, logger)
        expected_keys = [
            "total_rows", "total_columns",
            "missing_values", "duplicate_rows",
            "data_types", "column_names"
        ]
        for key in expected_keys:
            assert key in report


# =============================
# calculate_kpis() TESTS
# =============================

class TestCalculateKpis:

    def test_calculates_total_revenue(self):
        """Total revenue should be the sum of the revenue column."""
        df = make_sample_df()  # 100 + 200 + 300 + 400 = 1000
        kpis = calculate_kpis(df, logger)
        assert kpis["Total Revenue"] == 1000.0

    def test_calculates_total_orders(self):
        """Total orders should count unique invoice numbers."""
        df = make_sample_df()
        kpis = calculate_kpis(df, logger)
        assert kpis["Total Orders"] == 4

    def test_calculates_total_customers(self):
        """Total customers should count unique customer IDs."""
        df = make_sample_df()
        kpis = calculate_kpis(df, logger)
        assert kpis["Total Customers"] == 4

    def test_calculates_average_order_value(self):
        """AOV should be total revenue divided by total orders."""
        df = make_sample_df()  # 1000 / 4 = 250.0
        kpis = calculate_kpis(df, logger)
        assert kpis["Average Order Value"] == 250.0

    def test_finds_top_country(self):
        """Top country should be the most frequent country in the data."""
        df = make_sample_df()  # UK appears 3 times, US once
        kpis = calculate_kpis(df, logger)
        assert kpis["Top Country"] == "UK"

    def test_returns_empty_dict_on_empty_dataframe(self):
        """Empty DataFrame should return empty KPIs without crashing."""
        df = pd.DataFrame(columns=["revenue", "invoiceno", "customerid", "country"])
        result = calculate_kpis(df, logger)
        assert result == {}

    def test_returns_empty_dict_on_missing_columns(self):
        """If required columns are missing, KPIs should not be calculated."""
        df = pd.DataFrame({"price": [100, 200]})
        result = calculate_kpis(df, logger)
        assert result == {}

    def test_handles_duplicate_invoices(self):
        """Duplicate invoice numbers should be counted as one order."""
        df = pd.DataFrame({
            "revenue":    [100.0, 200.0],
            "invoiceno":  ["A1",  "A1"],   # same invoice, 2 rows
            "customerid": [1,     1],
            "country":    ["UK",  "UK"]
        })
        kpis = calculate_kpis(df, logger)
        assert kpis["Total Orders"] == 1   # not 2