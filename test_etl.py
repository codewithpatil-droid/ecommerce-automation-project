"""
Unit Tests for E-commerce ETL Pipeline
Run with: pytest test_etl.py -v
"""

import pandas as pd
import logging
import pytest
from etl_pipeline import clean_data, validate_data, calculate_kpis

# suppress logs during tests so output stays readable
logger = logging.getLogger()


# =============================
# SHARED TEST DATA
# =============================

def make_sample_df():
    """Standard 4-row DataFrame used across multiple tests."""
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
        df = pd.DataFrame({
            "revenue":    [100.0, 100.0, 200.0],
            "invoiceno":  ["A1",  "A1",  "A2"],
            "customerid": [1,     1,     2],
            "country":    ["UK",  "UK",  "US"]
        })
        result = clean_data(df, logger)
        assert len(result) == 2

    def test_fills_missing_numeric_with_zero(self):
        df = pd.DataFrame({
            "unit_price": [5.0, None, 8.0],
            "discount":   [0.1, None, 0.2]
        })
        result = clean_data(df, logger)
        assert result["unit_price"].isnull().sum() == 0
        assert result["discount"].isnull().sum() == 0

    def test_revenue_nulls_are_not_filled(self):
        # revenue nulls should stay as-is — filling with 0 would
        # silently lower total revenue and corrupt KPI calculations
        df = pd.DataFrame({
            "revenue":  [100.0, None, 300.0],
            "quantity": [1.0,   None, 3.0]
        })
        result = clean_data(df, logger)
        assert result["revenue"].isnull().sum() == 1
        assert result["quantity"].isnull().sum() == 1

    def test_fills_missing_categorical_with_unknown(self):
        df = pd.DataFrame({"country": ["UK", None, "US"]})
        result = clean_data(df, logger)
        assert result["country"].iloc[1] == "Unknown"

    def test_standardizes_column_names(self):
        df = pd.DataFrame({"Total Revenue": [100], "Customer ID": [1]})
        result = clean_data(df, logger)
        assert "total_revenue" in result.columns
        assert "customer_id" in result.columns

    def test_empty_dataframe_returns_empty(self):
        result = clean_data(pd.DataFrame(), logger)
        assert result.empty

    def test_no_rows_removed_when_no_duplicates(self):
        df = make_sample_df()
        result = clean_data(df, logger)
        assert len(result) == 4

    def test_all_duplicate_rows_leaves_one(self):
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

    def test_row_count_is_correct(self):
        report = validate_data(make_sample_df(), logger)
        assert report["total_rows"] == 4

    def test_column_count_is_correct(self):
        report = validate_data(make_sample_df(), logger)
        assert report["total_columns"] == 4

    def test_detects_missing_values(self):
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
        df = pd.DataFrame({
            "revenue":    [100.0, 100.0],
            "invoiceno":  ["A1",  "A1"],
            "customerid": [1,     1],
            "country":    ["UK",  "UK"]
        })
        report = validate_data(df, logger)
        assert report["duplicate_rows"] == 1

    def test_report_contains_required_keys(self):
        report = validate_data(make_sample_df(), logger)
        for key in ["total_rows", "total_columns", "missing_values",
                    "duplicate_rows", "data_types", "column_names"]:
            assert key in report

    def test_flags_negative_revenue_rows(self):
        # negative revenue = likely returns or data errors
        # validation should count and flag them rather than ignoring them
        df = pd.DataFrame({
            "revenue":    [100.0, -50.0, 300.0],
            "invoiceno":  ["A1",  "A2",  "A3"],
            "customerid": [1,     2,     3],
            "country":    ["UK",  "UK",  "US"]
        })
        report = validate_data(df, logger)
        assert report["negative_revenue_rows"] == 1


# =============================
# calculate_kpis() TESTS
# =============================

class TestCalculateKpis:

    def test_total_revenue(self):
        kpis = calculate_kpis(make_sample_df(), logger)
        assert kpis["Total Revenue"] == 1000.0

    def test_total_orders_counts_unique_invoices(self):
        kpis = calculate_kpis(make_sample_df(), logger)
        assert kpis["Total Orders"] == 4

    def test_total_customers(self):
        kpis = calculate_kpis(make_sample_df(), logger)
        assert kpis["Total Customers"] == 4

    def test_average_order_value(self):
        # 1000 total revenue / 4 orders = 250.0
        kpis = calculate_kpis(make_sample_df(), logger)
        assert kpis["Average Order Value"] == 250.0

    def test_top_country(self):
        # UK appears 3 times vs US once
        kpis = calculate_kpis(make_sample_df(), logger)
        assert kpis["Top Country"] == "UK"

    def test_empty_dataframe_returns_empty_dict(self):
        df = pd.DataFrame(columns=["revenue", "invoiceno", "customerid", "country"])
        assert calculate_kpis(df, logger) == {}

    def test_missing_columns_returns_empty_dict(self):
        df = pd.DataFrame({"price": [100, 200]})
        assert calculate_kpis(df, logger) == {}

    def test_duplicate_invoices_count_as_one_order(self):
        # two rows with same invoice number = 1 order, not 2
        df = pd.DataFrame({
            "revenue":    [100.0, 200.0],
            "invoiceno":  ["A1",  "A1"],
            "customerid": [1,     1],
            "country":    ["UK",  "UK"]
        })
        kpis = calculate_kpis(df, logger)
        assert kpis["Total Orders"] == 1
