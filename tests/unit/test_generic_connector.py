"""
Unit tests for column projection:
 - GenericConnector._build_select_clause (SQL SELECT pushdown + validation)
 - GenericIngestor._resolve_columns (auto-inclusion of technical columns)
"""
import pytest
from unittest.mock import MagicMock

from src.connectors.generic_connector import GenericConnector
from src.ingestors.generic_ingestor import GenericIngestor


@pytest.mark.unit
@pytest.mark.sql
class TestBuildSelectClause:
    """Projection clause building and validation."""

    def test_none_returns_star(self):
        assert GenericConnector._build_select_clause(None) == "*"

    def test_empty_list_returns_star(self):
        assert GenericConnector._build_select_clause([]) == "*"

    def test_simple_columns(self):
        clause = GenericConnector._build_select_clause(["ProductID", "ProductName"])
        assert clause == "ProductID, ProductName"

    def test_qualified_columns(self):
        clause = GenericConnector._build_select_clause(["dbo.Orders", "t.Amount"])
        assert clause == "dbo.Orders, t.Amount"

    def test_columns_are_trimmed(self):
        clause = GenericConnector._build_select_clause([" ProductID ", "Name"])
        assert clause == "ProductID, Name"

    def test_empty_column_name_rejected(self):
        with pytest.raises(ValueError, match="empty column name"):
            GenericConnector._build_select_clause(["ProductID", "  "])

    def test_non_list_rejected(self):
        with pytest.raises(ValueError, match="must be a list"):
            GenericConnector._build_select_clause("ProductID")

    @pytest.mark.parametrize("bad", [
        "ProductID; DROP TABLE users",
        "1 OR 1=1",
        "col--comment",
        "col name",
        "*",
        "col)",
    ])
    def test_injection_attempts_rejected(self, bad):
        with pytest.raises(ValueError, match="Invalid column name"):
            GenericConnector._build_select_clause(["ProductID", bad])


@pytest.mark.unit
@pytest.mark.ingestor
class TestResolveColumns:
    """Effective column resolution incl. technical columns."""

    def _ingestor(self):
        return GenericIngestor(
            spark=MagicMock(),
            connector=MagicMock(),
            catalog_name="bronze",
            bronze_schema="raw",
            adls_base_path="abfss://x@acct.dfs.core.windows.net",
        )

    def test_no_columns_returns_none(self):
        cfg = {"table_name": "dbo.Products", "load_mode": "full"}
        assert self._ingestor()._resolve_columns(cfg) is None

    def test_full_load_keeps_user_columns(self):
        cfg = {
            "table_name": "dbo.Products",
            "load_mode": "full",
            "columns": ["ProductID", "ProductName"],
        }
        assert self._ingestor()._resolve_columns(cfg) == ["ProductID", "ProductName"]

    def test_incremental_column_auto_included(self):
        cfg = {
            "table_name": "dbo.Orders",
            "load_mode": "incremental",
            "incremental_column": "OrderDate",
            "columns": ["OrderID", "Amount"],
        }
        result = self._ingestor()._resolve_columns(cfg)
        assert result == ["OrderID", "Amount", "OrderDate"]

    def test_incremental_column_not_duplicated(self):
        cfg = {
            "table_name": "dbo.Orders",
            "load_mode": "incremental",
            "incremental_column": "OrderDate",
            "columns": ["OrderID", "OrderDate"],
        }
        result = self._ingestor()._resolve_columns(cfg)
        assert result == ["OrderID", "OrderDate"]

    def test_merge_key_str_auto_included(self):
        cfg = {
            "table_name": "dbo.Orders",
            "load_mode": "full",
            "merge_key": "OrderID",
            "columns": ["Amount"],
        }
        result = self._ingestor()._resolve_columns(cfg)
        assert result == ["Amount", "OrderID"]

    def test_merge_key_list_auto_included(self):
        cfg = {
            "table_name": "warehouse.Inventory",
            "load_mode": "incremental",
            "incremental_column": "LastUpdateTimestamp",
            "merge_key": ["WarehouseID", "ProductID"],
            "columns": ["Qty"],
        }
        result = self._ingestor()._resolve_columns(cfg)
        assert result == ["Qty", "LastUpdateTimestamp", "WarehouseID", "ProductID"]
