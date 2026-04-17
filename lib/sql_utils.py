"""Shared SQL utility functions for DuckDB/SQLite operations."""


def escape_sql_string(value: str) -> str:
    """Escape single quotes for SQL string literals.

    Example: "O'Brien" -> "O''Brien"
    """
    return value.replace("'", "''")


def quote_identifier(name: str) -> str:
    """Double-quote a SQL identifier (table/column name).

    Example: "my column" -> '"my column"'
    Example: 'has"quote' -> '"has""quote"'
    """
    return '"' + name.replace('"', '""') + '"'
