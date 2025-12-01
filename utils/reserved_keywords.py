"""
StarRocks Reserved Keywords Validator
Detects and handles reserved keywords in column names
"""

# Official StarRocks reserved keywords (from StarRocks 3.x documentation)
STARROCKS_RESERVED_KEYWORDS = {
    # A
    "ADD",
    "ALL",
    "ALTER",
    "ANALYZE",
    "AND",
    "ARRAY",
    "AS",
    "ASC",
    # B
    "BETWEEN",
    "BIGINT",
    "BITMAP",
    "BOTH",
    "BY",
    # C
    "CASE",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "COMPACTION",
    "CONVERT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURRENT_ROLE",
    # D
    "DIV",
    "DATABASE",
    "DATABASES",
    "DECIMAL",
    "DECIMALV2",
    "DECIMAL32",
    "DECIMAL64",
    "DECIMAL128",
    "DEFAULT",
    "DELETE",
    "DENSE_RANK",
    "DESC",
    "DESCRIBE",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "DUAL",
    "DEFERRED",
    # E
    "ELSE",
    "EXCEPT",
    "EXCLUDE",
    "EXISTS",
    "EXPLAIN",
    # F
    "FALSE",
    "FIRST_VALUE",
    "FLOAT",
    "FOR",
    "FORCE",
    "FROM",
    "FULL",
    "FUNCTION",
    # G
    "GRANT",
    "GROUP",
    "GROUPS",
    "GROUPING",
    "GROUPING_ID",
    # H
    "HAVING",
    "HLL",
    "HOST",
    # I
    "IF",
    "IGNORE",
    "IN",
    "INDEX",
    "INFILE",
    "INNER",
    "INSERT",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTO",
    "IS",
    "IMMEDIATE",
    # J
    "JOIN",
    "JSON",
    # K
    "KEY",
    "KEYS",
    "KILL",
    # L
    "LAG",
    "LARGEINT",
    "LAST_VALUE",
    "LATERAL",
    "LEAD",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOAD",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    # M
    "MAXVALUE",
    "MINUS",
    "MOD",
    # N
    "NTILE",
    "NOT",
    "NULL",
    # O
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OUTFILE",
    "OVER",
    # P
    "PARTITION",
    "PERCENTILE",
    "PRIMARY",
    "PROCEDURE",
    # Q
    "QUALIFY",
    # R
    "RANGE",
    "RANK",
    "READ",
    "REGEXP",
    "RELEASE",
    "RENAME",
    "REPLACE",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "ROW",
    "ROWS",
    "ROW_NUMBER",
    # S
    "SCHEMA",
    "SCHEMAS",
    "SELECT",
    "SET",
    "SET_VAR",
    "SHOW",
    "SMALLINT",
    "SYSTEM",
    # T
    "TABLE",
    "TERMINATED",
    "TEXT",
    "THEN",
    "TINYINT",
    "TO",
    "TRUE",
    # U
    "UNION",
    "UNIQUE",
    "UNSIGNED",
    "UPDATE",
    "USE",
    "USING",
    # V
    "VALUES",
    "VARCHAR",
    # W
    "WHEN",
    "WHERE",
    "WITH",
}


def is_reserved_keyword(column_name: str) -> bool:
    """
    Check if a column name is a reserved keyword in StarRocks.
    Uses exact matching (case-insensitive) - does NOT match substrings.

    Examples:
        - "Div" → True (exact match, reserved keyword)
        - "DivisionField" → False (not exact match)
        - "Select" → True (exact match, reserved keyword)
        - "DropDownSelectionForPoi" → False (contains "Select" but not exact match)
        - "SAL" → True (exact match, reserved keyword)
        - "SALARY" → False (not exact match)

    Args:
        column_name: Name to check (case-insensitive)

    Returns:
        True if the name is exactly a reserved keyword, False otherwise
    """
    # Exact match only - case insensitive
    return column_name.upper() in STARROCKS_RESERVED_KEYWORDS


def get_safe_column_name(column_name: str, suffix: str = "_raw") -> str:
    """
    Get a safe column name by adding suffix if it conflicts with reserved keywords.

    Args:
        column_name: Original column name
        suffix: Suffix to add (default: "_raw")

    Returns:
        Original name if safe, or name + suffix if it's a reserved keyword
    """
    if is_reserved_keyword(column_name):
        return f"{column_name}{suffix}"
    return column_name


def check_and_rename_reserved_columns(df, logger=None):
    """
    Check dataframe columns for reserved keywords and rename them.

    Args:
        df: Polars DataFrame
        logger: Optional logger for output

    Returns:
        Tuple of (modified_dataframe, list_of_renamed_columns)
    """
    renamed_columns = {}

    for col_name in df.columns:
        if is_reserved_keyword(col_name):
            safe_name = get_safe_column_name(col_name)
            renamed_columns[col_name] = safe_name

            if logger:
                logger(f"⚠️  Renaming reserved keyword column: {col_name} → {safe_name}")

    # Rename columns in dataframe
    if renamed_columns:
        df = df.rename(renamed_columns)

    return df, renamed_columns


# Example usage for logging
RESERVED_KEYWORDS_BY_CATEGORY = {
    "SQL Keywords": ["SELECT", "FROM", "WHERE", "AND", "OR"],
    "Data Types": ["INT", "VARCHAR", "BIGINT", "FLOAT", "DOUBLE"],
    "StarRocks Specific": ["PARTITION", "DISTRIBUTED", "DUPLICATE", "KEY"],
    "Common Abbreviations": ["SAL", "PL", "WSS", "DIV", "BAL", "TXN"],
}
