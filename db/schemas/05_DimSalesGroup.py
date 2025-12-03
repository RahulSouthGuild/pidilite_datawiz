"""
DimSalesGroup Table Definition
Stores unique Sales Group codes and their corresponding divisions and verticals
"""

TABLE = {
    "order": 5,
    "name": "dim_sales_group",
    "schema": """CREATE TABLE dim_sales_group (
            division VARCHAR(50) NOT NULL,
            sales_group SMALLINT NOT NULL,
            vertical VARCHAR(50) NOT NULL
        )
        DISTRIBUTED BY HASH(sales_group) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        );""",
    "comments": {
        "table": "Stores unique Sales Group codes and their corresponding divisions and verticals.",
        "columns": {},
    },
    "indexes": {},
}
