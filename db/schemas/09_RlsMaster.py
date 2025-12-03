"""
RlsMaster Table Definition
Stores Row-Level Security (RLS) hierarchy paths for access control
"""

TABLE = {
    "order": 9,
    "name": "rls_master",
    "schema": """CREATE TABLE rls_master (
                cluster VARCHAR(200),
                division VARCHAR(50),
                email_id VARCHAR(200),
                hierarchy_path VARCHAR(2000),
                sales_group SMALLINT,
                sh_2 VARCHAR(200),
                sh_3 VARCHAR(200),
                sh_4 VARCHAR(200),
                sh_5 VARCHAR(200),
                sh_6 VARCHAR(200),
                sh_7 VARCHAR(200),
                vertical VARCHAR(50)luster VARCHAR(200),
                division VARCHAR(50),
                email_id VARCHAR(200),
                hierarchy_path VARCHAR(2000),
                sales_group SMALLINT,
                sh_2 VARCHAR(200),
                sh_3 VARCHAR(200),
                sh_4 VARCHAR(200),
                sh_5 VARCHAR(200),
                sh_6 VARCHAR(200),
                sh_7 VARCHAR(200),
                vertical VARCHAR(50)
            )
            DISTRIBUTED BY HASH(email_id) BUCKETS 10
            PROPERTIES (
                "replication_num" = "1"
            );""",
    "comments": {
        "table": "Stores Row-Level Security (RLS) hierarchy paths for access control.",
        "columns": {},
    },
    "indexes": {},
}
