"""
Data Loaders - Data loading into target systems

This module contains loaders for inserting validated data into target databases.

Modules (PLANNED):
    starrocks_loader - Loads cleaned and validated Parquet data into StarRocks
                       Handles batch inserts, schema creation, and data validation

    incremental_loader - Incremental data loading with change tracking
                         Implements merge logic and handles updates/inserts/deletes

Future:
    Will support multiple target systems (StarRocks, Snowflake, BigQuery, etc.)
"""
