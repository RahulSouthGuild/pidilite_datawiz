"""
Data Transformers - Data transformation and cleaning pipeline

This module contains transformers for converting and validating data formats.

Modules:
    csv_to_parquet - Converts CSV files to Apache Parquet format with schema inference
                     Includes filtering, type casting, and compression optimization

    parquet_cleaner - Validates and cleans Parquet files against database schemas
                      Performs data validation, schema migration, and generates ALTER TABLE statements
"""
