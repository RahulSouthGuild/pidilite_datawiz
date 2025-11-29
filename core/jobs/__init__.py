"""
ETL Jobs - Job orchestration and scheduling

This module contains job definitions for ETL pipeline orchestration and scheduling.

Modules (PLANNED):
    daily_sync_job - Daily full and incremental data sync from Azure to StarRocks
                     Orchestrates extraction, transformation, and loading

    incremental_job - Incremental data update job for frequent data synchronization
                      Monitors for new data and updates only changed records

    reconciliation_job - Data quality and reconciliation between source and target
                        Validates data integrity and reports discrepancies

Future:
    Will support scheduling via APScheduler, Airflow, or Prefect
"""
