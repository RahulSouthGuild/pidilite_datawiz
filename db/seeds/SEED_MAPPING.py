"""
Seed Data Mapping Configuration

Central registry mapping table names to their corresponding CSV seed files.
This is the SINGLE SOURCE OF TRUTH for all seed data loading operations.

Used by: db/load_seed_data.py

Structure:
  SEED_CONFIG[table_name] = {
    "csv_file": str,              - Filename in db/seeds/ directory
    "enabled": bool,              - Whether to load this seed
    "description": str,           - Human-readable description
    "truncate_before_load": bool, - Clear table before loading
  }

Note: This file is SEPARATE from table schemas (db/schemas/)
to maintain clear separation between DDL and DML operations.
"""

SEED_CONFIG = {
    "DimMaterialMapping": {
        "csv_file": "DimMaterialMapping-MASTER-16-04-2025.csv",
        "enabled": True,
        "description": "Material classification mapping - maps materials to divisions and verticals",
        "truncate_before_load": True,
    },
    "DimSalesGroup": {
        "csv_file": "DimSalesGroup.csv",
        "enabled": True,
        "description": "Sales groups with divisions and verticals - reference data for sales reporting",
        "truncate_before_load": True,
    },
    # Template for adding new seed tables:
    # "TableName": {
    #     "csv_file": "TableName.csv",
    #     "enabled": False,  # Set to True to enable loading
    #     "description": "Description of what this table contains",
    #     "truncate_before_load": True,
    # },
}


def get_enabled_seeds():
    """Return only enabled seed configurations"""
    return {table_name: config for table_name, config in SEED_CONFIG.items() if config["enabled"]}


def get_seed_config(table_name):
    """Get configuration for a specific table"""
    return SEED_CONFIG.get(table_name)


def is_enabled(table_name):
    """Check if a table seed is enabled"""
    config = get_seed_config(table_name)
    return config is not None and config.get("enabled", False)
