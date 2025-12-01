"""
Table Schema Definitions
Each file contains one table definition
"""

from pathlib import Path
import importlib.util


def load_table_from_file(filepath):
    """Load table definition from a Python file"""
    spec = importlib.util.spec_from_file_location("table_module", filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.TABLE


def load_all_tables():
    """Load all table definitions from the schemas directory"""
    schemas_dir = Path(__file__).parent
    tables = []

    # Get all Python files except __init__.py
    for filepath in sorted(schemas_dir.glob("*.py")):
        if filepath.name == "__init__.py":
            continue

        try:
            table = load_table_from_file(filepath)
            tables.append(table)
        except Exception as e:
            print(f"Error loading {filepath.name}: {e}")

    # Sort by order
    tables.sort(key=lambda x: x.get("order", 999))

    return tables


# Export all tables
TABLES = load_all_tables()
