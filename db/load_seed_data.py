#!/usr/bin/env python3
"""
Dynamic Seed Data Loader for StarRocks

This script dynamically loads CSV data into any table defined in SEED_MAPPING.py.
It reads configuration from SEED_MAPPING and loads corresponding CSV files.

Usage:
    python load_seed_data.py                    # Interactive menu
    python load_seed_data.py --load-all         # Load all enabled seeds
    python load_seed_data.py --reload-all       # Truncate and reload all
    python load_seed_data.py --load TableName   # Load specific table
    python load_seed_data.py --help             # Show help

Author: DataWiz Team
"""

import sys
import logging
import csv
from pathlib import Path
from typing import List, Dict, Tuple
import pymysql
from colorama import init, Fore, Style

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.DB_CONFIG import DB_CONFIG  # noqa: E402
from db.seeds.SEED_MAPPING import SEED_CONFIG  # noqa: E402

# Initialize colorama
init(autoreset=True)

# Configure logging
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "seed_data_load.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


class SeedDataLoader:
    """Dynamically load seed data from CSV files to StarRocks tables"""

    def __init__(self):
        """Initialize loader with database config and seed mapping"""
        self.db_config = DB_CONFIG
        self.seed_config = SEED_CONFIG
        self.connection = None
        self.seeds_dir = PROJECT_ROOT / "db" / "seeds"

        logger.info("SeedDataLoader initialized")
        logger.info(f"Seeds directory: {self.seeds_dir}")
        logger.info(f"Configured seeds: {list(self.seed_config.keys())}")

    def print_info(self, msg: str, color=Fore.CYAN):
        """Print colored info message"""
        print(f"{color}[INFO]{Style.RESET_ALL} {msg}")

    def print_success(self, msg: str):
        """Print colored success message"""
        print(f"{Fore.GREEN}[SUCCESS]{Style.RESET_ALL} {msg}")

    def print_warning(self, msg: str):
        """Print colored warning message"""
        print(f"{Fore.YELLOW}[WARNING]{Style.RESET_ALL} {msg}")

    def print_error(self, msg: str):
        """Print colored error message"""
        print(f"{Fore.RED}[ERROR]{Style.RESET_ALL} {msg}")

    def connect(self) -> bool:
        """Connect to StarRocks database"""
        try:
            self.print_info("🔌 Connecting to StarRocks...", Fore.CYAN)
            logger.info(f"Connecting to {self.db_config['host']}:{self.db_config['port']}")

            self.connection = pymysql.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                database=self.db_config["database"],
                charset=self.db_config["charset"],
                autocommit=self.db_config["autocommit"],
            )

            self.print_success(
                f"✅ Connected to '{self.db_config['database']}' as " f"'{self.db_config['user']}'"
            )
            logger.info("Connected to StarRocks successfully")
            return True

        except Exception as e:
            self.print_error(f"Failed to connect: {e}")
            logger.error(f"Connection failed: {e}", exc_info=True)
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from StarRocks")

    def read_csv(self, csv_file: str) -> Tuple[List[str], List[Dict]]:
        """
        Read CSV file and return headers and rows

        Args:
            csv_file: Filename in db/seeds/ directory

        Returns:
            Tuple of (headers list, rows list of dicts)
        """
        csv_path = self.seeds_dir / csv_file

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        logger.info(f"Reading CSV: {csv_path}")
        headers = []
        rows = []

        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                for row in reader:
                    # Filter empty values
                    cleaned_row = {k: v for k, v in row.items() if v is not None and v.strip()}
                    rows.append(cleaned_row)

            logger.info(f"Read {len(rows)} rows from {csv_file}")
            self.print_info(f"📄 Read {len(rows)} rows from {csv_file}")
            return headers, rows

        except Exception as e:
            logger.error(f"CSV read error: {e}")
            raise

    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate table before loading

        Args:
            table_name: Name of table to truncate

        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            cursor.close()

            self.print_info(f"🗑️  Truncated {table_name}")
            logger.info(f"Truncated table: {table_name}")
            return True

        except Exception as e:
            self.print_warning(f"Failed to truncate {table_name}: {e}")
            logger.warning(f"Truncate failed for {table_name}: {e}")
            return False

    def load_data_batch(
        self, table_name: str, headers: List[str], rows: List[Dict], batch_size: int = 10000
    ) -> int:
        """
        Load data using batch INSERT statements

        Args:
            table_name: Target table name
            headers: Column names from CSV
            rows: Row data as list of dicts
            batch_size: Rows per INSERT batch

        Returns:
            Number of rows inserted
        """
        if not rows:
            self.print_warning(f"No data to load into {table_name}")
            return 0

        try:
            cursor = self.connection.cursor()
            total_inserted = 0

            # Process in batches
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                values_list = []

                for row in batch:
                    # Create values in column order
                    values = tuple(row.get(h, None) for h in headers)
                    formatted_values = []

                    for val in values:
                        if val is None or val == "":
                            formatted_values.append("NULL")
                        else:
                            # Escape quotes
                            escaped_val = str(val).replace("'", "''")
                            formatted_values.append(f"'{escaped_val}'")

                    values_list.append(f"({', '.join(formatted_values)})")

                # Build INSERT statement
                columns = ", ".join(headers)
                values_str = ", ".join(values_list)
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES {values_str};"

                try:
                    cursor.execute(insert_sql)
                    batch_count = len(batch)
                    total_inserted += batch_count

                    batch_num = (i // batch_size) + 1
                    self.print_info(f"✓ Batch {batch_num}: {batch_count} rows inserted")
                    logger.info(f"Batch {batch_num} inserted: {batch_count} rows")

                except Exception as e:
                    batch_num = (i // batch_size) + 1
                    self.print_error(f"Batch {batch_num} failed: {str(e)[:80]}")
                    logger.error(f"Batch {batch_num} error: {e}")
                    # Continue with next batch

            cursor.close()
            return total_inserted

        except Exception as e:
            self.print_error(f"Load data error: {e}")
            logger.error(f"Load data error: {e}", exc_info=True)
            return 0

    def load_seed(self, table_name: str, truncate: bool = False) -> int:
        """
        Load seed data for a specific table

        Args:
            table_name: Table name from SEED_CONFIG
            truncate: Whether to truncate before loading

        Returns:
            Number of rows loaded
        """
        # Get seed config
        config = self.seed_config.get(table_name)
        if not config:
            self.print_error(f"Table '{table_name}' not in SEED_CONFIG")
            logger.error(f"Unknown table: {table_name}")
            return 0

        if not config.get("enabled", False):
            self.print_warning(f"Table '{table_name}' is disabled in SEED_CONFIG")
            logger.warning(f"Table disabled: {table_name}")
            return 0

        csv_file = config.get("csv_file")
        description = config.get("description", "")

        self.print_info(f"\n📥 Loading {table_name}", Fore.CYAN)
        if description:
            self.print_info(f"   {description}")

        try:
            # Read CSV
            headers, rows = self.read_csv(csv_file)

            # Truncate if requested
            if truncate:
                self.truncate_table(table_name)
            elif config.get("truncate_before_load", False):
                self.truncate_table(table_name)

            # Load data
            rows_loaded = self.load_data_batch(table_name, headers, rows)

            if rows_loaded > 0:
                self.print_success(f"✅ Loaded {rows_loaded} rows into {table_name}")
                logger.info(f"Successfully loaded {rows_loaded} rows into {table_name}")
            else:
                self.print_warning(f"No rows loaded into {table_name}")

            return rows_loaded

        except Exception as e:
            self.print_error(f"Failed to load {table_name}: {e}")
            logger.error(f"Load failed for {table_name}: {e}", exc_info=True)
            return 0

    def get_row_count(self, table_name: str) -> int:
        """Get current row count in table"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"Failed to get row count for {table_name}: {e}")
            return 0

    def show_status(self):
        """Show current row counts for all tables"""
        print("\n" + "=" * 70)
        print(f"{Fore.CYAN}📊 Seed Data Status{Style.RESET_ALL}")
        print("=" * 70)

        for table_name, config in self.seed_config.items():
            count = self.get_row_count(table_name)
            enabled = "✅" if config.get("enabled") else "❌"
            description = config.get("description", "")

            print(f"\n{Fore.YELLOW}{enabled} {table_name}{Style.RESET_ALL}")
            print(f"   Rows: {count:>10,}")
            if description:
                print(f"   Info: {description}")

        print("\n" + "=" * 70 + "\n")

    def show_menu(self):
        """Display interactive menu"""
        print("\n" + "=" * 70)
        print(f"{Fore.CYAN}🌱 Seed Data Loader - Interactive Menu{Style.RESET_ALL}")
        print("=" * 70)

        print(f"\n{Fore.YELLOW}Load Individual Tables:{Style.RESET_ALL}")
        for i, (table_name, config) in enumerate(self.seed_config.items(), 1):
            enabled = "✅" if config.get("enabled") else "❌"
            print(f"  {i}. {enabled} {table_name} (Truncate + Load)")

        print(f"\n{Fore.YELLOW}Quick Actions:{Style.RESET_ALL}")
        print("  3. Load All type all (Truncate + Load)")
        print("  0. Exit")
        print()

    def run_interactive(self):
        """Run interactive mode"""
        if not self.connect():
            return

        try:
            while True:
                self.show_menu()
                choice = input(f"{Fore.YELLOW}Enter choice: {Style.RESET_ALL}").strip()

                if choice == "0":
                    self.print_info("👋 Goodbye!")
                    break

                elif choice == "all":
                    # Load all with truncate
                    self.load_all(truncate=True)

                else:
                    try:
                        idx = int(choice) - 1
                        table_name = list(self.seed_config.keys())[idx]
                        # Always truncate and load
                        self.load_seed(table_name, truncate=True)
                    except (ValueError, IndexError):
                        self.print_warning("Invalid choice. Please try again.")

        except KeyboardInterrupt:
            print("\n")
            self.print_warning("Operation cancelled")
        finally:
            self.disconnect()

    def load_all(self, truncate: bool = False) -> int:
        """
        Load all enabled seeds

        Args:
            truncate: Whether to truncate tables first

        Returns:
            Total rows loaded
        """
        total_loaded = 0

        for table_name, config in self.seed_config.items():
            if config.get("enabled", False):
                rows = self.load_seed(table_name, truncate=truncate)
                total_loaded += rows

        print("\n" + "=" * 70)
        self.print_success("Seed data loading complete!")
        self.print_info(f"Total rows loaded: {total_loaded:,}")
        print("=" * 70 + "\n")

        return total_loaded

    def load_specific(self, table_name: str, truncate: bool = False) -> int:
        """Load specific table by name"""
        if table_name not in self.seed_config:
            self.print_error(f"Table '{table_name}' not found in SEED_CONFIG")
            available = ", ".join(self.seed_config.keys())
            self.print_info(f"Available tables: {available}")
            return 0

        return self.load_seed(table_name, truncate=truncate)


def main():
    """Main entry point"""
    loader = SeedDataLoader()

    # Parse command line arguments
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()

        if cmd == "--help":
            print(__doc__)
            return 0

        if cmd == "--load-all":
            if not loader.connect():
                return 1
            try:
                loader.load_all(truncate=False)
                return 0
            finally:
                loader.disconnect()

        elif cmd == "--reload-all":
            if not loader.connect():
                return 1
            try:
                loader.load_all(truncate=True)
                return 0
            finally:
                loader.disconnect()

        elif cmd == "--load" and len(sys.argv) > 2:
            table_name = sys.argv[2]
            if not loader.connect():
                return 1
            try:
                loader.load_specific(table_name, truncate=False)
                return 0
            finally:
                loader.disconnect()

        elif cmd == "--reload" and len(sys.argv) > 2:
            table_name = sys.argv[2]
            if not loader.connect():
                return 1
            try:
                loader.load_specific(table_name, truncate=True)
                return 0
            finally:
                loader.disconnect()

        elif cmd == "--status":
            if not loader.connect():
                return 1
            try:
                loader.show_status()
                return 0
            finally:
                loader.disconnect()

        else:
            print(f"Unknown command: {cmd}")
            print("Use --help for usage information")
            return 1
    else:
        # Interactive mode
        loader.run_interactive()
        return 0


if __name__ == "__main__":
    sys.exit(main())
