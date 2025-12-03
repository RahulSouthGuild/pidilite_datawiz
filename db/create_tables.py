"""
StarRocks Table Creation Script
Creates tables, views, and other database objects with proper logging and error handling
"""

import sys
import time
import logging
from pathlib import Path
from typing import List, Dict, Optional
import pymysql
from colorama import init, Fore, Style

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.DB_CONFIG import DB_CONFIG  # noqa: E402
from db.schema import TABLES  # noqa: E402

# Initialize colorama
init(autoreset=True)

# Configure logging
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "create_tables.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


class StarRocksTableManager:
    """Manages StarRocks table creation and deletion"""

    def __init__(self):
        self.config = DB_CONFIG
        self.connection = None

    def connect(self) -> pymysql.Connection:
        """Establish connection to StarRocks"""
        try:
            self.print_info("🔌 Connecting to StarRocks...", Fore.CYAN)
            logger.info(f"Connecting to StarRocks at {self.config['host']}:{self.config['port']}")

            self.connection = pymysql.connect(
                host=self.config["host"],
                port=self.config["port"],
                user=self.config["user"],
                password=self.config["password"],
                database=self.config["database"],
                charset=self.config["charset"],
                autocommit=self.config["autocommit"],
            )

            self.print_success(f"Connected to StarRocks database '{self.config['database']}'")
            logger.info("Successfully connected to StarRocks")
            return self.connection

        except Exception as e:
            self.print_error(f"Failed to connect to StarRocks: {e}")
            logger.error(f"Connection error: {e}", exc_info=True)
            raise

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from StarRocks")

    @staticmethod
    def print_success(message: str):
        """Print success message"""
        print(f"{Style.BRIGHT}{Fore.GREEN}✅ {message}{Style.RESET_ALL}")

    @staticmethod
    def print_error(message: str):
        """Print error message"""
        print(f"{Style.BRIGHT}{Fore.RED}❌ {message}{Style.RESET_ALL}")

    @staticmethod
    def print_warning(message: str):
        """Print warning message"""
        print(f"{Style.BRIGHT}{Fore.YELLOW}⚠️  {message}{Style.RESET_ALL}")

    @staticmethod
    def print_info(message: str, color=Fore.CYAN):
        """Print info message"""
        print(f"{Style.BRIGHT}{color}{message}{Style.RESET_ALL}")

    def execute_query(self, query: str, description: str = "", max_retries: int = 3) -> bool:
        """Execute SQL query with retry logic"""
        cursor = None
        try:
            cursor = self.connection.cursor()
            retries = 0

            while retries < max_retries:
                try:
                    logger.debug(f"Executing: {query[:100]}...")
                    cursor.execute(query)
                    self.connection.commit()
                    logger.info(f"Successfully executed: {description}")
                    return True

                except pymysql.err.OperationalError as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(
                            f"Failed to execute {description} after {max_retries} retries: {e}"
                        )
                        self.print_error(f"Failed to execute {description}: {e}")
                        return False

                    self.print_warning(f"Retrying {description} ({retries}/{max_retries})...")
                    logger.warning(f"Retry {retries}/{max_retries} for {description}: {e}")
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Error executing {description}: {e}", exc_info=True)
                    self.print_error(f"Error executing {description}: {e}")
                    return False

        finally:
            if cursor:
                cursor.close()

    def create_table(self, table: Dict, skip_indexes: bool = False) -> bool:
        """Create a single table or view"""
        try:
            table_name = table["name"]
            table_type = table.get("type", "TABLE")

            if table_type == "VIEW":
                return self._create_view(table)
            else:
                return self._create_table(table, skip_indexes)

        except Exception as e:
            logger.error(f"Error creating {table.get('name', 'unknown')}: {e}", exc_info=True)
            self.print_error(f"Error creating {table.get('name', 'unknown')}: {e}")
            return False

    def _create_view(self, view: Dict) -> bool:
        """Create a view"""
        view_name = view["name"]
        self.print_info(f"👁️  Creating view {view_name}...", Fore.CYAN)
        logger.info(f"Creating view: {view_name}")

        # Drop existing view
        if not self.execute_query(f"DROP VIEW IF EXISTS {view_name}", f"drop view {view_name}"):
            return False

        # Create view
        schema = view["schema"]
        if not self.execute_query(schema, f"create view {view_name}"):
            return False

        # Handle comments
        self._handle_comments(view)

        self.print_success(f"Successfully created view {view_name}")
        logger.info(f"View {view_name} created successfully")
        return True

    def _create_table(self, table: Dict, skip_indexes: bool = False) -> bool:
        """Create a table"""
        table_name = table["name"]
        self.print_info(f"📊 Creating table {table_name}...", Fore.BLUE)
        logger.info(f"Creating table: {table_name}")

        # Drop existing table
        if not self.execute_query(f"DROP TABLE IF EXISTS {table_name}", f"drop table {table_name}"):
            return False

        # Execute pre-create steps
        for step in sorted(table.get("pre_create_steps", []), key=lambda x: x["step"]):
            cmd = step["cmd"]
            desc = f"pre-create step {step['step']} for {table_name}"
            if not self.execute_query(cmd, desc):
                self.print_warning(f"Skipping {desc} due to error")

        # Create table
        schema = table["schema"]
        if not self.execute_query(schema, f"create table {table_name}"):
            return False

        # Execute post-create steps (INSERT statements, etc.)
        for step in sorted(table.get("post_create_steps", []), key=lambda x: x["step"]):
            cmd = step["cmd"]
            desc = f"post-create step {step['step']} for {table_name}"
            if self.execute_query(cmd, desc):
                self.print_success(f"Executed {desc}")
            else:
                self.print_warning(f"Skipping {desc} due to error")

        # Handle comments
        self._handle_comments(table)

        self.print_success(f"Successfully created table {table_name}")
        logger.info(f"Table {table_name} created successfully")
        return True

    def _handle_comments(self, table: Dict):
        """Handle table and column comments"""
        try:
            comments = table.get("comments", {})
            table_name = table["name"]

            # Note: StarRocks supports comments in CREATE TABLE/VIEW statements
            # Comments are typically added inline, not via ALTER statements
            if comments.get("table"):
                logger.info(f"Table comment for {table_name}: {comments['table']}")

            if comments.get("columns"):
                for column, comment in comments["columns"].items():
                    logger.info(f"Column comment for {column} in {table_name}: {comment}")

        except Exception as e:
            logger.warning(f"Error handling comments for {table['name']}: {e}")

    def create_multiple_tables(self, tables: List[Dict], skip_indexes: bool = False) -> tuple:
        """Create multiple tables/views"""
        start_time = time.time()
        success_count = 0
        failed_count = 0

        # Sort by order
        sorted_tables = sorted(tables, key=lambda x: x.get("order", 0))

        for table in sorted_tables:
            if self.create_table(table, skip_indexes):
                success_count += 1
            else:
                failed_count += 1

        elapsed_time = time.time() - start_time

        self.print_info(f"\n{'='*60}\n📊 Summary\n{'='*60}", Fore.CYAN)
        self.print_success(f"Created: {success_count} object(s)")
        if failed_count > 0:
            self.print_error(f"Failed: {failed_count} object(s)")
        self.print_info(f"⏱️  Total time: {elapsed_time:.2f} seconds", Fore.CYAN)

        logger.info(
            f"Batch operation completed: {success_count} succeeded, {failed_count} failed in {elapsed_time:.2f}s"
        )

        return success_count, failed_count

    def drop_table(self, table_name: str, object_type: str = "TABLE") -> bool:
        """Drop a single table or view"""
        try:
            if object_type == "VIEW":
                query = f"DROP VIEW IF EXISTS {table_name}"
            else:
                query = f"DROP TABLE IF EXISTS {table_name}"

            if self.execute_query(query, f"drop {object_type.lower()} {table_name}"):
                self.print_info(f"🗑️  Dropped {object_type.lower()} {table_name}", Fore.YELLOW)
                logger.info(f"Dropped {object_type.lower()}: {table_name}")
                return True
            return False

        except Exception as e:
            logger.error(f"Error dropping {table_name}: {e}", exc_info=True)
            self.print_error(f"Error dropping {table_name}: {e}")
            return False

    def drop_all_objects(self, object_types: Optional[List[str]] = None) -> tuple:
        """Drop all objects (tables and/or views)"""
        start_time = time.time()

        if object_types is None:
            object_types = ["view", "table"]

        success_count = 0
        failed_count = 0

        if "view" in object_types:
            self.print_info("🗑️  Dropping all views...", Fore.RED)
            views = [t for t in TABLES if t.get("type") == "VIEW"]
            for view in views:
                if self.drop_table(view["name"], "VIEW"):
                    success_count += 1
                else:
                    failed_count += 1

        if "table" in object_types:
            self.print_info("🗑️  Dropping all tables...", Fore.RED)
            tables = [t for t in TABLES if not t.get("type")]
            for table in tables:
                if self.drop_table(table["name"], "TABLE"):
                    success_count += 1
                else:
                    failed_count += 1

        elapsed_time = time.time() - start_time

        self.print_info(f"\n{'='*60}\n📊 Drop Summary\n{'='*60}", Fore.RED)
        self.print_success(f"Dropped: {success_count} object(s)")
        if failed_count > 0:
            self.print_error(f"Failed: {failed_count} object(s)")
        self.print_info(f"⏱️  Total time: {elapsed_time:.2f} seconds", Fore.RED)

        logger.info(
            f"Drop operation completed: {success_count} succeeded, {failed_count} failed in {elapsed_time:.2f}s"
        )

        return success_count, failed_count


def select_objects_interactive(objects: List[Dict], obj_type: str) -> List[Dict]:
    """Interactive selection of objects"""
    print(f"\n{Style.BRIGHT}{Fore.CYAN}Select {obj_type}(s):{Style.RESET_ALL}")

    for i, obj in enumerate(objects, 1):
        obj_desc = obj["name"]
        if obj.get("comments", {}).get("table"):
            obj_desc += f" - {obj['comments']['table']}"
        print(f"{Style.BRIGHT}{Fore.WHITE}{i}. {obj_desc}{Style.RESET_ALL}")

    selections = input(f"\nEnter {obj_type} numbers (comma-separated, 0 for all): ")

    if selections.strip() == "0":
        return objects
    else:
        indices = [
            int(x.strip()) - 1
            for x in selections.split(",")
            if x.strip().isdigit() and 0 <= int(x.strip()) - 1 < len(objects)
        ]
        return [objects[i] for i in indices]


def display_menu():
    """Display interactive menu"""
    manager = StarRocksTableManager()

    while True:
        try:
            print(f"\n{Style.BRIGHT}{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
            print(f"{Style.BRIGHT}{Fore.CYAN}🔧 StarRocks Database Setup Menu{Style.RESET_ALL}")
            print(f"{Style.BRIGHT}{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

            print(f"\n{Style.BRIGHT}{Fore.BLUE}Creation Operations:{Style.RESET_ALL}")
            print(f"{Fore.WHITE}1. Create ALL Objects (Tables + Views){Style.RESET_ALL}")
            print(f"{Fore.WHITE}2. Create All Tables{Style.RESET_ALL}")
            print(f"{Fore.WHITE}3. Create Specific Table(s){Style.RESET_ALL}")
            print(f"{Fore.WHITE}4. Create All Views{Style.RESET_ALL}")
            print(f"{Fore.WHITE}5. Create Specific View(s){Style.RESET_ALL}")

            print(f"\n{Style.BRIGHT}{Fore.RED}Deletion Operations:{Style.RESET_ALL}")
            print(f"{Fore.RED}6. Drop All Objects{Style.RESET_ALL}")
            print(f"{Fore.RED}7. Drop Specific Object{Style.RESET_ALL}")

            print(f"\n{Fore.WHITE}0. Exit{Style.RESET_ALL}")

            choice = input(f"\n{Style.BRIGHT}Enter choice: {Style.RESET_ALL}")

            if choice == "0":
                print(f"{Style.BRIGHT}{Fore.CYAN}👋 Goodbye!{Style.RESET_ALL}")
                logger.info("Application exited normally")
                break

            # Connect to database
            manager.connect()

            try:
                if choice == "1":
                    # Create all objects
                    manager.create_multiple_tables(TABLES)

                elif choice == "2":
                    # Create all tables
                    tables = [t for t in TABLES if not t.get("type")]
                    manager.create_multiple_tables(tables)

                elif choice == "3":
                    # Create specific tables
                    tables = [t for t in TABLES if not t.get("type")]
                    if tables:
                        selected = select_objects_interactive(tables, "table")
                        if selected:
                            manager.create_multiple_tables(selected)
                    else:
                        manager.print_error("No tables defined")

                elif choice == "4":
                    # Create all views
                    views = [t for t in TABLES if t.get("type") == "VIEW"]
                    manager.create_multiple_tables(views)

                elif choice == "5":
                    # Create specific views
                    views = [t for t in TABLES if t.get("type") == "VIEW"]
                    if views:
                        selected = select_objects_interactive(views, "view")
                        if selected:
                            manager.create_multiple_tables(selected)
                    else:
                        manager.print_error("No views defined")

                elif choice == "6":
                    # Drop all objects
                    confirm = input(
                        f"{Style.BRIGHT}{Fore.RED}⚠️  WARNING: This will drop ALL database objects. Type 'CONFIRM' to proceed: {Style.RESET_ALL}"
                    )
                    if confirm == "CONFIRM":
                        manager.drop_all_objects()
                    else:
                        manager.print_warning("Operation cancelled")

                elif choice == "7":
                    # Drop specific object
                    print(f"\n{Style.BRIGHT}{Fore.MAGENTA}Select object type:{Style.RESET_ALL}")
                    print("1. Table")
                    print("2. View")

                    type_choice = input("\nEnter choice: ")

                    if type_choice == "1":
                        objects = [t for t in TABLES if not t.get("type")]
                        obj_type = "TABLE"
                    elif type_choice == "2":
                        objects = [t for t in TABLES if t.get("type") == "VIEW"]
                        obj_type = "VIEW"
                    else:
                        manager.print_error("Invalid choice")
                        continue

                    if not objects:
                        manager.print_error(f"No {obj_type.lower()}s found")
                        continue

                    selected = select_objects_interactive(objects, obj_type.lower())
                    if selected:
                        confirm = input(
                            f"{Style.BRIGHT}{Fore.RED}⚠️  Confirm deletion of {len(selected)} object(s) (y/n): {Style.RESET_ALL}"
                        )
                        if confirm.lower() == "y":
                            for obj in selected:
                                manager.drop_table(obj["name"], obj_type)
                        else:
                            manager.print_warning("Deletion cancelled")

                else:
                    manager.print_error("Invalid choice")

            finally:
                manager.disconnect()

        except KeyboardInterrupt:
            print(f"\n{Fore.CYAN}👋 Operation cancelled by user{Style.RESET_ALL}")
            logger.info("Operation cancelled by user")
            break

        except Exception as e:
            manager.print_error(f"Error: {e}")
            logger.error(f"Menu error: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        logger.info("StarRocks Table Manager started")
        display_menu()
    except KeyboardInterrupt:
        print(f"\n{Fore.CYAN}👋 Exiting...{Style.RESET_ALL}")
        logger.info("Application interrupted")
    except Exception as e:
        print(f"{Style.BRIGHT}{Fore.RED}❌ Fatal error: {e}{Style.RESET_ALL}")
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
