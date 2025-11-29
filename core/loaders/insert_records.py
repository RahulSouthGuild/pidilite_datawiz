import time
from pathlib import Path
import polars as pl
import requests
import tempfile
import os
from tqdm import tqdm
import sys
import pymysql
from colorama import init

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.DB_CONFIG import DB_CONFIG  # noqa: E402

init(autoreset=True)

print(DB_CONFIG)

RED, GREEN, YELLOW, RESET = "\033[31m", "\033[32m", "\033[33m", "\033[0m"

# StarRocks Stream Load Configuration
STARROCKS_CONFIG = {
    "host": DB_CONFIG["host"],
    "mysql_port": DB_CONFIG["port"],
    "http_port": int(os.getenv("STARROCKS_HTTP_PORT", "8040")),
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "database": DB_CONFIG["database"],
}

# Stream Load settings
STREAM_LOAD_TIMEOUT = 1800  # 30 minutes
MAX_ERROR_RATIO = 0.1  # 10% error tolerance
CHUNK_SIZE = 100000  # Records per chunk


def stream_load_csv(table_name, csv_file_path, chunk_id=None, columns=None):
    """Load CSV data into StarRocks using Stream Load API"""
    # Prepare the Stream Load URL
    url = f"http://{STARROCKS_CONFIG['host']}:{STARROCKS_CONFIG['http_port']}/api/{STARROCKS_CONFIG['database']}/{table_name}/_stream_load"

    # Prepare headers
    headers = {
        "label": f"{table_name}_{int(time.time())}_{chunk_id if chunk_id else ''}",
        "column_separator": ",",
        "format": "CSV",
        "max_filter_ratio": str(MAX_ERROR_RATIO),
        "strict_mode": "false",
        "timezone": "Asia/Shanghai",
        "Expect": "100-continue",
    }

    # Add columns specification if provided
    if columns:
        headers["columns"] = ",".join(columns)

    # Authentication
    auth = (STARROCKS_CONFIG["user"], STARROCKS_CONFIG["password"])

    try:
        # Read the file
        with open(csv_file_path, "rb") as f:
            file_data = f.read()

        # Execute Stream Load
        response = requests.put(
            url, headers=headers, data=file_data, auth=auth, timeout=STREAM_LOAD_TIMEOUT
        )

        # Parse response
        result = response.json()

        if result.get("Status") == "Success":
            return True, result
        else:
            print(f"{YELLOW}Stream Load warning for chunk {chunk_id}:{RESET}")
            print(f"{YELLOW}  Status: {result.get('Status')}{RESET}")
            print(f"{YELLOW}  Message: {result.get('Message')}{RESET}")
            if "NumberTotalRows" in result:
                print(
                    f"{YELLOW}  Total: {result.get('NumberTotalRows')}, Loaded: {result.get('NumberLoadedRows')}, Filtered: {result.get('NumberFilteredRows')}{RESET}"
                )
            return False, result

    except Exception as e:
        return False, {"Message": str(e)}


def get_starrocks_connection():
    """Create StarRocks MySQL connection for metadata operations"""
    return pymysql.connect(
        host=STARROCKS_CONFIG["host"],
        port=STARROCKS_CONFIG["mysql_port"],
        user=STARROCKS_CONFIG["user"],
        password=STARROCKS_CONFIG["password"],
        database=STARROCKS_CONFIG["database"],
        charset="utf8mb4",
        autocommit=True,
    )


def get_table_columns(conn, table_name):
    """Get column names from StarRocks table"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DESC {table_name}")
            columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"{RED}Error getting table columns: {e}{RESET}")
        return []


def delete_existing_records(table_name, stem):
    """Delete existing records based on table type"""
    conn = None
    try:
        conn = get_starrocks_connection()

        if "FactInvoiceSecondary" in stem:
            parts = stem.split("_")
            sales_groups = parts[1:]
            groups_str = ", ".join(f"'{g}'" for g in sales_groups)

            if "DD" in stem:
                print(f"{YELLOW}Deleting FactInvoiceSecondary records for RecordType DD{RESET}")
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM FactInvoiceSecondary WHERE RecordType = 'DD'")
            else:
                print(
                    f"{YELLOW}Deleting FactInvoiceSecondary records for SalesGroupCodes: {groups_str}{RESET}"
                )
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"DELETE FROM FactInvoiceSecondary WHERE SalesGroupCode IN ({groups_str}) AND RecordType != 'DD'"
                    )
            print(f"{GREEN}Deleted records{RESET}")

        elif "FactInvoiceDetails" in stem:
            parts = stem.split("_")
            sales_groups = parts[1:]
            if "107" in sales_groups and "112" in sales_groups:
                additional_groups = [
                    "101",
                    "203",
                    "204",
                    "202",
                    "205",
                    "302",
                    "303",
                    "304",
                    "408",
                    "409",
                    "402",
                    "403",
                    "407",
                    "451",
                    "452",
                    "453",
                    "454",
                    "455",
                    "456",
                    "457",
                    "501",
                    "502",
                    "503",
                    "504",
                    "505",
                    "506",
                    "508",
                    "509",
                    "601",
                    "602",
                    "604",
                    "956",
                    "957",
                    "961",
                    "949",
                    "951",
                    "958",
                    "959",
                    "960",
                ]
                sales_groups.extend([g for g in additional_groups if g not in sales_groups])
            groups_str = ", ".join(f"'{g}'" for g in sales_groups)
            print(
                f"{YELLOW}Deleting FactInvoiceDetails records for SalesGroupCodes: {groups_str}{RESET}"
            )
            with conn.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM FactInvoiceDetails WHERE SalesGroupCode IN ({groups_str})"
                )
            print(f"{GREEN}Deleted records{RESET}")
        else:
            print(f"{YELLOW}Truncating {table_name}...{RESET}")
            with conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {table_name}")
            print(f"{GREEN}Truncated {table_name}{RESET}")
    except Exception as e:
        print(f"{RED}Error deleting records: {e}{RESET}")
    finally:
        if conn:
            conn.close()


def process_records(file, delete_existing=True):
    """Process parquet file and load to StarRocks using Stream Load API"""
    start = time.time()
    try:
        print(f"{GREEN}Processing: {file.name}{RESET}")
        stem = file.stem

        # Determine table name
        table_name = (
            "FactInvoiceSecondary"
            if "FactInvoiceSecondary" in stem
            else (
                "FactInvoiceDetails"
                if "FactInvoiceDetails" in stem
                else "DimDealerMaster" if "DimDealer" in stem else stem
            )
        )
        print(f"{YELLOW}Table: {table_name}{RESET}")

        # Delete existing records if requested
        if delete_existing:
            delete_existing_records(table_name, stem)

        # Read parquet file
        df = pl.read_parquet(file)
        total = df.height

        if total == 0:
            print(f"{YELLOW}No records to process{RESET}")
            return

        total_chunks = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
        print(f"{GREEN}Processing {total:,} records in {total_chunks} chunks{RESET}")

        # Get table columns for Stream Load
        conn = get_starrocks_connection()
        table_columns = get_table_columns(conn, table_name)
        conn.close()

        # Sanitize column names
        df_columns_lower = {col.lower(): col for col in df.columns}
        ordered_columns = []
        actual_columns = []

        for table_col in table_columns:
            table_col_lower = table_col.lower()
            if table_col_lower in df_columns_lower:
                ordered_columns.append(df_columns_lower[table_col_lower])
                actual_columns.append(table_col)

        if not ordered_columns:
            print(f"{RED}No matching columns found in dataframe{RESET}")
            return

        df = df[ordered_columns]

        successful_chunks = 0
        failed_chunks = 0
        total_rows_loaded = 0

        with tqdm(total=total_chunks, desc="Loading chunks", ncols=100) as pbar:
            for chunk_num, start_row in enumerate(range(0, total, CHUNK_SIZE), 1):
                try:
                    chunk = df.slice(start_row, CHUNK_SIZE)

                    if chunk.is_empty():
                        pbar.update(1)
                        continue

                    # Create temporary CSV file
                    with tempfile.NamedTemporaryFile(
                        mode="w", suffix=".csv", delete=False
                    ) as tmp_file:
                        chunk.write_csv(tmp_file.name, include_header=False)

                        # Load using Stream Load
                        success, result = stream_load_csv(
                            table_name, tmp_file.name, chunk_id=chunk_num, columns=actual_columns
                        )

                        if success:
                            successful_chunks += 1
                            rows_loaded = result.get("NumberLoadedRows", len(chunk))
                            total_rows_loaded += rows_loaded
                        else:
                            failed_chunks += 1

                        # Clean up temp file
                        os.unlink(tmp_file.name)

                except Exception as e:
                    print(f"{RED}Chunk {chunk_num} error: {e}{RESET}")
                    failed_chunks += 1

                pbar.update(1)

        total_time = time.time() - start
        print(f"{GREEN}Loaded {total_rows_loaded:,}/{total:,} rows in {total_time:.2f}s{RESET}")
        print(
            f"{GREEN}Successful chunks: {successful_chunks}, Failed chunks: {failed_chunks}{RESET}"
        )

    except Exception as e:
        print(f"{RED}Processing error: {str(e)}{RESET}")
        raise


def list_files():
    """List available parquet files"""
    try:
        start_time = time.time()
        cleaned_parquet_path = PROJECT_ROOT / "data" / "data_historical" / "cleaned_parquets"
        parquet_files = list(cleaned_parquet_path.rglob("*.parquet"))
        if not parquet_files:
            print(f"{YELLOW}No parquet files found in {cleaned_parquet_path}.{RESET}")
            return None
        files_dict = {"fact": [], "other": []}
        for f in parquet_files:
            key = "fact" if "FactInvoiceSecondary" in f.stem else "other"
            files_dict[key].append(f)
        files = files_dict["other"] + files_dict["fact"]
        for i, f in enumerate(files, 1):
            try:
                size_bytes = f.stat().st_size
                if size_bytes < 1024:
                    size_str = f"{size_bytes} B"
                elif size_bytes < 1024 * 1024:
                    size_str = f"{size_bytes / 1024:.2f} KB"
                elif size_bytes < 1024 * 1024 * 1024:
                    size_str = f"{size_bytes / (1024 * 1024):.2f} MB"
                else:
                    size_str = f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
                # Optimized: Use scan_parquet to get metadata without loading data
                lf = pl.scan_parquet(f)
                # Get row count from metadata without loading any data
                records = lf.select(pl.len()).collect().item()
                if records < 1000:
                    count_str = f"{records}"
                elif records < 100000:
                    count_str = f"{records / 1000:.1f}K"
                elif records < 10000000:
                    count_str = f"{records / 100000:.1f} lakh"
                else:
                    count_str = f"{records / 10000000:.2f} crore"
                rel_path = f.relative_to(PROJECT_ROOT)
                print(f"{i}. {rel_path} - {size_str}, {count_str} records")
            except Exception as e:
                print(f"{i}. {f.name} - {RED}Error: {str(e)}{RESET}")
        print(f"\n{GREEN}Processed in {time.time() - start_time:.2f}s{RESET}")
        print("\n0. Exit")
        return files
    except Exception as e:
        print(f"{RED}Error listing files: {str(e)}{RESET}")
        return None


def process_file_menu():
    """Interactive menu for file selection and processing"""
    while True:
        print("\n1. Process Specific File")
        print("2. Process All Files")
        print("0. Exit")
        choice = input("Enter choice: ").strip()
        if choice == "0":
            break
        elif choice == "1":
            while True:
                files = list_files()
                if not files:
                    break
                file_choice = input("Enter file number (0 to exit): ").strip()
                if file_choice == "0":
                    break
                try:
                    idx = int(file_choice) - 1
                    if 0 <= idx < len(files):
                        opt = input("Delete existing records? (y/n): ").strip().lower()
                        delete_existing = opt == "y"
                        process_records(files[idx], delete_existing)
                    else:
                        print(f"{RED}Invalid selection{RESET}")
                except Exception as e:
                    print(f"{RED}Invalid input: {str(e)}{RESET}")
                again = input("Process another? (y/n): ").strip().lower()
                if again != "y":
                    break
        elif choice == "2":
            files = list_files()
            if files:
                for file in files:
                    try:
                        print(f"{YELLOW}Processing {file.name}{RESET}")
                        process_records(file)
                    except Exception as e:
                        print(f"{RED}Error processing {file.name}: {str(e)}{RESET}")
        else:
            print(f"{RED}Invalid choice{RESET}")


def main():
    """Main function"""
    try:
        process_file_menu()
    except Exception as e:
        print(f"{RED}Fatal error: {str(e)}{RESET}")


if __name__ == "__main__":
    main()
