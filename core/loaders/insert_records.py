import time
from pathlib import Path
import polars as pl
import pandas as pd
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
CYAN = "\033[36m"

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
    """Load CSV data into StarRocks using Stream Load API

    Args:
        table_name: Target table name
        csv_file_path: Path to CSV file
        chunk_id: Chunk identifier for logging
        columns: List of column names to map CSV columns to database columns (in order)
    """
    # Prepare the Stream Load URL
    url = f"http://{STARROCKS_CONFIG['host']}:{STARROCKS_CONFIG['http_port']}/api/{STARROCKS_CONFIG['database']}/{table_name}/_stream_load"

    # Prepare headers
    headers = {
        "label": f"{table_name}_{int(time.time())}_{chunk_id if chunk_id else ''}",
        "column_separator": "\x01",
        "format": "CSV",
        "max_filter_ratio": str(MAX_ERROR_RATIO),
        "strict_mode": "false",
        "timezone": "Asia/Shanghai",
        "Expect": "100-continue",
    }

    # Add columns specification if provided (CRITICAL for correct column mapping!)
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
            # Get detailed error information
            status = result.get("Status", "Unknown")
            message = result.get("Message", "No message provided")
            total = result.get("NumberTotalRows", 0)
            loaded = result.get("NumberLoadedRows", 0)
            filtered = result.get("NumberFilteredRows", 0)

            # Check if this is a critical failure (too many filtered rows)
            if filtered > 0 and total > 0:
                filter_ratio = (filtered / total) * 100
                is_critical = filter_ratio > (MAX_ERROR_RATIO * 100)
            else:
                is_critical = status == "Fail"

            # Print error in red if critical, yellow if warning
            error_color = RED if is_critical else YELLOW
            error_type = "ERROR" if is_critical else "WARNING"

            print(f"{error_color}❌ Stream Load {error_type} for chunk {chunk_id}:{RESET}")
            print(f"{error_color}  Status: {status}{RESET}")
            print(f"{error_color}  Message: {message}{RESET}")

            if total > 0:
                filter_percentage = (filtered / total) * 100 if total > 0 else 0
                print(f"{error_color}  Total Rows: {total:,}{RESET}")
                print(f"{error_color}  Loaded Rows: {loaded:,}{RESET}")
                print(
                    f"{error_color}  Filtered Rows: {filtered:,} ({filter_percentage:.1f}%){RESET}"
                )

                # Additional diagnostics
                if filtered > 0:
                    print(f"{error_color}  ⚠️  High filter ratio detected!{RESET}")
                    print(f"{error_color}     This typically means:{RESET}")
                    print(f"{error_color}     1. Data types don't match schema columns{RESET}")
                    print(
                        f"{error_color}     2. Column values are NULL when NOT NULL is required{RESET}"
                    )
                    print(
                        f"{error_color}     3. Data values are out of range for the column type{RESET}"
                    )
                    print(
                        f"{error_color}     4. Column count mismatch between CSV and table{RESET}"
                    )

            return False, result

    except Exception as e:
        print(f"{RED}❌ Stream Load ERROR - Exception:{RESET}")
        print(f"{RED}  {str(e)}{RESET}")
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
                print(f"{YELLOW}Deleting fact_invoice_secondary records for RecordType DD{RESET}")
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM fact_invoice_secondary WHERE record_type = 'DD'")
            else:
                print(
                    f"{YELLOW}Deleting fact_invoice_secondary records for SalesGroupCodes: {groups_str}{RESET}"
                )
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"DELETE FROM fact_invoice_secondary WHERE sales_group_code IN ({groups_str}) AND record_type != 'DD'"
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
                f"{YELLOW}Deleting fact_invoice_details records for SalesGroupCodes: {groups_str}{RESET}"
            )
            with conn.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM fact_invoice_details WHERE sales_group_code IN ({groups_str})"
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

        # Determine table name (convert to snake_case for database)
        if "FactInvoiceSecondary" in stem:
            table_name = "fact_invoice_secondary"
        elif "FactInvoiceDetails" in stem:
            table_name = "fact_invoice_details"
        elif "DimCustomerMaster" in stem:
            table_name = "dim_customer_master"
        elif "DimDealerMaster" in stem:
            table_name = "dim_dealer_master"
        elif "DimMaterial" in stem:
            table_name = "dim_material"
        elif "DimHierarchy" in stem:
            table_name = "dim_hierarchy"
        elif "DimSalesGroup" in stem:
            table_name = "dim_sales_group"
        elif "DimMaterialMapping" in stem:
            table_name = "dim_material_mapping"
        elif "RlsMaster" in stem:
            table_name = "rls_master"
        else:
            table_name = stem
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

        # CRITICAL: Validate that parquet types match database schema
        print(f"\n{CYAN}Validating data types...{RESET}")
        try:
            conn = get_starrocks_connection()
            cursor = conn.cursor()
            cursor.execute(f"DESC {table_name}")
            db_columns = cursor.fetchall()

            # Build type mapping from database
            db_type_map = {}
            for col_info in db_columns:
                col_name = col_info[0]
                col_type = col_info[1]
                db_type_map[col_name] = col_type

            # Polars type to SQL type mapping
            polars_to_sql = {
                "Int8": "TINYINT",
                "Int16": "SMALLINT",
                "Int32": "INT",
                "Int64": "BIGINT",
                "Float32": "FLOAT",
                "Float64": "DOUBLE",
                "String": "VARCHAR",
            }

            # Check each column in parquet
            type_errors = []
            for col_name in df.columns:
                parquet_type = str(df[col_name].dtype)
                expected_db_type = db_type_map.get(col_name)

                if expected_db_type is None:
                    continue  # Column might be extra, already checked above

                # Get base SQL type for parquet
                parquet_base_type = polars_to_sql.get(parquet_type, parquet_type.upper())
                expected_base_type = expected_db_type.split("(")[0].upper()

                # Check compatibility
                is_compatible = False
                if parquet_base_type == expected_base_type:
                    is_compatible = True
                elif "INT" in parquet_base_type and "INT" in expected_base_type:
                    is_compatible = True
                elif "DOUBLE" in parquet_base_type and (
                    "DOUBLE" in expected_base_type or "FLOAT" in expected_base_type
                ):
                    is_compatible = True
                elif "VARCHAR" in parquet_base_type and "VARCHAR" in expected_base_type:
                    is_compatible = True

                if not is_compatible:
                    type_errors.append(
                        {
                            "column": col_name,
                            "parquet_type": parquet_type,
                            "db_type": expected_db_type,
                        }
                    )

            if type_errors:
                print(f"\n{RED}❌ CRITICAL: DATA TYPE MISMATCHES - Cannot ingest!{RESET}")
                for error in type_errors:
                    print(
                        f"  {error['column']:<40} Parquet: {error['parquet_type']:<15} Expected: {error['db_type']}"
                    )
                raise ValueError("Data type mismatches detected - see errors above")
            else:
                print(f"{GREEN}✅ All data types match database schema!{RESET}")

            conn.close()
        except Exception as e:
            print(f"{RED}❌ Type validation failed: {e}{RESET}")
            raise

        # Diagnostic: Check CSV schema vs Database schema
        try:
            conn = get_starrocks_connection()
            db_columns = get_table_columns(conn, table_name)
            csv_columns = df.columns
            conn.close()

            # Check for mismatches
            missing_in_db = set(csv_columns) - set(db_columns)
            missing_in_csv = set(db_columns) - set(csv_columns)

            if missing_in_db:
                print(f"{RED}❌ CSV has columns not in {table_name}: {missing_in_db}{RESET}")
            if missing_in_csv:
                print(f"{RED}⚠️  {table_name} has columns not in CSV: {missing_in_csv}{RESET}")

            print(f"{GREEN}✓ CSV columns: {len(csv_columns)}, DB columns: {len(db_columns)}{RESET}")
        except Exception as e:
            print(f"{YELLOW}⚠️  Could not verify schema: {e}{RESET}")

        total_chunks = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
        print(f"{GREEN}Processing {total:,} records in {total_chunks} chunks{RESET}")

        # CRITICAL: Get database columns in correct order for Stream Load mapping
        try:
            conn = get_starrocks_connection()
            with conn.cursor() as cursor:
                cursor.execute(f"DESC {table_name}")
                db_columns_raw = cursor.fetchall()
                db_columns_list = [col[0] for col in db_columns_raw]
            conn.close()

            # Build ordered column list matching database table order
            csv_columns = df.columns
            df_columns_lower = {col.lower(): col for col in csv_columns}

            ordered_columns = []
            ordered_db_columns = []

            for db_col in db_columns_list:
                db_col_lower = db_col.lower()
                if db_col_lower in df_columns_lower:
                    ordered_columns.append(df_columns_lower[db_col_lower])
                    ordered_db_columns.append(db_col)

            # Reorder dataframe to match database table order
            if ordered_columns:
                df = df.select(ordered_columns)

            print(
                f"{GREEN}✓ Column mapping verified - {len(ordered_db_columns)} columns will be loaded{RESET}"
            )
        except Exception as e:
            print(f"{YELLOW}⚠️  Could not verify column order: {e}{RESET}")
            # Fallback: use columns as-is
            ordered_db_columns = list(df.columns)

        successful_chunks = 0
        failed_chunks = 0
        total_rows_loaded = 0
        failed_chunk_details = []

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
                        # Export to CSV with SOH (\x01) delimiter
                        # SOH (Start of Heading) is rarely found in data and avoids issues with commas in fields
                        chunk_pd = chunk.to_pandas()
                        chunk_pd.to_csv(tmp_file.name, sep="\x01", header=False, index=False)

                        # Load using Stream Load with explicit column mapping
                        success, result = stream_load_csv(
                            table_name,
                            tmp_file.name,
                            chunk_id=chunk_num,
                            columns=ordered_db_columns,
                        )

                        if success:
                            successful_chunks += 1
                            rows_loaded = result.get("NumberLoadedRows", len(chunk))
                            total_rows_loaded += rows_loaded
                        else:
                            failed_chunks += 1
                            failed_chunk_details.append(
                                {
                                    "chunk": chunk_num,
                                    "filtered": result.get("NumberFilteredRows", 0),
                                    "total": result.get("NumberTotalRows", 0),
                                    "message": result.get("Message", "Unknown error"),
                                }
                            )

                        # Clean up temp file
                        os.unlink(tmp_file.name)

                except Exception as e:
                    print(f"{RED}Chunk {chunk_num} error: {e}{RESET}")
                    failed_chunks += 1
                    failed_chunk_details.append({"chunk": chunk_num, "error": str(e)})

                pbar.update(1)

        total_time = time.time() - start
        print(f"\n{GREEN}Loaded {total_rows_loaded:,}/{total:,} rows in {total_time:.2f}s{RESET}")

        if failed_chunks > 0:
            print(
                f"{RED}❌ Successful chunks: {successful_chunks}, Failed chunks: {failed_chunks}{RESET}"
            )

            # Show summary of failures
            if failed_chunk_details:
                print(f"\n{RED}Failed Chunk Details:{RESET}")
                for detail in failed_chunk_details:
                    print(f"{RED}  Chunk {detail.get('chunk')}:{RESET}")
                    if "error" in detail:
                        print(f"{RED}    Exception: {detail['error']}{RESET}")
                    else:
                        total_rows = detail.get("total", 0)
                        filtered = detail.get("filtered", 0)
                        if total_rows > 0:
                            filter_pct = (filtered / total_rows) * 100
                            print(
                                f"{RED}    Filtered: {filtered:,}/{total_rows:,} ({filter_pct:.1f}%){RESET}"
                            )
                        print(f"{RED}    Message: {detail.get('message', 'Unknown error')}{RESET}")
        else:
            print(
                f"{GREEN}✓ Successful chunks: {successful_chunks}, Failed chunks: {failed_chunks}{RESET}"
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
