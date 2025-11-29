import os
import shutil
import asyncio
from pathlib import Path
import polars as pl
import time
from typing import List
import sys

from colorama import init, Fore, Style

init(autoreset=True)

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.schema_validator import SchemaValidator  # noqa: E402

# Initialize schema validator with schema files from db/schemas and column mappings
SCHEMAS_DIR = Path(__file__).parent.parent.parent / "db" / "schemas"
COLUMN_MAPPINGS_DIR = Path(__file__).parent.parent.parent / "db" / "column_mappings"
validator = SchemaValidator.from_schema_files(SCHEMAS_DIR, COLUMN_MAPPINGS_DIR)


def get_table_name_from_file(file_stem: str) -> str:
    """Map parquet filename to database table name."""
    if "FactInvoiceSecondary" in file_stem:
        return "FactInvoiceSecondary"
    elif "FactInvoiceDetails" in file_stem:
        return "FactInvoiceDetails"
    elif "DimDealer" in file_stem:
        return "DimDealerMaster"
    else:
        # Try to use stem as-is
        return file_stem


def determine_chunk_size(file_size_mb):
    """
    Determine chunk size based on file size - optimized for 64GB RAM system.

    Strategy:
    - Large files (>5GB): Use smaller chunks to avoid memory spike
    - Medium files (1-5GB): Larger chunks for better throughput
    - Small files (<1GB): Process in single pass for speed
    """
    if file_size_mb > 5000:  # > 5GB - be conservative
        return 500000  # 500K records per chunk
    elif file_size_mb > 2000:  # 2-5GB
        return 1000000  # 1M records per chunk
    elif file_size_mb > 1000:  # 1-2GB
        return 2000000  # 2M records per chunk
    elif file_size_mb > 500:  # 500MB-1GB
        return 3000000  # 3M records per chunk
    elif file_size_mb > 100:  # 100MB-500MB
        return 5000000  # 5M records per chunk
    else:
        return None  # Process entire file at once (< 100MB)


async def process_large_file_chunked(parquet_file, output_dir, chunk_size):
    """Process large files in chunks with optimized memory usage - streaming approach"""
    try:
        start_time = time.time()
        print(f"\n{Fore.CYAN}Processing large file in chunks: {parquet_file}{Style.RESET_ALL}")

        # Get table name from file
        table_name = get_table_name_from_file(parquet_file.stem)

        # Read file lazily
        lf = pl.scan_parquet(parquet_file)
        total_rows = lf.select(pl.len()).collect().item()

        print(f"{Fore.CYAN}Total rows: {total_rows}, Chunk size: {chunk_size}{Style.RESET_ALL}")

        # Prepare output path
        output_path = output_dir / parquet_file.name
        if output_path.exists():
            output_path.unlink()

        # Create temporary directory for chunks
        temp_dir = output_dir / f"temp_{parquet_file.stem}_{int(time.time())}"
        temp_dir.mkdir(exist_ok=True)

        try:
            chunk_files = []

            # Process all chunks first
            for i in range(0, total_rows, chunk_size):
                chunk_num = i // chunk_size + 1
                print(
                    f"{Fore.CYAN}Processing chunk {chunk_num}/{(total_rows + chunk_size - 1) // chunk_size}{Style.RESET_ALL}"
                )

                # Process chunk
                df_chunk = lf.slice(i, chunk_size).collect()

                # Apply validation
                df_chunk = await validate_and_transform_dataframe(df_chunk, table_name)

                # Save chunk to temp directory
                chunk_file = temp_dir / f"chunk_{chunk_num:06d}.parquet"
                df_chunk.write_parquet(chunk_file)
                chunk_files.append(chunk_file)

                # Clear chunk from memory immediately
                del df_chunk

            # Now combine all chunks using lazy evaluation
            print(
                f"{Fore.CYAN}Combining {len(chunk_files)} chunks using streaming{Style.RESET_ALL}"
            )

            if chunk_files:
                # Use lazy scanning for memory efficiency
                lazy_frames = [pl.scan_parquet(chunk_file) for chunk_file in chunk_files]
                combined_lf = pl.concat(lazy_frames)

                # Stream write the final result
                combined_lf.sink_parquet(output_path)

            elapsed_time = time.time() - start_time
            print(
                f"{Fore.GREEN}Written chunked parquet to {output_path} in {elapsed_time:.2f} seconds{Style.RESET_ALL}"
            )

        finally:
            # Clean up temporary directory
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

    except Exception as e:
        print(f"{Fore.RED}Error processing chunked file {parquet_file}: {e}{Style.RESET_ALL}")
        raise


async def process_large_file_chunked_optimized(parquet_file, output_dir, chunk_size):
    """Most memory-efficient approach with progressive combining"""
    try:
        start_time = time.time()
        print(
            f"\n{Fore.CYAN}Processing large file in chunks (optimized): {parquet_file}{Style.RESET_ALL}"
        )

        # Get table name from file
        table_name = get_table_name_from_file(parquet_file.stem)

        # Read file lazily
        lf = pl.scan_parquet(parquet_file)
        total_rows = lf.select(pl.len()).collect().item()

        print(f"{Fore.CYAN}Total rows: {total_rows}, Chunk size: {chunk_size}{Style.RESET_ALL}")

        # Prepare output path
        output_path = output_dir / parquet_file.name
        if output_path.exists():
            output_path.unlink()

        # Create temporary directory for chunks
        temp_dir = output_dir / f"temp_{parquet_file.stem}_{int(time.time())}"
        temp_dir.mkdir(exist_ok=True)

        try:
            chunk_files = []
            merge_threshold = 5  # Merge every 5 chunks to keep memory low

            # Process chunks with progressive merging
            for i in range(0, total_rows, chunk_size):
                chunk_num = i // chunk_size + 1
                print(
                    f"{Fore.CYAN}Processing chunk {chunk_num}/{(total_rows + chunk_size - 1) // chunk_size}{Style.RESET_ALL}"
                )

                # Process chunk
                df_chunk = lf.slice(i, chunk_size).collect()

                # Apply validation
                df_chunk = await validate_and_transform_dataframe(df_chunk, table_name)

                # Save chunk to temp directory
                chunk_file = temp_dir / f"chunk_{chunk_num:06d}.parquet"
                df_chunk.write_parquet(chunk_file)
                chunk_files.append(chunk_file)

                # Clear chunk from memory immediately
                del df_chunk

                # Progressive merging to keep chunk count manageable
                if len(chunk_files) >= merge_threshold:
                    print(
                        f"{Fore.CYAN}Merging {len(chunk_files)} intermediate chunks{Style.RESET_ALL}"
                    )
                    merged_file = await _merge_chunks_efficiently(chunk_files, temp_dir)
                    chunk_files = [merged_file]

            # Final combination
            print(f"{Fore.CYAN}Final combination of {len(chunk_files)} chunks{Style.RESET_ALL}")

            if len(chunk_files) == 1:
                # Just move the single file
                shutil.move(str(chunk_files[0]), str(output_path))
            else:
                # Combine remaining chunks
                lazy_frames = [pl.scan_parquet(chunk_file) for chunk_file in chunk_files]
                combined_lf = pl.concat(lazy_frames)
                combined_lf.sink_parquet(output_path)

            elapsed_time = time.time() - start_time
            print(
                f"{Fore.GREEN}Written optimized chunked parquet to {output_path} in {elapsed_time:.2f} seconds{Style.RESET_ALL}"
            )

        finally:
            # Clean up temporary directory
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

    except Exception as e:
        print(
            f"{Fore.RED}Error processing optimized chunked file {parquet_file}: {e}{Style.RESET_ALL}"
        )
        raise


async def _merge_chunks_efficiently(chunk_files, temp_dir):
    """Efficiently merge chunks using lazy evaluation"""
    if len(chunk_files) <= 1:
        return chunk_files[0] if chunk_files else None

    # Use lazy frames for memory efficiency
    lazy_frames = [pl.scan_parquet(chunk_file) for chunk_file in chunk_files]
    combined_lf = pl.concat(lazy_frames)

    # Create merged file
    merged_file = temp_dir / f"merged_{int(time.time() * 1000000)}.parquet"
    combined_lf.sink_parquet(merged_file)

    # Clean up individual chunks
    for chunk_file in chunk_files:
        try:
            chunk_file.unlink()
        except Exception:
            pass  # Ignore cleanup errors

    return merged_file


def configure_polars_for_low_memory():
    """
    Configure Polars for memory-efficient operations on 64GB RAM system.

    Optimizations:
    - Use all available CPU cores (parallelism)
    - Larger streaming chunks for better throughput
    - Increased memory budget for better performance
    """
    # Set larger streaming chunk size for better performance with 64GB RAM
    pl.Config.set_streaming_chunk_size(100000)  # 100K rows per chunk (up from 25K)

    # Use all CPU cores for maximum parallelism
    cpu_count = os.cpu_count()
    os.environ["POLARS_MAX_THREADS"] = str(cpu_count)

    print(f"{Fore.CYAN}Polars configured for {cpu_count} CPU cores with 64GB RAM{Style.RESET_ALL}")

    # Enable streaming engine for memory efficiency
    try:
        pl.Config.set_streaming_engine(True)
    except Exception:
        pass  # Ignore if not available


async def validate_and_transform_dataframe(df: pl.DataFrame, table_name: str) -> pl.DataFrame:
    """
    Validate dataframe against database schema limits and apply type conversions.
    Automatically upgrades schema when data exceeds limits.
    Renames columns that conflict with StarRocks reserved keywords.

    Args:
        df: Polars DataFrame from parquet file
        table_name: Database table name for schema lookup

    Returns:
        Validated and potentially transformed dataframe

    Raises:
        ValueError: If validation fails critically
    """
    print(f"{Fore.CYAN}Validating data against table schema: {table_name}{Style.RESET_ALL}")

    # Step 1: Reserved keywords are now handled via quoted column names in the database schema
    # The schema file uses quotes (e.g., "Div", "Sal") to allow reserved words as column names
    # No renaming needed here - columns stay as-is in the parquet file

    # Step 2: Validate against schema
    is_valid, error_msg, transformed_df = validator.validate_dataframe_against_schema(
        df, table_name
    )

    if not is_valid:
        error_msg = f"{Fore.RED}Validation Error for {table_name}: {error_msg}{Style.RESET_ALL}"
        print(error_msg)
        raise ValueError(error_msg)

    print(f"{Fore.GREEN}Validation passed for {table_name}{Style.RESET_ALL}")
    return transformed_df


async def process_parquet_file(parquet_file, output_dir):
    try:
        start_time = time.time()
        print(parquet_file)
        print(f"\n{Fore.CYAN}Processing file: {parquet_file}{Style.RESET_ALL}")

        # Get table name from file
        table_name = get_table_name_from_file(parquet_file.stem)

        # Check file size for chunking decision
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        chunk_size = determine_chunk_size(file_size_mb)

        if chunk_size:
            print(
                f"{Fore.CYAN}File size: {file_size_mb:.2f}MB - Using optimized chunked processing{Style.RESET_ALL}"
            )
            # Use the optimized version instead
            await process_large_file_chunked_optimized(parquet_file, output_dir, chunk_size)
            return

        # Regular processing for smaller files
        print(
            f"{Fore.CYAN}File size: {file_size_mb:.2f}MB - Using regular processing{Style.RESET_ALL}"
        )

        df = pl.read_parquet(parquet_file)
        print(f"{Fore.GREEN}Read {parquet_file} successfully{Style.RESET_ALL}")

        # Apply validation
        df = await validate_and_transform_dataframe(df, table_name)

        output_path = output_dir / parquet_file.name
        if output_path.exists():
            output_path.unlink()
        df.write_parquet(output_path)

        elapsed_time = time.time() - start_time
        print(
            f"{Fore.GREEN}Written cleaned parquet to {output_path} in {elapsed_time:.2f} seconds{Style.RESET_ALL}"
        )
    except Exception as e:
        print(f"{Fore.RED}Error processing {parquet_file}: {e}{Style.RESET_ALL}")
        raise


async def process_batch(files, output_dir, semaphore):
    async with semaphore:
        tasks = []
        for file in files:
            task = asyncio.create_task(process_parquet_file(file, output_dir))
            tasks.append(task)
        return await asyncio.gather(*tasks, return_exceptions=True)


async def display_file_menu(files: List[Path]) -> List[int]:
    """Display file menu and return list of selected file indices"""
    print("\nAvailable files:")
    for idx, file in enumerate(files, 1):
        print(f"{idx}. {file.name}")
    print("0. Exit")

    while True:
        try:
            choice_input = input(
                "\nEnter file number(s) to process (comma-separated for multiple, 0 to exit): "
            )

            # Handle exit
            if choice_input.strip() == "0":
                return [0]

            # Parse comma-separated choices
            choices_str = choice_input.split(",")
            selected_indices = []

            for choice_str in choices_str:
                choice = int(choice_str.strip())
                if not (1 <= choice <= len(files)):
                    print(
                        f"{Fore.RED}Invalid choice: {choice}. Please enter numbers between 1 and {len(files)}{Style.RESET_ALL}"
                    )
                    break
                selected_indices.append(choice)
            else:
                # All choices were valid
                if selected_indices:
                    return selected_indices
                else:
                    print(f"{Fore.RED}Please enter at least one valid file number{Style.RESET_ALL}")

        except ValueError:
            print(
                f"{Fore.RED}Please enter valid numbers separated by commas (e.g., 1,3,5){Style.RESET_ALL}"
            )


async def main():
    start_time = time.time()
    try:
        # Configure Polars for memory efficiency
        configure_polars_for_low_memory()

        base_dir = PROJECT_ROOT / "data" / "data_historical"
        raw_dir = base_dir / "raw_parquets"
        clean_dir = base_dir / "cleaned_parquets"
        if clean_dir.exists():
            shutil.rmtree(clean_dir)
        print(f"{Fore.CYAN}Creating necessary directories{Style.RESET_ALL}")
        os.makedirs(clean_dir, exist_ok=True)

        print(
            f"{Fore.CYAN}Schema validator initialized with {len(validator.tables)} tables from tables.py{Style.RESET_ALL}"
        )

        files_to_process = list(raw_dir.glob("**/*.parquet"))
        total_files = len(files_to_process)

        while True:
            print(f"\n{Fore.CYAN}Menu Options:{Style.RESET_ALL}")
            print("1. Process all files")
            print("2. Select specific file")
            print("3. Exit")

            try:
                choice = int(input("\nEnter your choice (1-3): "))

                if choice == 1:
                    print(f"{Fore.CYAN}Found {total_files} files to process{Style.RESET_ALL}")
                    # Process all files
                    chunk_size = 1
                    max_concurrent_tasks = 1
                    semaphore = asyncio.Semaphore(max_concurrent_tasks)

                    for i in range(0, total_files, chunk_size):
                        chunk = files_to_process[i : i + chunk_size]
                        print(
                            f"{Fore.CYAN}Processing chunk {i // chunk_size + 1}/{(total_files + chunk_size - 1) // chunk_size}{Style.RESET_ALL}"
                        )
                        results = await process_batch(chunk, clean_dir, semaphore)

                        for result, file in zip(results, chunk):
                            if isinstance(result, Exception):
                                print(
                                    f"{Fore.RED}Error processing {file}: {result}{Style.RESET_ALL}"
                                )

                elif choice == 2:
                    selected_indices = await display_file_menu(files_to_process)

                    # Check if user chose to exit
                    if selected_indices == [0]:
                        continue

                    # Process selected files
                    print(
                        f"{Fore.CYAN}Selected {len(selected_indices)} file(s) to process{Style.RESET_ALL}"
                    )
                    semaphore = asyncio.Semaphore(1)

                    for idx in selected_indices:
                        selected_file = files_to_process[idx - 1]
                        print(
                            f"{Fore.CYAN}Processing file {idx} of {len(selected_indices)}: {selected_file.name}{Style.RESET_ALL}"
                        )
                        try:
                            await process_parquet_file(selected_file, clean_dir)
                        except Exception as e:
                            print(
                                f"{Fore.RED}Error processing {selected_file.name}: {e}{Style.RESET_ALL}"
                            )

                elif choice == 3:
                    print(f"{Fore.GREEN}Exiting program{Style.RESET_ALL}")
                    sys.exit(0)

                else:
                    print(f"{Fore.RED}Invalid choice. Please enter 1, 2, or 3{Style.RESET_ALL}")

            except ValueError:
                print(f"{Fore.RED}Please enter a valid number{Style.RESET_ALL}")

    except Exception as e:
        print(f"{Fore.RED}Error in main: {e}{Style.RESET_ALL}")
    finally:
        # Display schema change summary
        try:
            summary = validator.get_schema_change_summary()
            if summary:
                print(f"\n{Fore.YELLOW}{'='*80}{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}SCHEMA CHANGES SUMMARY{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}{'='*80}{Style.RESET_ALL}")
                print(summary)
                print(f"{Fore.YELLOW}{'='*80}{Style.RESET_ALL}\n")
        except Exception as e:
            print(f"{Fore.RED}Error displaying schema summary: {e}{Style.RESET_ALL}")

        # Display and save ALTER TABLE statements
        try:
            alter_stmts = validator.get_alter_table_statements()
            if alter_stmts:
                validator.print_alter_statements()
                validator.save_alter_statements_to_file()
        except Exception as e:
            print(f"{Fore.RED}Error handling ALTER statements: {e}{Style.RESET_ALL}")

        end_time = time.time()
        print(f"{Fore.GREEN}Operation time: {end_time - start_time:.2f} seconds{Style.RESET_ALL}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"{Fore.RED}Unhandled error: {e}{Style.RESET_ALL}")
