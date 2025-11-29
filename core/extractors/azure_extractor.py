from azure.storage.blob.aio import BlobServiceClient
import asyncio
import os
import aiofiles
import time
from colorama import init, Fore
from pathlib import Path

init(autoreset=True)


PROJECT_ROOT = Path(__file__).parent.parent


async def decompress_file(local_path: str, output_path: str, max_retries: int = 3) -> bool:
    start = time.perf_counter()
    retries = 0

    while retries <= max_retries:
        try:
            compressed_size = os.path.getsize(local_path)
            escaped_local_path = local_path.replace(" ", "\\ ")
            escaped_output_path = output_path.replace(" ", "\\ ")

            cmd = f"gzip -d -c {escaped_local_path} > {escaped_output_path}"
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                shell=True,
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                raise Exception(f"gzip failed: {stderr.decode()}")

            uncompressed_size = os.path.getsize(output_path)
            elapsed = time.perf_counter() - start
            print(
                f"{Fore.BLUE}Decompressed: {compressed_size / 1024 / 1024:.1f}MB → {uncompressed_size / 1024 / 1024:.1f}MB in {elapsed:.2f}s"
            )
            os.remove(local_path)
            return True
        except Exception as e:
            retries += 1
            if retries <= max_retries:
                wait_time = retries * 2
                print(
                    f"{Fore.YELLOW}Decompression failed for {local_path}: {str(e)}. Retrying in {wait_time}s... (Attempt {retries}/{max_retries})"
                )
                await asyncio.sleep(wait_time)
            else:
                print(
                    f"{Fore.RED}Decompression failed after {max_retries} attempts for {local_path}: {str(e)}"
                )
                return False


async def download_blob(blob_client, blob_path: str) -> bytes:
    try:
        print(f"{Fore.YELLOW}⬇️  Downloading: {Fore.CYAN}{blob_path}")
        blob_data = await blob_client.download_blob()
        size = blob_data.size
        content = await blob_data.readall()
        print(f"{Fore.CYAN}Downloaded: {size / 1024 / 1024:.1f}MB")
        return content
    except Exception as e:
        print(f"{Fore.RED}Download error for {blob_path}: {str(e)}")
        return None


async def process_blob(
    blob_path: str,
    client: BlobServiceClient,
    container_name: str,
    download_semaphore: asyncio.Semaphore,
    decompress_semaphore: asyncio.Semaphore,
    base_path: str,
) -> bool:
    start = time.perf_counter()
    try:
        container = client.get_container_client(container_name)
        blob = container.get_blob_client(blob_path)

        local_path = os.path.join(base_path, blob_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        output_path = (
            local_path.rsplit(".", 2)[0] + ".csv"
            if local_path.lower().endswith(".gz")
            else local_path
        )

        if os.path.exists(local_path):
            os.remove(local_path)
        if os.path.exists(output_path):
            os.remove(output_path)

        async with download_semaphore:
            content = await download_blob(blob, blob_path)
            if not content:
                return False

        print(f"{Fore.YELLOW}Saving to: {Fore.CYAN}{local_path}")
        async with aiofiles.open(local_path, "wb") as f:
            await f.write(content)

        if local_path.lower().endswith(".gz"):
            async with decompress_semaphore:
                if not await decompress_file(local_path, output_path):
                    return False

        elapsed = time.perf_counter() - start
        size = os.path.getsize(output_path)
        print(
            f"{Fore.GREEN}✓ {Fore.YELLOW}{output_path} {Fore.GREEN}[{Fore.CYAN}{size / 1024 / 1024:.1f}MB in {elapsed:.2f}s{Fore.GREEN}]"
        )
        return True
    except Exception as e:
        print(f"{Fore.RED}✗ {Fore.YELLOW}{blob_path}: {str(e)}")
        return False


async def get_blobs(client: BlobServiceClient, container_name: str, folders: list) -> list:
    try:
        container = client.get_container_client(container_name)
        blobs = []
        total_size = 0
        for folder in folders:
            async for blob in container.list_blobs(name_starts_with=folder):
                blobs.append((blob.name, blob.size))
                total_size += blob.size

        # Sort blobs by name for consistent display
        blobs.sort(key=lambda x: x[0])

        print(
            f"{Fore.CYAN}Found: {Fore.GREEN}{len(blobs)} files ({total_size / 1024 / 1024:.2f}MB total)"
        )
        return blobs
    except Exception as e:
        print(f"{Fore.RED}Listing error: {str(e)}")
        return []


async def get_missing_blobs(blobs, base_path):
    missing_blobs = []
    for blob_name, blob_size in blobs:
        output_path = os.path.join(
            base_path,
            (
                blob_name.rsplit(".", 2)[0] + ".csv"
                if blob_name.lower().endswith(".gz")
                else blob_name
            ),
        )
        if not os.path.exists(output_path):
            missing_blobs.append((blob_name, blob_size))
    return missing_blobs


async def find_unextracted_files(base_path):
    start = time.perf_counter()
    try:
        walk_result = await asyncio.to_thread(lambda: list(os.walk(base_path)))
        unextracted_files = []
        for root, _, files in walk_result:
            for file in files:
                if file.lower().endswith(".gz"):
                    gz_path = os.path.join(root, file)
                    unextracted_files.append(gz_path)

        # Sort the unextracted files for better readability
        unextracted_files.sort()

        elapsed = time.perf_counter() - start
        print(
            f"{Fore.BLUE}Scanned: {len(unextracted_files)} unextracted .gz files in {elapsed:.2f}s"
        )
        return unextracted_files
    except Exception as e:
        print(f"{Fore.RED}Error scanning files: {str(e)}")
        return []


async def select_blobs(blobs):
    print(f"{Fore.CYAN}Available files:")
    sorted_blobs = sorted(blobs, key=lambda x: x[0])
    for i, (blob_name, blob_size) in enumerate(sorted_blobs):
        print(
            f"{Fore.GREEN}{i + 1}. {Fore.YELLOW}{blob_name} {Fore.CYAN}({blob_size / 1024 / 1024:.2f}MB)"
        )

    selected = input(f"{Fore.MAGENTA}Enter file numbers to download (comma-separated, or 'all'): ")
    if selected.strip().lower() == "all":
        return [b[0] for b in sorted_blobs]

    try:
        indices = [int(idx.strip()) - 1 for idx in selected.split(",")]
        return [sorted_blobs[i][0] for i in indices if 0 <= i < len(sorted_blobs)]
    except Exception as e:
        print(f"{Fore.RED}Invalid selection: {str(e)}")
        return []


async def display_menu():
    print(f"\n{Fore.CYAN}=== Blob Downloader Options ===")
    print(f"{Fore.GREEN}1. Download all files")
    print(f"{Fore.GREEN}2. Download missing files only")
    print(f"{Fore.GREEN}3. Select specific files to download")
    print(f"{Fore.GREEN}4. Extract downloaded .gz files")
    print(f"{Fore.GREEN}5. Exit")

    choice = input(f"{Fore.MAGENTA}Enter your choice (1-5): ")
    try:
        return int(choice)
    except ValueError:
        print(f"{Fore.RED}Invalid choice. Please enter a number between 1 and 5.")
        return 0


async def main():
    start = time.perf_counter()
    FILES_PATH = PROJECT_ROOT.parent
    base_path = FILES_PATH / "data" / "data_historical" / "raw"
    print(f"{Fore.MAGENTA}Base path: {base_path}")
    account_url = "https://pidaimlblobstorage.blob.core.windows.net"
    container_name = "synapsedataprod"
    sas_token = "?sp=rl&st=2024-12-26T12:04:11Z&se=2027-12-26T20:04:11Z&spr=https&sv=2022-11-02&sr=c&sig=N4QWqJf6AkcOOZ%2BTgIgg8s9pcDdmwns0pybAcp9ZeNs%3D"
    folders = [
        "Incremental/DimHierarchy/LatestData/",
        "Incremental/DimDealer_MS/LatestData/",
        "Incremental/DimCustomerMaster/LatestData/",
        "Incremental/DimMaterial/LatestData/",
        # "Historical_S_Historical/FactInvoiceSecondary/PreviousData/",
        "Historical_S_Historical/FactInvoiceSecondary/LatestData/",
        "Historical_S_Historical/FactInvoiceSecondary_107_112/LatestData/",
        # "Historical_S_Historical/FactInvoiceSecondary_107_112/PreviousData/",
        # "Historical_S_Historical/FactInvoiceDetails_107_112/LatestData/",
        # "Historical_S_Historical/FactInvoiceDetails/LatestData/",
        # "Historical_S_Historical/FactInvoiceDetails_107_112/PreviousData/",
    ]

    # Control concurrency - adjust these values based on your network bandwidth and system resources
    max_concurrent_downloads = 10
    max_concurrent_decompressions = 5

    os.makedirs(base_path, exist_ok=True)

    choice = await display_menu()
    if choice == 5:
        print(f"{Fore.YELLOW}Exiting program.")
        return

    try:
        if choice == 4:
            unextracted_files = await find_unextracted_files(base_path)

            if not unextracted_files:
                print(f"{Fore.YELLOW}No unextracted .gz files found.")
                return

            print(f"{Fore.CYAN}Found {len(unextracted_files)} unextracted .gz files")

            decompress_semaphore = asyncio.Semaphore(max_concurrent_decompressions)
            tasks = []

            for gz_path in unextracted_files:
                csv_path = gz_path.rsplit(".", 2)[0] + ".csv"
                tasks.append(decompress_file(gz_path, csv_path))

            results = await asyncio.gather(*tasks)
            success_count = sum(1 for r in results if r)

            elapsed = time.perf_counter() - start
            print(
                f"\n{Fore.GREEN}✓ Extracted: {Fore.CYAN}{success_count}/{len(unextracted_files)} files in {elapsed:.2f}s"
            )
            return

        async with BlobServiceClient(account_url=account_url, credential=sas_token) as client:
            all_blobs = await get_blobs(client, container_name, folders)

            blob_names_to_download = []
            if choice == 1:
                if input(f"{Fore.YELLOW}Clear existing files? (y/n): ").lower() == "y":
                    import shutil

                    shutil.rmtree(base_path, ignore_errors=True)
                    os.makedirs(base_path, exist_ok=True)
                blob_names_to_download = [b[0] for b in all_blobs]
            elif choice == 2:
                missing_blobs = await get_missing_blobs(all_blobs, base_path)
                print(f"{Fore.CYAN}Found {len(missing_blobs)} missing files")
                blob_names_to_download = [b[0] for b in missing_blobs]
            elif choice == 3:
                blob_names_to_download = await select_blobs(all_blobs)
            else:
                print(f"{Fore.RED}Invalid choice. Exiting.")
                return

            if not blob_names_to_download:
                print(f"{Fore.YELLOW}No files to download.")
                return

            print(
                f"{Fore.GREEN}Downloading {len(blob_names_to_download)} files with {max_concurrent_downloads} parallel connections..."
            )
            download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
            decompress_semaphore = asyncio.Semaphore(max_concurrent_decompressions)

            tasks = [
                process_blob(
                    blob,
                    client,
                    container_name,
                    download_semaphore,
                    decompress_semaphore,
                    base_path,
                )
                for blob in blob_names_to_download
            ]
            results = await asyncio.gather(*tasks)

            success_count = sum(1 for r in results if r)
            elapsed = time.perf_counter() - start
            try:
                total_size = sum(
                    os.path.getsize(
                        os.path.join(
                            base_path,
                            (
                                blob.rsplit(".", 2)[0] + ".csv"
                                if blob.lower().endswith(".gz")
                                else blob
                            ),
                        )
                    )
                    for blob in blob_names_to_download
                    if os.path.exists(
                        os.path.join(
                            base_path,
                            (
                                blob.rsplit(".", 2)[0] + ".csv"
                                if blob.lower().endswith(".gz")
                                else blob
                            ),
                        )
                    )
                )
                print(
                    f"\n{Fore.GREEN}✓ Processed: {Fore.CYAN}{success_count}/{len(blob_names_to_download)} files ({total_size / 1024 / 1024:.2f}MB) in {elapsed:.2f}s"
                )
            except Exception as e:
                print(
                    f"\n{Fore.GREEN}✓ Processed: {Fore.CYAN}{success_count}/{len(blob_names_to_download)} files in {elapsed:.2f}s {e}"
                )
    except Exception as e:
        print(f"{Fore.RED}Fatal error: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
