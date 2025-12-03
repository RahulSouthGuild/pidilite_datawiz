import asyncio
import aiomysql
import sys
from pathlib import Path
from time import perf_counter
from typing import List, Dict, Any
from tqdm import tqdm

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from config.settings import DB_CONFIG  # noqa: E402

RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"


async def get_db_pool():
    """Create connection pool for StarRocks"""
    try:
        return await aiomysql.create_pool(
            host=DB_CONFIG.get("host", "localhost"),
            port=DB_CONFIG.get("port", 9030),
            user=DB_CONFIG.get("user", "root"),
            password=DB_CONFIG.get("password", ""),
            db=DB_CONFIG.get("database", ""),
            minsize=10,
            maxsize=50,
            autocommit=True,
        )
    except Exception as e:
        print(f"{RED}Failed to create connection pool: {e}{RESET}")
        raise


async def sanitize_email(email: str) -> str:
    """Sanitize email to create path component"""
    try:
        if not email:
            return ""
        sanitized = (
            email.strip()
            .lower()
            .replace("@", "_at_")
            .replace(".", "_dot_")
            .replace("-", "_")
            .replace(" ", "_")
        )
        return "".join(c for c in sanitized if c.isalnum() or c == "_")
    except Exception as e:
        print(f"{RED}Email sanitization error: {e}{RESET}")
        return ""


async def process_hierarchy_batch(
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Process batch of hierarchy records and build RLS Master rows"""
    try:
        records = []
        for row in rows:
            # Collect all SH emails from the hierarchy
            emails = [row.get(f"sh_{i}_email") for i in range(2, 8) if row.get(f"sh_{i}_email")]

            sanitized = [await sanitize_email(email) for email in emails]
            # Remove duplicates but keep order
            unique = [
                email
                for i, email in enumerate(sanitized)
                if email and (i == 0 or sanitized[i - 1] != email)
            ]

            if len(unique) >= 2:
                path = ".".join(unique)
                records.append(
                    {
                        "cluster_code": row.get("cluster_code", ""),
                        "division_code": row.get("division_code", 0),
                        "sales_group": row.get("sales_group", 0),
                        "wss_territory_code": row.get("wss_territory_code", ""),
                        "sh_2": row.get("sh_2_email", ""),
                        "sh_3": row.get("sh_3_email", ""),
                        "sh_4": row.get("sh_4_email", ""),
                        "sh_5": row.get("sh_5_email", ""),
                        "sh_6": row.get("sh_6_email", ""),
                        "sh_7": row.get("sh_7_email", ""),
                        "email_id": emails[0] if emails else "",
                        "hierarchy_path": path,
                    }
                )
        return records
    except Exception as e:
        print(f"{RED}Batch processing error: {e}{RESET}")
        return []


async def fetch_and_store_data(pool):
    """Fetch hierarchy data from DimHierarchy and populate RLS Master"""
    start = perf_counter()
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # TRUNCATE rls_master table
                await cursor.execute("TRUNCATE TABLE rls_master")
                print(f"{YELLOW}Truncated rls_master table{RESET}")

                # Query to fetch hierarchy data
                # Fetch all relevant columns from DimHierarchy
                query = """
                    SELECT
                        cluster_code,
                        division_code,
                        sales_group,
                        wss_territory_code,
                        sh_2_email,
                        sh_3_email,
                        sh_4_email,
                        sh_5_email,
                        sh_6_email,
                        sh_7_email
                    FROM dim_hierarchy
                    WHERE txn_type LIKE '%Sales Hierarchy%'
                    AND sh_2_email IS NOT NULL
                """

                await cursor.execute(query)
                rows_list = await cursor.fetchall()

                # Convert tuple rows to dict rows
                rows = []
                for row in rows_list:
                    rows.append(
                        {
                            "cluster_code": row[0],
                            "division_code": row[1],
                            "sales_group": row[2],
                            "wss_territory_code": row[3],
                            "sh_2_email": row[4],
                            "sh_3_email": row[5],
                            "sh_4_email": row[6],
                            "sh_5_email": row[7],
                            "sh_6_email": row[8],
                            "sh_7_email": row[9],
                        }
                    )

                batch_size = 1000
                total_records = 0

                print(f"{YELLOW}Processing {len(rows)} hierarchy records...{RESET}")
                with tqdm(total=len(rows), desc="Processing records", unit="record") as pbar:
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i : i + batch_size]
                        records = await process_hierarchy_batch(batch)

                        if records:
                            # Prepare bulk insert
                            insert_query = """
                                INSERT INTO rls_master 
                                (cluster, division, email_id, hierarchy_path, sales_group, sh_2, sh_3, sh_4, sh_5, sh_6, sh_7)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """

                            values = [
                                (
                                    r.get("cluster_code", ""),
                                    r.get("division_code", 0),
                                    r.get("email_id", ""),
                                    r.get("hierarchy_path", ""),
                                    r.get("sales_group", 0),
                                    r.get("sh_2", ""),
                                    r.get("sh_3", ""),
                                    r.get("sh_4", ""),
                                    r.get("sh_5", ""),
                                    r.get("sh_6", ""),
                                    r.get("sh_7", ""),
                                )
                                for r in records
                            ]

                            await cursor.executemany(insert_query, values)
                            total_records += len(records)
                            pbar.update(len(batch))
                            pbar.set_postfix({"inserted": total_records})

                print(f"{GREEN}✓ Total records inserted: {total_records}{RESET}")
                print(f"{YELLOW}Time taken: {perf_counter() - start:.2f}s{RESET}")

    except Exception as e:
        print(f"{RED}✗ Data processing error: {e}{RESET}")
        raise


async def get_wss_codes_by_email(pool, email: str) -> List[str]:
    """Get WSS Territory codes for a given email from RLS Master"""
    try:
        sanitized = await sanitize_email(email)
        if not sanitized:
            return []

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Query using LIKE pattern for hierarchy path matching
                pattern = f"%.{sanitized}.%"
                query = "SELECT DISTINCT wss_territory_code FROM rls_master WHERE hierarchy_path LIKE %s"
                await cursor.execute(query, (pattern,))
                rows = await cursor.fetchall()
                return [row[0] for row in rows if row[0]]
    except Exception as e:
        print(f"{RED}✗ WSS code lookup error for {email}: {e}{RESET}")
        return []


async def main():
    """Main function: Populate RLS Master and test queries"""
    pool = None
    try:
        pool = await get_db_pool()
        print(f"{GREEN}✓ Connected to StarRocks{RESET}")

        await fetch_and_store_data(pool)

        # Test query
        test_email = "ANKIT.KAWAD@PIDILITE.COM"
        wss_codes = await get_wss_codes_by_email(pool, test_email)

        if wss_codes:
            print(f"{GREEN}✓ WSS Territory Codes for {test_email}:{RESET}")
            for code in wss_codes:
                print(f"  - {code}")
        else:
            print(f"{YELLOW}⚠ No WSS Territory codes found for {test_email}{RESET}")

        print(f"{GREEN}✓ RLS Master population complete!{RESET}")

    except Exception as e:
        print(f"{RED}✗ Application error: {e}{RESET}")
    finally:
        if pool:
            pool.close()
            await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
