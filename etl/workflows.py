import pandas as pd
from database import client_db
from .config_loader import get_config_loader
from .transformations import TransformationEngine, apply_row_filters, apply_aggregations


def copydata1_products(source_file: str, db_name: str, **kwargs):
    """
    CopyData1: Products
    Source: SourceFile (SourceFileA or SourceFileB)
    Destination: Product
    Record Join: RecordId -> ProductId
    Mappings:
        - RecordId -> ProductId
        - CustomerId -> CustomerId
        - ProductName -> ProductName
    """
    df = pd.read_csv(source_file)

    products = df[["RecordId", "CustomerId", "ProductName"]].copy()
    products.columns = ["ProductId", "CustomerId", "ProductName"]
    products = products.drop_duplicates(subset=["ProductId"])

    conn, cur = client_db(db_name)

    for _, row in products.iterrows():
        cur.execute("""
            INSERT OR REPLACE INTO Product (ProductId, CustomerId, ProductName)
            VALUES (?, ?, ?)
        """, (row["ProductId"], row["CustomerId"], row["ProductName"]))

    conn.commit()
    conn.close()

    print("CopyData1: Loaded {} products from {} to {}".format(len(products), source_file, db_name))


def copydata2_customers(source_file: str, db_name: str, **kwargs):
    """
    CopyData2: Customers
    Source: AggregatedDataset Customer
    Destination: Customer
    Record Join: CustomerId -> CustomerId
    Aggregated fields: CustomerName (first), CustomerVAT (first)
    Group by fields: CustomerId
    Mappings:
        - CustomerId -> CustomerId
        - CustomerVAT -> CustomerVAT
        - CustomerName -> CustomerName
    """
    df = pd.read_csv(source_file)

    customers = df.groupby("CustomerId").agg({
        "CustomerName": "first",
        "CustomerVAT": "first"
    }).reset_index()

    conn, cur = client_db(db_name)

    for _, row in customers.iterrows():
        cur.execute("""
            INSERT OR REPLACE INTO Customer (CustomerId, CustomerVAT, CustomerName)
            VALUES (?, ?, ?)
        """, (row["CustomerId"], row["CustomerVAT"], row["CustomerName"]))

    conn.commit()
    conn.close()

    print("CopyData2: Loaded {} customers from {} to {}".format(len(customers), source_file, db_name))


def copydata3_addresses(source_file: str, db_name: str, **kwargs):
    """
    CopyData3: Addresses
    Source: AggregatedDataset Address
    Destination: Address
    Record Join: CustomerId, address -> CustomerId, Address
    Group by fields: CustomerId, Address
    Aggregated fields: N/A
    Mappings:
        - Autoinc function -> AddressId
        - CustomerId -> CustomerId
        - Address -> Address
    """
    df = pd.read_csv(source_file)

    addresses = df[["CustomerId", "Address"]].drop_duplicates()

    conn, cur = client_db(db_name)

    for _, row in addresses.iterrows():
        cur.execute("""
            INSERT OR IGNORE INTO Address (CustomerId, Address)
            VALUES (?, ?)
        """, (row["CustomerId"], row["Address"]))

    conn.commit()
    conn.close()

    print("CopyData3: Loaded {} addresses from {} to {}".format(len(addresses), source_file, db_name))


def copydata4_phones(source_file: str, db_name: str, **kwargs):
    """
    CopyData4: Phones
    Source: AggregatedDataset Phone
    Destination: Phone
    Record Join: CustomerId, phonumber -> CustomerId, phonenumber
    Group by fields: CustomerId, Phone
    Aggregated fields: N/A
    Mappings:
        - Autoinc function -> PhoneId
        - CustomerId -> CustomerId
        - Phonenumber -> Phonenumber
    """
    df = pd.read_csv(source_file)

    phones = df[["CustomerId", "Phonenumber"]].drop_duplicates()

    conn, cur = client_db(db_name)

    for _, row in phones.iterrows():
        cur.execute("""
            INSERT OR IGNORE INTO Phone (CustomerId, PhoneNumber)
            VALUES (?, ?)
        """, (row["CustomerId"], row["Phonenumber"]))

    conn.commit()
    conn.close()

    print("CopyData4: Loaded {} phones from {} to {}".format(len(phones), source_file, db_name))


# SystemA wrapper functions (for YAML configuration)
SYSTEM_A_SOURCE_FILE = "/home/kevit/PycharmProjects/QP/EXUS/test-airflow-dag-factoory/data/system_A/incoming/source_file_A.csv"
SYSTEM_A_DB = "systemA.db"


def copydata1_products_a(**kwargs):
    copydata1_products(SYSTEM_A_SOURCE_FILE, SYSTEM_A_DB, **kwargs)


def copydata2_customers_a(**kwargs):
    copydata2_customers(SYSTEM_A_SOURCE_FILE, SYSTEM_A_DB, **kwargs)


def copydata3_addresses_a(**kwargs):
    copydata3_addresses(SYSTEM_A_SOURCE_FILE, SYSTEM_A_DB, **kwargs)


def copydata4_phones_a(**kwargs):
    copydata4_phones(SYSTEM_A_SOURCE_FILE, SYSTEM_A_DB, **kwargs)


# SystemB wrapper functions (for YAML configuration)
SYSTEM_B_SOURCE_FILE = "/home/kevit/PycharmProjects/QP/EXUS/test-airflow-dag-factoory/data/system_B/incoming/source_file_B.csv"
SYSTEM_B_DB = "systemB.db"


def copydata1_products_b(**kwargs):
    copydata1_products(SYSTEM_B_SOURCE_FILE, SYSTEM_B_DB, **kwargs)


def copydata2_customers_b(**kwargs):
    copydata2_customers(SYSTEM_B_SOURCE_FILE, SYSTEM_B_DB, **kwargs)


def copydata3_addresses_b(**kwargs):
    copydata3_addresses(SYSTEM_B_SOURCE_FILE, SYSTEM_B_DB, **kwargs)


def copydata4_phones_b(**kwargs):
    copydata4_phones(SYSTEM_B_SOURCE_FILE, SYSTEM_B_DB, **kwargs)


# ============================================================================
# Configuration-Driven ETL Functions (New Approach)
# ============================================================================

def execute_copy_data_task(
    system: str,
    source: str,
    source_type: str,
    destination_table: str,
    copydata_task: str,
    **kwargs
):
    """
    Execute a CopyData task based on YAML configuration.

    This is the main entry point for configuration-driven ETL operations.
    It loads configurations, reads source data, applies transformations
    and aggregations, and loads data into the destination table.

    New Scalable Configuration Structure:
    - configs/sources/{system}/{source_name}.yaml
    - configs/aggregations/{system}/{agg_name}.yaml
    - configs/copydata/{system}/{task_name}.yaml

    Args:
        system: System identifier ('a', 'b', 'system_a', 'system_b')
        source: Source name ('source_file_a', 'customer', 'address', etc.)
        source_type: Source type ('source_file' or 'aggregated_dataset')
        destination_table: Destination table name ('Product', 'Customer', etc.)
        copydata_task: CopyData task name ('products', 'customers', etc.)
        **kwargs: Additional Airflow context parameters
    """
    config_loader = get_config_loader()
    transformer = TransformationEngine()

    # Normalize system name
    system = system.lower().replace("system_", "")

    # Load CopyData task configuration
    copydata_config = config_loader.load_copydata_task(system, copydata_task)
    task_def = copydata_config.get('task', {})

    # Determine source file and apply transformations based on source_type
    if source_type == 'aggregated_dataset':
        # Load aggregation configuration
        agg_config = config_loader.load_aggregation(system, source)
        agg_def = agg_config.get('aggregation', {})

        # Get base source from aggregation config
        base_source_name = agg_config.get('source', {}).get('name', source)
        base_source_config = config_loader.load_source_file(system, base_source_name)

        source_file_path = base_source_config.get('file_config', {}).get('path')
        source_columns = base_source_config.get('columns', [])
        row_filters = base_source_config.get('row_filters', [])

        group_by = agg_def.get('group_by', [])
        aggregated_columns = agg_def.get('aggregated_columns', [])

    else:  # source_file
        # Load source file configuration directly
        source_config = config_loader.load_source_file(system, source)
        source_file_path = source_config.get('file_config', {}).get('path')
        source_columns = source_config.get('columns', [])
        row_filters = source_config.get('row_filters', [])

        group_by = []
        aggregated_columns = []

    # Read source file
    df = pd.read_csv(source_file_path)

    # Apply transformations to source columns
    df = transformer.apply_transformations(df, source_columns)

    # Apply row filters
    df = apply_row_filters(df, row_filters)

    # Apply aggregations if needed
    if source_type == 'aggregated_dataset' and group_by:
        df = apply_aggregations(df, group_by, aggregated_columns)

    # Get column mappings from copydata config (at root level, not under task)
    column_mappings = copydata_config.get('column_mappings', [])

    # Build the destination DataFrame
    dest_columns = {}
    for mapping in column_mappings:
        source_col = mapping.get('source')
        dest_col = mapping.get('destination')
        if source_col in df.columns:
            dest_columns[dest_col] = df[source_col]

    result_df = pd.DataFrame(dest_columns)

    # Drop duplicates based on record join columns (at root level, not under task)
    record_join = copydata_config.get('record_join', {})
    join_source_cols = record_join.get('source_columns', [])
    if join_source_cols:
        # Map source columns to destination columns for deduplication
        join_dest_cols = []
        for src_col in join_source_cols:
            for mapping in column_mappings:
                if mapping.get('source') == src_col:
                    join_dest_cols.append(mapping.get('destination'))
                    break
        if join_dest_cols:
            result_df = result_df.drop_duplicates(subset=join_dest_cols, keep='first')

    # Get database name from CopyData config (destination is at root level, not under task)
    database = copydata_config.get('destination', {}).get('database')
    conn, cur = client_db(database)

    # Insert/Update records (copy_mode is also at root level)
    copy_mode = copydata_config.get('copy_mode', 'insert_update')

    for _, row in result_df.iterrows():
        if copy_mode == 'insert_update':
            # Check if record exists
            where_clauses = []
            where_values = []
            for src_col, dest_col in zip(join_source_cols, join_dest_cols):
                where_clauses.append(f"{dest_col} = ?")
                where_values.append(row[dest_col])

            where_clause = " AND ".join(where_clauses)

            # Check if exists
            cur.execute(f"SELECT 1 FROM {destination_table} WHERE {where_clause}", where_values)
            exists = cur.fetchone() is not None

            if exists:
                # Update
                set_clauses = []
                update_values = []
                for mapping in column_mappings:
                    dest_col = mapping.get('destination')
                    empty_behavior = mapping.get('empty_source_behavior', 'update')
                    val = row.get(dest_col)

                    if pd.isna(val) or val == '' or val is None:
                        if empty_behavior == 'keep':
                            continue

                    set_clauses.append(f"{dest_col} = ?")
                    update_values.append(row[dest_col])

                if set_clauses:
                    set_clause = ", ".join(set_clauses)
                    update_values.extend(where_values)
                    cur.execute(
                        f"UPDATE {destination_table} SET {set_clause} WHERE {where_clause}",
                        update_values
                    )
            else:
                # Insert
                columns = [m.get('destination') for m in column_mappings]
                values = [row.get(col) for col in columns]
                placeholders = ", ".join(["?" for _ in columns])
                cols_str = ", ".join(columns)
                cur.execute(
                    f"INSERT OR REPLACE INTO {destination_table} ({cols_str}) VALUES ({placeholders})",
                    values
                )
        elif copy_mode == 'insert_only':
            # Insert only, skip if exists
            columns = [m.get('destination') for m in column_mappings]
            values = [row.get(col) for col in columns]
            placeholders = ", ".join(["?" for _ in columns])
            cols_str = ", ".join(columns)
            cur.execute(
                f"INSERT OR IGNORE INTO {destination_table} ({cols_str}) VALUES ({placeholders})",
                values
            )

    conn.commit()
    conn.close()

    print(f"CopyData task '{copydata_task}' completed: Loaded {len(result_df)} records to {destination_table} in {database}")
