import logging
import os
from sqlalchemy.orm import Session
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

def upsert_csv_to_postgres(db: Session, file_path: str, table_name: str, unique_columns: list[str]):
    """
    Performs a bulk "upsert" of a CSV file into a PostgreSQL table using a temporary table.

    This non-destructive function will:
    1. Create a temporary table with the same structure as the target table.
    2. Bulk-load the CSV data into the temporary table using COPY.
    3. Execute an INSERT...ON CONFLICT...DO UPDATE command to merge the data
       from the temporary table into the final target table.

    Args:
        db (Session): The SQLAlchemy database session.
        file_path (str): The local path to the CSV file.
        table_name (str): The name of the target table in PostgreSQL.
        unique_columns (list[str]): A list of column names that form the unique constraint
                                    for the ON CONFLICT clause (e.g., ['product_id']).
    """
    if not table_name.replace('_', '').isalnum():
        raise ValueError(f"Invalid table name: {table_name}")
    if not unique_columns:
        raise ValueError("unique_columns list cannot be empty for an upsert operation.")

    temp_table_name = f"temp_{table_name}"
    conn = None

    try:
        conn = db.connection().connection
        cursor = conn.cursor()

        logger.info(f"Starting upsert for '{os.path.basename(file_path)}' into table '{table_name}'.")

        # 1. Create a temporary table with the same structure as the target table.
        # This assumes the target table already exists.
        logger.info(f"Creating temporary table '{temp_table_name}'.")
        cursor.execute(f"CREATE TEMP TABLE {temp_table_name} (LIKE {table_name} INCLUDING DEFAULTS);")

        # 2. Bulk-load the CSV into the temporary table.
        logger.info(f"Loading data into temporary table using COPY.")
        with open(file_path, 'r', encoding='utf-8') as f:
            cursor.copy_expert(f"COPY {temp_table_name} FROM STDIN WITH CSV HEADER", f)

        # 3. Build and execute the UPSERT command.
        with open(file_path, 'r', encoding='utf-8') as f:
            header = f.readline().strip()
        all_columns = [f'"{col.strip()}"' for col in header.split(',')]
        
        # Columns to update are all columns EXCEPT the unique constraint columns.
        update_columns = [col for col in all_columns if col.strip('"') not in unique_columns]
        
        if not update_columns:
             # This can happen if all columns are part of the unique key. In this case, we only need to insert.
            update_clause = "DO NOTHING"
        else:
            update_clause = f"DO UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in update_columns])}"

        constraint_cols_str = ", ".join([f'"{col}"' for col in unique_columns])
        all_cols_str = ", ".join(all_columns)

        upsert_sql = f"""
            INSERT INTO {table_name} ({all_cols_str})
            SELECT {all_cols_str} FROM {temp_table_name}
            ON CONFLICT ({constraint_cols_str})
            {update_clause};
        """
        
        logger.info(f"Executing UPSERT from temp table to '{table_name}'.")
        cursor.execute(upsert_sql)
        
        # Get the number of rows affected by the upsert
        affected_rows = cursor.rowcount
        logger.info(f"{affected_rows} rows were inserted or updated in '{table_name}'.")

        # The temporary table is automatically dropped at the end of the session.
        conn.commit()
        logger.info(f"Upsert for '{table_name}' completed and transaction committed.")
        cursor.close()

    except Exception as e:
        logger.error(f"Failed to upsert CSV '{file_path}' to table '{table_name}': {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise

def get_db():
    """Dependency function to get a SQLAlchemy database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()