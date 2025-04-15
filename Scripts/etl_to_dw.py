import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DW_DIR = pathlib.Path("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("histo_inventory.db")
PREPARED_DATA_DIR = pathlib.Path("data").joinpath("prepared")

def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS histo_inventory (
            short_name INTEGER PRIMARY KEY,
            vendor TEXT,
            quantity INTEGER,
            min_stock_level INTEGER,
           max_in_stock INTEGER,
                 barcode TEXT,
                unit, TEXT,
                qty_per_unit INTEGER,
                catalog_number INTEGER,
                notes TEXT,
                date DATE      
        )
    """)
    
   
def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the  tables."""
    cursor.execute("DELETE FROM histo_inventory")
   

def insert_histo_inventory(histo_inventory_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert inventory data into the inventory table."""
    histo_inventory_df.to_sql("histo_inventory", cursor.connection, if_exists="append", index=False)



def load_data_to_db() -> None:
    try:
        # Connect to SQLite – will create the file if it doesn't exist
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create schema and clear existing records
        create_schema(cursor)
        delete_existing_records(cursor)

        # Load prepared data using pandas
        histo_inventory_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("histo_inventory_prepared.csv"))
      

        # Insert data into the database
        insert_histo_inventory(histo_inventory_df, cursor)
        

        conn.commit()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_data_to_db()
   
