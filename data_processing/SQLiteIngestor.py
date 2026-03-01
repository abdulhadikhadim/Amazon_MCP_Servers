import sqlite3
import pandas as pd
import numpy as np
from DataProcessing import DataProcessing


class SQLiteIngestor:
    """
    Properly normalized e-commerce data ingestion.
    
    Schema:
    - products: unique products (sku, asin, category, size, style)
    - orders: unique orders with order-level info (date, status, fulfilment, currency, b2b)
    - order_items: line items (links orders to products with quantity/amount)
    """
    
    def __init__(self, db_path="data/amazon_sales.db"):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.data_processing = DataProcessing()
        self.product_id_map = {}  # Cache: (sku, asin, category, size, style) -> id
    
    def connect(self):
        """Establish connection to SQLite database"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        # Enable foreign keys
        self.cursor.execute("PRAGMA foreign_keys = ON")
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def create_tables(self):
        """Create normalized database schema"""
        # Products table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL,
                asin TEXT,
                category TEXT,
                size TEXT,
                style TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sku, asin, category, size, style)
            )
        """)
        
        # Orders table (one row per Order ID)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                date TEXT,
                status TEXT,
                fulfilment_type TEXT,
                currency TEXT,
                b2b BOOLEAN,
                courier_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Order Items table (one row per item in an order)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                sku TEXT NOT NULL,
                quantity INTEGER,
                amount REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
                UNIQUE(order_id, sku)
            )
        """)
        
        self.conn.commit()
        print("[OK] Database schema created successfully")
    
    def ingest_products(self):
        """Insert products into database with conflict handling"""
        products_df = self.data_processing.create_products_table()
        
        inserted_count = 0
        for _, row in products_df.iterrows():
            try:
                self.cursor.execute("""
                    INSERT OR IGNORE INTO products 
                    (sku, asin, category, size, style) 
                    VALUES (?, ?, ?, ?, ?)
                """, (row['sku'], row['asin'], row['category'], 
                      row['size'], row['style']))
                
                if self.cursor.rowcount > 0:
                    inserted_count += 1
            except Exception as e:
                print(f"[WARN] Error inserting product {row['sku']}: {e}")
        
        self.conn.commit()
        print(f"[OK] Processed {len(products_df)} products ({inserted_count} new)")
    
    def build_product_map(self):
        """Load product ID map from database - keyed by SKU for order_items"""
        self.product_id_map = {}
        self.cursor.execute("SELECT id, sku FROM products")
        
        for row in self.cursor.fetchall():
            product_id, sku = row
            self.product_id_map[sku] = product_id
        
        print(f"[OK] Loaded {len(self.product_id_map)} products from database")
    
    def ingest_orders(self):
        """Insert orders (unique by Order ID) into database"""
        orders_df = self.data_processing.create_orders_table()
        
        inserted_count = 0
        for _, row in orders_df.iterrows():
            try:
                self.cursor.execute("""
                    INSERT OR IGNORE INTO orders 
                    (order_id, date, status, fulfilment_type, currency, b2b, courier_status) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (row['order_id'], row['date'], row['status'], 
                      row['fulfilment_type'], row['currency'], 
                      row['b2b'], row['courier_status']))
                
                if self.cursor.rowcount > 0:
                    inserted_count += 1
            except Exception as e:
                print(f"[WARN] Error inserting order {row['order_id']}: {e}")
        
        self.conn.commit()
        print(f"[OK] Processed {len(orders_df)} orders ({inserted_count} new)")
    
    def ingest_order_items(self):
        """
        Insert order items (links orders to products).
        Uses DB-fetched product IDs to ensure referential integrity.
        """
        products_df = self.data_processing.create_products_table()
        items_df = self.data_processing.create_order_items_table(products_df)
        
        inserted_count = 0
        skipped_count = 0
        
        for _, row in items_df.iterrows():
            try:
                sku = str(row['sku']).strip()
                
                # Skip if product not found
                if sku not in self.product_id_map:
                    skipped_count += 1
                    continue
                
                # Convert NaN to None for SQLite
                quantity = row['quantity'] if pd.notna(row['quantity']) else None
                amount = row['amount'] if pd.notna(row['amount']) else None
                
                self.cursor.execute("""
                    INSERT OR IGNORE INTO order_items 
                    (order_id, sku, quantity, amount) 
                    VALUES (?, ?, ?, ?)
                """, (str(row['order_id']), sku, quantity, amount))
                
                if self.cursor.rowcount > 0:
                    inserted_count += 1
            except Exception as e:
                print(f"[WARN] Error inserting order item: {e}")
                skipped_count += 1
        
        self.conn.commit()
        print(f"[OK] Processed {len(items_df)} order items ({inserted_count} new, {skipped_count} skipped)")
    
    def ingest_all(self):
        """Execute complete ingestion pipeline (step by step)"""
        try:
            self.connect()
            self.create_tables()
            self.ingest_products()
            self.build_product_map()
            self.ingest_orders()
            self.ingest_order_items()
            print("\n[OK] Data ingestion completed successfully!")
        except Exception as e:
            print(f"[ERROR] Critical error during ingestion: {e}")
            self.conn.rollback()
            raise
        finally:
            self.disconnect()
    
    def query_sample(self, table_name, limit=5):
        """Query and display sample data from a table"""
        self.connect()
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        result = pd.read_sql_query(query, self.conn)
        self.disconnect()
        return result
    
    def get_stats(self):
        """Get database statistics"""
        self.connect()
        stats = {}
        
        for table in ['products', 'orders', 'order_items']:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = self.cursor.fetchone()[0]
        
        self.disconnect()
        return stats


if __name__ == "__main__":
    ingestor = SQLiteIngestor()
    ingestor.ingest_all()
    
    # Display statistics
    stats = ingestor.get_stats()
    print("\n--- Database Statistics ---")
    for table, count in stats.items():
        print(f"{table}: {count:,} rows")
    
    # Display sample data
    print("\n--- Sample Products ---")
    print(ingestor.query_sample("products", 3))
    
    print("\n--- Sample Orders ---")
    print(ingestor.query_sample("orders", 3))
    
    print("\n--- Sample Order Items ---")
    print(ingestor.query_sample("order_items", 5))
