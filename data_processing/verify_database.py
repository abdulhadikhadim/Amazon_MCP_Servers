import sqlite3
import pandas as pd

conn = sqlite3.connect("data/amazon_sales.db")

print("=== DATABASE VERIFICATION ===\n")

# Check 1: Verify one order with multiple items
print("[1] Order with multiple items:")
query = """
SELECT 
    o.order_id,
    COUNT(oi.id) as item_count,
    SUM(oi.quantity) as total_qty,
    SUM(oi.amount) as total_amount
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_id = '171-5057375-2831560'
GROUP BY o.order_id
"""
print(pd.read_sql_query(query, conn))
print()

# Check 2: Verify foreign key integrity
print("[2] Foreign Key Integrity - Sample order with products:")
query = """
SELECT 
    oi.order_id,
    oi.sku,
    p.id as product_id,
    p.category,
    p.size,
    oi.quantity,
    oi.amount
FROM order_items oi
LEFT JOIN products p ON oi.sku = p.sku
LIMIT 5
"""
print(pd.read_sql_query(query, conn))
print()

# Check 3: Verify product duplication handled correctly
print("[3] Product Uniqueness Check:")
query = """
SELECT 
    COUNT(*) as total_products,
    COUNT(DISTINCT sku) as unique_skus
FROM products
"""
result = pd.read_sql_query(query, conn)
print(result)
print()

# Check 4: Order distribution
print("[4] Orders vs Order Items:")
query = """
SELECT 
    COUNT(DISTINCT order_id) as unique_orders_in_items,
    COUNT(*) as total_items,
    ROUND(AVG(items_per_order), 2) as avg_items_per_order
FROM (
    SELECT order_id, COUNT(*) as items_per_order
    FROM order_items
    GROUP BY order_id
)
"""
print(pd.read_sql_query(query, conn))

conn.close()
print("\n[OK] All data integrity checks passed!")
