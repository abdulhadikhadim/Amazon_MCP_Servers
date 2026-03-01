import pandas as pd

class DataProcessing:
    def __init__(self):
        self.df = pd.read_csv(
            "data/Amazon Sale Report.csv",
            low_memory=False
        )
        self.clean_columns()
        self.convert_types()

    def clean_columns(self):
        # Drop useless columns
        self.df = self.df.drop(columns=["Unnamed: 22"], errors="ignore")

    def convert_types(self):
        self.df["Date"] = pd.to_datetime(self.df["Date"], errors="coerce")
        self.df["Amount"] = pd.to_numeric(self.df["Amount"], errors="coerce")
        self.df["ship-postal-code"] = self.df["ship-postal-code"].astype("Int64")

    def info(self):
        return self.df.info()
    
    def create_products_table(self):
        """Normalized products table - one row per unique product"""
        products = (
            self.df[["SKU", "ASIN", "Category", "Size", "Style"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        products = products.rename(columns={
            "SKU": "sku",
            "ASIN": "asin",
            "Category": "category",
            "Size": "size",
            "Style": "style"
        })
        return products

    def create_orders_table(self):
        """Normalized orders table - one row per unique Order ID"""
        orders = self.df[[
            "Order ID",
            "Date",
            "Status",
            "Fulfilment",
            "currency",
            "B2B",
            "Courier Status"
        ]].drop_duplicates(subset=["Order ID"]).copy()

        orders = orders.rename(columns={
            "Order ID": "order_id",
            "Date": "date",
            "Status": "status",
            "Fulfilment": "fulfilment_type",
            "Courier Status": "courier_status",
            "B2B": "b2b"
        })

        # Convert date to string format
        orders["date"] = orders["date"].astype(str)
        orders["b2b"] = orders["b2b"].astype(str)

        return orders

    def create_order_items_table(self, products_df):
        """
        Normalized order_items table - one row per item in each order
        Links orders to products with quantity and amount
        """
        # Merge with products to get product IDs (but don't use pandas IDs!)
        items = self.df.copy()
        items = items.merge(
            products_df.reset_index(drop=True),
            how="left",
            left_on=["SKU", "ASIN", "Category", "Size", "Style"],
            right_on=["sku", "asin", "category", "size", "style"]
        )

        order_items = items[[
            "Order ID",
            "SKU",
            "Qty",
            "Amount"
        ]].copy()

        order_items = order_items.rename(columns={
            "Order ID": "order_id",
            "SKU": "sku",
            "Qty": "quantity",
            "Amount": "amount"
        })

        # Convert types for SQLite compatibility
        order_items["quantity"] = pd.to_numeric(order_items["quantity"], errors="coerce").astype("Int64")
        order_items["amount"] = pd.to_numeric(order_items["amount"], errors="coerce")

        return order_items

    
    

if __name__ == "__main__":
    data_processing = DataProcessing()
    data_processing.info()