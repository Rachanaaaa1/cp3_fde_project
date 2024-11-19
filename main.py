from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data

def etl_pipeline():
    # Extract data from CSV files
    sales = extract_data("data/raw/sales.csv")
    customers = extract_data("data/raw/customers.csv")
    products = extract_data("data/raw/products.csv")
    payments = extract_data("data/raw/payments.csv")
    promotions = extract_data("data/raw/promotions.csv")

    if sales is not None and customers is not None and products is not None and payments is not None and promotions is not None:
        # Transform data
        transformed_data = transform_data(sales, customers, products, payments, promotions)
        
        # Load transformed data to Parquet
        load_data(transformed_data, "data/processed/processed_sales.parquet")
    else:
        print("ETL Pipeline failed due to missing data.")

if __name__ == "__main__":
    etl_pipeline()
