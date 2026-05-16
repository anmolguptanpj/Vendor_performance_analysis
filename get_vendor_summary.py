import sqlite3
import pandas as pd
import logging
import os
from ingestion_db import ingest_db


# =========================
# LOGGING CONFIGURATION
# =========================
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename='logs/vendor_summary.log',
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    filemode="a"
)

logger = logging.getLogger(__name__)


# =========================
# CREATE SUMMARY TABLE
# =========================
def create_vendor_summary(conn):

    logger.info("Starting vendor summary creation")

    query = """
        WITH FreightSummary AS (
            SELECT 
                VendorNumber,
                SUM(Freight) AS FreightCost
            FROM vendor_invoice
            GROUP BY VendorNumber
        ),

        PurchaseSummary AS (
            SELECT 
                p.VendorNumber,
                p.VendorName,
                p.Brand,
                p.Description,
                p.PurchasePrice,
                pp.Price AS ActualPrice,
                pp.Volume,
                SUM(p.Quantity) AS TotalPurchaseQuantity,
                SUM(p.Dollars) AS TotalPurchaseDollars

            FROM purchases p

            JOIN purchase_prices pp
                ON p.Brand = pp.Brand

            WHERE p.PurchasePrice > 0

            GROUP BY 
                p.VendorNumber,
                p.VendorName,
                p.Brand,
                p.Description,
                p.PurchasePrice,
                pp.Price,
                pp.Volume
        ),

        SalesSummary AS (
            SELECT 
                VendorNo,
                Brand,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(ExciseTax) AS TotalExciseTax

            FROM sales

            GROUP BY VendorNo, Brand
        )

        SELECT
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.PurchasePrice,
            ps.ActualPrice,
            ps.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,

            COALESCE(ss.TotalSalesDollars, 0) AS TotalSalesDollars,
            COALESCE(ss.TotalSalesQuantity, 0) AS TotalSalesQuantity,
            COALESCE(ss.TotalSalesPrice, 0) AS TotalSalesPrice,
            COALESCE(ss.TotalExciseTax, 0) AS TotalExciseTax,

            COALESCE(fs.FreightCost, 0) AS FreightCost

        FROM PurchaseSummary ps

        LEFT JOIN SalesSummary ss
            ON ps.VendorNumber = ss.VendorNo
            AND ps.Brand = ss.Brand

        LEFT JOIN FreightSummary fs
            ON ps.VendorNumber = fs.VendorNumber

        ORDER BY ps.TotalPurchaseDollars DESC
    """

    vendor_sales_summary = pd.read_sql_query(query, conn)

    logger.info(
        f"Vendor summary created successfully with {len(vendor_sales_summary)} rows"
    )

    return vendor_sales_summary


# =========================
# CLEAN DATA
# =========================
def clean_data(df):

    logger.info("Starting data cleaning process")

    df = df.copy()

    # Fill null values
    df.fillna(0, inplace=True)

    # Convert datatype
    df["Volume"] = df["Volume"].astype("float64")

    # Clean text columns
    df["VendorName"] = df["VendorName"].astype(str).str.strip()
    df["Description"] = df["Description"].astype(str).str.strip()

    # =========================
    # FEATURE ENGINEERING
    # =========================

    # Gross Profit
    df["GrossProfit"] = (
        df["TotalSalesDollars"] - df["TotalPurchaseDollars"]
    )

    # Profit Margin
    df["ProfitMargin"] = (
        df["GrossProfit"] / df["TotalSalesDollars"].replace(0, 1)
    ) * 100

    # Stock Turnover
    df["StockTurnover"] = (
        df["TotalSalesQuantity"] /
        df["TotalPurchaseQuantity"].replace(0, 1)
    )

    # Sales To Purchase Ratio
    df["SalesToPurchaseRatio"] = (
        df["TotalSalesDollars"] /
        df["TotalPurchaseDollars"].replace(0, 1)
    )

    logger.info("Data cleaning and feature engineering completed")

    return df


# =========================
# MAIN EXECUTION
# =========================
if __name__ == '__main__':

    try:
        logger.info("Connecting to SQLite database")

        conn = sqlite3.connect('inventory.db')

        logger.info("Database connection successful")

        # Create Summary
        summary_df = create_vendor_summary(conn)
        logger.info(summary_df.head())

        # Clean Data
        clean_df = clean_data(summary_df)

        logger.info("Starting ingestion into database")
        logger.info(clean_df.head())

        # Ingest into DB
        ingest_db(clean_df, 'vendor_sales_summary', conn)

        logger.info(
            "vendor_sales_summary table ingested successfully"
        )

        print("Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        print(f"Error: {e}")

    finally:
        conn.close()
        logger.info("Database connection closed")