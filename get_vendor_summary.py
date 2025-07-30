import pandas as pd
import sqlite3
import logging
from ingestion_db import ingest_db
import time 

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    '''this function will merge the diffrent tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summery = pd.read_sql_query("""with FreightSummery AS (
        select
            VendorNumber,
            sum(Freight) as FreightCost 
            From vendor_invoice 
            group by VendorNumber        
    ),
        PurchaseSummery as (
        select 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS Actualprice,
            pp.Volume,
            sum(p.Quantity) AS TotalPurchasesQuantity,
            sum(p.Dollars) AS TotalPurchasesDollars
            From purchases p
            join Purchase_prices pp
            on p.Brand = pp.Brand
            where p.purchaseprice > 0
            Group by p.VendorNumber, p.VendorName, p.Brand
    )""")    