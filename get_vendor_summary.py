import pandas as pd
import sqlite3
import logging
import time
from ingestion_db import ingest_db

# Reset logging before configuring again (important for Jupyter)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
	
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
      ''' this function will merge different tables to get overall vendor summary adding different columns from different tables '''
      query = """ With freightSummary AS(select VendorNumber, VendorName ,
                    sum(Freight) as FreightCost 
                    from vendor_invoice group by VendorNumber),
                    
	                PurchaseSummary AS(select
                    p.VendorName,p.VendorNumber,p.Brand,pp.PurchasePrice,pp.Volume,
                    p.PurchasePrice as ActualPrice,
                    sum(p.Quantity) as TotalPurchaseQuantity,
                    sum(p.Dollars) as TotalPurchaseDollars
                    from purchases p 
                    join purchase_prices pp 
                    on p.Brand = pp.Brand 
                    group by p.VendorNumber
                    order by p.VendorNumber),
                  
                    SalesSummary AS (select VendorName,VendorNo,Brand,
                    sum(SalesQuantity) as TotalSalesQuantity,
                    sum(SalesDollars) as TotalSalesDollars,
                    sum(SalesPrice) as TotalSalesPrice,
                    sum(ExciseTax) as TotalEXciseTax
                    from sales_price
                    group by(VendorNo) 
                    order by VendorNo) 
                    select 
                    ps.VendorNumber,
                    ps.VendorName,
                    ps.Brand,
                    ps.PurchasePrice,
                    ps.ActualPrice,
                    ps.Volume,
                    ps.TotalPurchaseQuantity,
                    ps.TotalPurchaseDollars,
                    ss.TotalSalesQuantity,
                    ss.TotalSalesDollars,
                    ss.TotalExciseTax,
                    fs.FreightCost
                    from PurchaseSummary ps
                    left join SalesSummary ss on ps.VendorNumber = ss.VendorNo and ps.Brand = ss.Brand 
                    left join freightSummary fs on ps.VendorNumber = fs.VendorNumber 
                    order by ps.TotalPurchaseDollars"""
                    
      return pd.read_sql_query(query,conn)

def clear_data(df):
        ''' this function will clear the data '''
        df['Volume'] = df['Volume'].astype('float64')
        df['VendorName'] = df['VendorName'].str.strip()
        df.fillna(0,inplace=True)
        
        # Adding columns for better analysis
        df['grossProfit']=df['TotalSalesDollars'] - df['TotalPurchaseDollars']
        df['profitMargin']=(df['grossProfit']/df['TotalSalesDollars'] )*100
        df['stockTurnOver']=df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
        df['salesToPurchaseRatio']=df['TotalSalesDollars']/df['TotalPurchaseDollars']
        return df

if __name__ == '__main__' :
    conn = sqlite3.connect('inventory.db')
    logging.info('Creating vendor summary table.....')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head().to_string() )
    logging.info('Cleaning data .....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head().to_string() )
    logging.info('Ingesting data ....')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('Completetd')