CREATE SCHEMA mydataset;

DROP TABLE IF EXISTS mydataset.user_purchase;

CREATE TABLE mydataset.user_purchase (
    InvoiceNo STRING,
    StockCode STRING,
    detail STRING,
    Quantity INT64,
    InvoiceDate DATE,
    UnitPrice FLOAT64,
    customerid INT64,
    Country STRING
) PARTITION BY _PARTITIONDATE;

DROP TABLE IF EXISTS mydataset.country_sales;

CREATE TABLE mydataset.country_sales (
    InvoiceDate DATE,
    Country STRING,
    Quantity INT64,
    GMV FLOAT64
);