I have used Brazilian E-Commerce Public Dataset by Olist from Kaggle.  This is a Brazilian 
ecommerce public dataset of orders made at Olist Store. The dataset has information of 100k orders 
from 2016 to 2018 made at multiple marketplaces in Brazil.  
There are 6 datasources used:  
• olist_order_items_dataset: Links orders to items, including order ID, item ID, product ID, seller 
ID, shipping limit date, price, and freight value.  
• olist_products_dataset: Contains product details such as product ID, category name, name 
length, description length, photos quantity, weight, dimensions (length, height, width).  
• olist_orders_dataset: Records order transactions, detailing order ID, customer ID, status, 
purchase timestamp, approved at, carrier delivery date, customer delivery date, and estimated 
delivery date.  
• olist_sellers_dataset: Lists seller information, including seller ID, zip code prefix, city, and 
state. 
• olist_geolocation_dataset: his dataset has information Brazilian zip codes and its lat/lng 
coordinates. Use it to plot maps and find distances between sellers and customers. 
• product_category_name_translation: Translates 
english(originially they are in portuguese).

https://drive.google.com/file/d/1S2MrrztvQgN8daMGWytfuMPvawLVL28J/view?usp=sharing    - Project representation

  ETL tool 
Apache Airflow has been choosen as an ETL engine. I have used docker compose to set up the airflow
webserver, scheduler and the database. The webserver is exposed on port 8080, allowing users to 
access the Airflow dashboard from their web browser. PostgreSQL is used as database as it is 
officially supported by Airflow. Database connection is set up by Airflow environment variable 
using credentials:  
AIRFLOW__CORE__SQL_ALCHEMY_CONN: 
postgresql+psycopg2://airflow:airflow@postgres/airflow 
For scheduling ecommerce_analysis_dag has been scheduled to run daily. It has 2 tasks to do. First is 
load which takes data from local csv files and upload it to PostgreSQL database. The second is 
perform_forecasting_2018_december which is for data science task to predict the next year holiday 
season using previous year data. 
Before loading I am doing data cleaning on datasets using Pandas which includes eliminating null values 
and few columns which are not needed for our analysis. Transformations happens on the Tableu later 
using SQL queries on original SQL tables.

