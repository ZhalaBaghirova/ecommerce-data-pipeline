from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def clean_data(file_path, drop_columns):
    try:
        df = pd.read_csv(file_path)
        df.drop(columns=drop_columns, inplace=True)
        df.dropna(inplace=True)
        return df
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
        raise
    
def load():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    datasets = {
        'olist_sellers': ('/Downloads/olist_sellers_dataset.csv',[]),
        'olist_order_items': ('/Downloads/olist_order_items_dataset.csv', ["freight_value", "shipping_limit_date"]),
        'olist_products': ('/Downloads/olist_products_dataset.csv', ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']),
        'olist_orders': ('/Downloads/olist_orders_dataset.csv', ["order_status", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]),
        'olist_forecast_results_2018':('/Downloads/forecast_results.csv',[]),
        'olist_geolocation':('/Downloads/olist_geolocation_dataset.csv',[]),
        'olist_product_category_name':('/Downloads/product_category_name_translation.csv',[]),
        'olist_forecast_results_2018':('/Downloads/forecast_results.csv',[])
     }

    for table_name, (file_path, columns_to_drop) in datasets.items():
        df = clean_data(file_path, columns_to_drop)
        if(file_path == '/Downloads/olist_geolocation_dataset.csv'):
            df=df.drop_duplicates(subset=['geolocation_zip_code_prefix'])  #only taking unique zipcodes
        df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Created table {table_name}")

        df.to_sql(table_name, engine, if_exists='append', index=False)
        logging.info(f"Data inserted into {table_name}")

def perform_forecasting():
    from prophet import Prophet
    products = pd.read_csv('/Downloads/olist_products_dataset.csv')
    orders = pd.read_csv('/Downloads/olist_orders_dataset.csv')
    order_items = pd.read_csv('/Downloads/olist_order_items_dataset.csv')
    category_translation = pd.read_csv('/Downloads/product_category_name_translation.csv')

    # Merge and prepare data
    data = pd.merge(order_items, orders, on='order_id')
    data = pd.merge(data, products, on='product_id')
    data = pd.merge(data, category_translation, on='product_category_name')

    # Filter data
    selected_categories = ('health_beauty', 'auto', 'toys', 'electronics', 'fashion_shoes')
    data = data[data['product_category_name_english'].isin(selected_categories)]
    data['order_purchase_timestamp'] = pd.to_datetime(data['order_purchase_timestamp'])
    data = data[data['order_purchase_timestamp'] >= '2017-01-01']

    # Aggregate sales by day and category
    daily_sales = data.groupby([pd.Grouper(key='order_purchase_timestamp', freq='D'), 'product_category_name_english']).agg({'price': 'sum'}).reset_index()
    daily_sales.rename(columns={'price': 'y', 'order_purchase_timestamp': 'ds', 'product_category_name_english': 'category'}, inplace=True)

    predictions = {}

    for category in selected_categories:
        df = daily_sales[daily_sales['category'] == category]
        m = Prophet(yearly_seasonality=True, daily_seasonality=False)
        m.fit(df[['ds', 'y']])
        last_date = pd.to_datetime(df['ds'].max())
        start_date = pd.to_datetime('2018-12-01')
        days_to_add = (start_date - last_date).days
        future = m.make_future_dataframe(periods=days_to_add +31, include_history=False)
        forecast = m.predict(future)
        forecast = forecast[(forecast['ds'] >= '2018-12-01') & (forecast['ds'] <= '2018-12-31')]
        forecast['category'] = category
        predictions[category] = forecast[['ds', 'category', 'yhat']]

        final_predictions = pd.concat(predictions.values())
        final_predictions.rename(columns={'ds': 'december_2018_day', 'yhat': 'predicted_price'}, inplace=True)
        final_predictions['december_2018_day'] = final_predictions['december_2018_day'].dt.day

        final_predictions.to_csv('/Downloads/forecast_results.csv', index=False)
        logging.info("Forecasting revenue per category is completed for 2018 december")


with DAG('ecommerce_analysis_dag',
         default_args=default_args,
         # sets schedule to run at 10 PM daily using Cron expressions
         schedule_interval='53 19 * * *',
         catchup=False) as dag:
    
    read_and_insert = PythonOperator(
        task_id='load',
        python_callable=load
    )
    forecasting_operator = PythonOperator(
    task_id='perform_forecasting_2018_december',
    python_callable=perform_forecasting,
    dag=dag)

read_and_insert
perform_forecasting
