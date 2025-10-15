from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
import pandas as pd
import matplotlib.pyplot as plt
import os


PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='retail_data_pipeline_dag',
    default_args=default_args,
    description='End-to-End Retail Data Pipeline with PostgreSQL, Python & Airflow',
    schedule_interval=None, 
    start_date=days_ago(1),
    catchup=False,
) as dag:

 
    def extract_data():
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = hook.get_conn()
        query = """
        SELECT 
            o.OrderDate::date AS sale_date,
            od.ProductID,
            p.ProductName,
            od.Quantity,
            p.Price
        FROM orders o
        JOIN order_details od ON o.OrderID = od.OrderID
        JOIN products p ON od.ProductID = p.ProductID;
        """
        df = pd.read_sql(query, conn)
        os.makedirs('/home/kiwilytics/pipline_project', exist_ok=True)
        df.to_csv('/home/kiwilytics/pipline_project/data/daily_sales_data.csv', index=False)
        print(" Data extracted and saved as daily_sales_data.csv")

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

 
    def calculate_daily_revenue():
        df = pd.read_csv('/home/kiwilytics/pipline_project/data/daily_sales_data.csv')
        df['total_revenue'] = df['quantity'] * df['price']
        revenue_per_day = df.groupby('sale_date').agg(total_revenue=('total_revenue', 'sum')).reset_index()
        revenue_per_day.to_csv('/home/kiwilytics/pipline_project/data/daily_revenue.csv', index=False)
        print(" Daily revenue calculated and saved as daily_revenue.csv")

    revenue_task = PythonOperator(
        task_id='calculate_daily_revenue',
        python_callable=calculate_daily_revenue
    )


    def visualize_revenue():
        df = pd.read_csv('/home/kiwilytics/pipline_project/data/daily_revenue.csv')
        plt.figure(figsize=(10, 5))
        plt.plot(df['sale_date'], df['total_revenue'], marker='o')
        plt.title('Daily Sales Revenue Over Time')
        plt.xlabel('Date')
        plt.ylabel('Total Revenue')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('/home/kiwilytics/pipline_project/plot/daily_revenue_plot.png')
        print("Plot saved as daily_revenue_plot.png")

    visualize_task = PythonOperator(
        task_id='visualize_revenue',
        python_callable=visualize_revenue
    )

    def save_revenue_for_specific_date():

        df = pd.read_csv('/home/kiwilytics/pipline_project/data/daily_revenue.csv')
        target_date = '1996-08-08'

        result = df[df['sale_date'] == target_date][['sale_date', 'total_revenue']]

        if not result.empty:
            os.makedirs('/home/kiwilytics/pipline_project', exist_ok=True)
            result.to_csv('/home/kiwilytics/pipline_project/data/revenue_1996-08-08.csv', index=False)
            print(f"Revenue for {target_date} saved to revenue_1996-08-08.csv")
        else:
            print(f"No data found for {target_date}.")

    save_fixed_date_task = PythonOperator(
    task_id='save_revenue_for_1996_08_08',
    python_callable=save_revenue_for_specific_date
    )

extract_task >> revenue_task >> visualize_task >> save_fixed_date_task