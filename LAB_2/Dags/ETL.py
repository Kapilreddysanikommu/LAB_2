import yfinance as yf
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

def return_snowflake_conn():
    """Return a Snowflake cursor object."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(ticker_symbols):
    """Extract stock data from yfinance"""
    data = {}
    for symbol in ticker_symbols:
        ticker = yf.Ticker(symbol)
        stock_data = ticker.history(period='180d')
        data[symbol] = stock_data
    return data

@task
def transform(stock_data):
    """Transform extracted stock data"""
    records = []
    for symbol, data in stock_data.items():
        for date, row in data.iterrows():
            records.append((symbol, date.date(), row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]))
    return records

@task
def load(records, target_table):
    """Load transformed stock data into Snowflake"""
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol VARCHAR,
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                PRIMARY KEY (symbol, date)
            );
        """)

        # Using MERGE (UPSERT) for better data consistency
        for r in records:
            sql = f"""
                MERGE INTO {target_table} AS target
                USING (SELECT '{r[0]}' AS symbol, '{r[1]}' AS date, {r[2]} AS open, {r[3]} AS high, 
                              {r[4]} AS low, {r[5]} AS close, {r[6]} AS volume) AS src
                ON target.symbol = src.symbol AND target.date = src.date
                WHEN MATCHED THEN 
                    UPDATE SET open = src.open, high = src.high, low = src.low, 
                               close = src.close, volume = src.volume
                WHEN NOT MATCHED THEN 
                    INSERT (symbol, date, open, high, low, close, volume) 
                    VALUES (src.symbol, src.date, src.open, src.high, src.low, src.close, src.volume);
            """
            cur.execute(sql)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id='ETL',
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=['ETL'],
    schedule_interval='0 7 * * *',
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=5)}
) as dag_1:

  # Handle missing Airflow Variable
    ticker_symbols = Variable.get("stock_symbol", default_var="TLSA,MSFT").split(",")
    target_table = "USER_DB_FINCH.RAW.stock_price"

    extracted_data = extract(ticker_symbols)
    transformed_records = transform(extracted_data)
    loaded_data = load(transformed_records, target_table)

    trigger_ELT = TriggerDagRunOperator(
        task_id='trigger_dbt_dag',
        trigger_dag_id='ELT_DBT',
        conf={},
        dag=dag_1
    )

    extracted_data >> transformed_records >> loaded_data >> trigger_ELT