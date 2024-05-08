import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session as session

def get_table_v1():
    
    conn = snowflake.connector.connect(
    account='JXGPFAR-REVOLT_PARTNER',
    user='USER_19',
    password='Rudsiq1234@',
    database='HACKATHON',
    warehouse='HACKATHON_WH',
    schema='TEAM_19',
    config_file='profiles.yml'
    )
    cursor = conn.cursor()

    # Execute a sample query
    query = "SELECT * FROM HACKATHON.DATA_SAMPLE.SAAS_SALES"
    cursor.execute(query)

    # Fetch results
    results = cursor.fetchall()

    # Print results
    columns = [desc[0] for desc in cursor.description]

    # Create a DataFrame from the query results and column names
    df = pd.DataFrame(results, columns=columns)

    # Print the DataFrame

    # Close the cursor and connection
    cursor.close()
    conn.close()

    df.head()
    df2 = df.copy()
    df1 = df.sort_values(by=['CUSTOMER_ID','SEGMENT','ORDER_DATE'])


    min_order_date = df1.groupby(['CUSTOMER_ID','SEGMENT']).agg({'ORDER_DATE': 'min'})
    min_order_date.rename(columns={'ORDER_DATE': 'ORDER_DATE_MIN'}, inplace=True)
    min_order_date.head(10)
    max_order_date = df1.groupby(['CUSTOMER_ID','SEGMENT']).agg({'ORDER_DATE': 'max'})
    max_order_date.rename(columns={'ORDER_DATE': 'ORDER_DATE_MAX'}, inplace=True)
    max_order_date.head(10)

    df_min_max_order_date = pd.concat([min_order_date, max_order_date], axis=1)
    df_min_max_order_date
    df_min_max_order_date = pd.concat([min_order_date, max_order_date], axis=1)
    df_min_max_order_date['ORDER_DATE_MIN'] = pd.to_datetime(df_min_max_order_date['ORDER_DATE_MIN'])
    df_min_max_order_date['ORDER_DATE_MAX'] = pd.to_datetime(df_min_max_order_date['ORDER_DATE_MAX'])


    df_min_max_order_date['Livespan_1'] = (df_min_max_order_date['ORDER_DATE_MAX'] - df_min_max_order_date['ORDER_DATE_MIN']).dt.days
    Average_Livespan_1 = df_min_max_order_date['Livespan_1'].mean()
    df_min_max_order_date
    # df_min_max_order_date['Retention_period_1'] = (df_min_max_order_date['ORDER_DATE_MAX'] - df_min_max_order_date['ORDER_DATE_MIN']).dt.days
    df_min_max_order_date['No_orders_time'] = (df_min_max_order_date['ORDER_DATE_MAX'].max() - df_min_max_order_date['ORDER_DATE_MAX']).dt.days
    # df_min_max_order_date.head()
    
    Retention_period_1 = df_min_max_order_date['No_orders_time'].quantile(0.8)
    df_min_max_order_date = session.create_dataframe(df_min_max_order_date)

    return Average_Livespan_1, Retention_period_1, df_min_max_order_date


def main():
    Average_Livespan_1, Retention_period_1, df_min_max_order_date = get_table_v1()

if __name__=="__main__":
    main()

