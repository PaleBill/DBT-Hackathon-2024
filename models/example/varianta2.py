import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session as session

def get_table_v2():
    
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
    df = df.sort_values(by=['CUSTOMER_ID','INDUSTRY','ORDER_DATE'])


    # min_order_date.rename(columns={'ORDER_DATE': 'ORDER_DATE_MIN'}, inplace=True)
    # min_order_date.head(10)
    df1

    min_order_date = df.groupby(['CUSTOMER_ID','INDUSTRY']).agg({'ORDER_DATE': 'min'})
    min_order_date.rename(columns={'ORDER_DATE': 'ORDER_DATE_MIN'}, inplace=True)
    min_order_date.head(10)
    max_order_date = df.groupby(['CUSTOMER_ID','INDUSTRY']).agg({'ORDER_DATE': 'max'})
    max_order_date.rename(columns={'ORDER_DATE': 'ORDER_DATE_MAX'}, inplace=True)
    max_order_date.head(10)

    df_min_max_order_date = pd.concat([min_order_date, max_order_date], axis=1)
    df_min_max_order_date
    df_min_max_order_date = pd.concat([min_order_date, max_order_date], axis=1)
    df_min_max_order_date['ORDER_DATE_MIN'] = pd.to_datetime(df_min_max_order_date['ORDER_DATE_MIN'])
    df_min_max_order_date['ORDER_DATE_MAX'] = pd.to_datetime(df_min_max_order_date['ORDER_DATE_MAX'])


    Average_Livespan_2 = 365*3
    df_min_max_order_date
    # df_min_max_order_date['Retention_period_1'] = (df_min_max_order_date['ORDER_DATE_MAX'] - df_min_max_order_date['ORDER_DATE_MIN']).dt.days
    Retention_period_2 = 90
    df_min_max_order_date2 = session.create_dataframe(df_min_max_order_date2)

    return Average_Livespan_2, Retention_period_2, df_min_max_order_date2



def main():
    Average_Livespan_1, Retention_period_1, df_min_max_order_date = get_table_v2()

if __name__=="__main__":
    main()

