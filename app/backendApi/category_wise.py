# import pandas as pd
# import numpy as np
# import psycopg2
# from psycopg2 import sql
# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# from typing import Optional, Union, List
# import uvicorn
# from fastapi.middleware.cors import CORSMiddleware

# from multiprocessing import Pool, cpu_count  # Import necessary modules for multiprocessing
# app = FastAPI()

# # Add CORS middleware
# origins = [
#     "http://localhost",
#     "http://localhost:3000",
#     "https://example.com",
#     "*",
#     "(*)"
# ]

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # Database connection parameters
# DB_PARAMS = {
#     'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
#     'port': '5434',
#     'dbname': 'KIESQUAREDE',
#     'user': 'KIESQUAREDE',
#     'password': 'KIESQUARE123'
# }

# # Schema name
# SCHEMA_NAME = 'dq'

# def to_native(value):
#     if isinstance(value, np.generic):
#         return value.item()
#     return value
# # Wrapper function for parallel processing

# def parallel_calculation(args):

#     df, metric, metric_dict = args

#     return calculate_metric(df, metric, metric_dict)



# # Function to handle multiprocessing for metric calculations

# def calculate_metrics_parallel(df, metrics, metric_dict):

#     # Prepare the arguments for parallel processing

#     args = [(df, metric, metric_dict) for metric in metrics]

#     # Use the Pool to parallelize the calculations

#     with Pool(processes=cpu_count()) as pool:

#         results = pool.map(parallel_calculation, args)

#     return dict(zip(metrics, results))
# def calculate_metric(df, metric, metric_dict):
#     #print(df.columns)
#     spend_column = df['search___spends'] if 'search___spends' in df.columns else \
#                          df['spends'] if 'spends' in df.columns else \
#                          df['display___spends'] if 'display___spends' in df.columns else None
#     add_to_column = df['add_to_cart'] if 'add_to_cart' in df.columns else \
#                  df['add_to_basket'] if 'add_to_basket' in df.columns else None
#     calculation_logic = metric_dict.get(metric, '').lower()
#     if 'average' in calculation_logic:
#         return to_native(df[metric].mean())
#     elif 'sum' in calculation_logic:
#         return to_native(df[metric].sum())

#     else:
#         # For any remaining complex calculations
#         if metric.lower() == 'ctr':
#             return to_native((df['clicks'].sum() / df['impressions'].sum()))
        
#         elif metric.lower() == 'ACOS %':
#             return to_native((df['Spends'].sum() / df['Sales'].sum()) * 100)
#         elif metric.lower() == 'engagement %':
#             return to_native((df['Engagement'].sum() / df['Followers'].sum()) * 100)
#         elif metric.lower() == 'engagement':
#             return to_native((df['Engagement'].sum() / df['Followers'].sum()) * 100)
#         # elif metric.lower() == 'vtr':
#         #     return to_native((df['Completed views'].sum() / df['impressions'].sum()) * 100)
#         elif metric.lower() == 'transaction rate':
#             return to_native((df['transactions'].sum() / df['clicks'].sum()) * 100)
#         elif metric.lower() == 'product views per session':
#             return to_native(df['Product views'].sum() / df['Session'].sum())
#         elif metric.lower() == 'sessions to product views %':
#             return to_native((df['Product Views'].sum() / df['Sessions'].sum()) * 100)
#         elif metric.lower() == 'product views to cart %':
#             return to_native((df['Add to cart'].sum() / df['Product Views'].sum()) * 100)
#         elif metric.lower() == 'cart to checkout %':
#             return to_native((df['Checkout'].sum() / df['Add to cart'].sum()) * 100)
#         elif metric.lower() == 'check out to transaction %':
#             return to_native((df['Orders'].sum() / df['checkout initiated'].sum()) * 100)
#         elif metric.lower() == 'overall conversion %':
#             return to_native((df['Transactions'].sum() / df['Sessions'].sum()) * 100)
#         elif metric.lower() == 'repeat rate %':
#             return to_native((1 - (df['customers acquired'].sum() / df['transactions'].sum())) * 100)
#         elif metric.lower() == 'transaction_rate':
#             if 'transactions' not in df.columns or  'clicks' not in df.columns:
#                 return None
#             return to_native((df['transactions'].sum() / df['clicks'].sum()))
#         elif metric.lower() == 'cost_per_transaction':
#             if 'transactions' not in df.columns or  'spends' not in df.columns:
#                 return None
#             return to_native((df['transactions'].sum() / df['spends'].sum()))
#         elif metric.lower() == 'order_conversion_rate':
#             if 'purchases' not in df.columns or  'clicks' not in df.columns:
#                 return None
#             return to_native((df['purchases'].sum() / df['clicks'].sum()))
#         elif metric.lower() == 'cpm':
#             if spend_column is None or 'impressions' not in df.columns:
#                 return None
#             return to_native((spend_column.sum() / df['impressions'].sum()) * 1000)
#         elif metric.lower() == 'cpc':
#             if spend_column is None or 'clicks' not in df.columns:
#                 return None
#             return to_native(spend_column.sum() / df['clicks'].sum())
#         elif metric.lower() == 'acos__':
#             if spend_column is None or 'sales_value' not in df.columns:
#                 return None
#             return to_native(spend_column.sum() / df['sales_value'].sum())
#         elif metric.lower() == 'click_to_cart__':
#             if 'add_to_cart' not in df.columns or 'clicks' not in df.columns:
#                 return None
#             return to_native(df['add_to_cart'].sum() / df['clicks'].sum())
#         elif metric.lower() == 'cart_to_checkout__':
#             if add_to_column is None or 'checkout' not in df.columns:
#                 return None
#             return to_native(df['checkout'].sum() / add_to_column.sum())
#         # elif metric.lower() == 'aov':
#         #     if spend_column is None or 'transactions' not in df.columns:
#         #         return None
#             return to_native(df['transactions'].sum() / spend_column.sum())
#         elif metric.lower() == 'product_views_per_session':
#             if 'product_views_per_session' not in df.columns or 'product_views' not in df.columns:
#                 return None
#             return to_native(df['sessions'].sum() / df['product_views'].sum())
#         elif metric.lower() == 'product_views_per_session':
#             if 'sessions' not in df.columns or 'product_views' not in df.columns:
#                 return None
#             return to_native(df['sessions'].sum() / df['product_views'].sum())
#         elif metric.lower() == 'sessions_to_product_views__':
#             if 'sessions' not in df.columns or 'product_views' not in df.columns:
#                 return None
#             return to_native(df['product_views'].sum() / df['sessions'].sum())
#         elif metric.lower() == 'product_views_to_cart__':
#             if add_to_column is None or 'product_views' not in df.columns:
#                 return None
#             return to_native(add_to_column.sum() / df['product_views'].sum())
#         elif metric.lower() == 'cart_to_checkout__':
#             if add_to_column is None or 'checkout' not in df.columns:
#                 return None
#             return to_native(df['checkout'].sum() / add_to_column.sum())
#         elif metric.lower() == 'overall_conversion__':
#             if 'transactions' not in df.columns or 'checkout' not in df.columns:
#                 return None
#             return to_native(df['checkout'].sum() / df['transactions'].sum())
#         else:
#             print(f"Warning: Complex calculation for {metric} not implemented. Returning sum.")
#             return to_native(df[metric].sum() if metric in df.columns else np.nan)

# def load_metric_list(file_path=r'/app/backendApi/Metric_List.csv'):
#     try:
#         df = pd.read_csv(file_path)
#         metric_dict = dict(zip(df['Metrics List'], df['Calculation Logic']))
#         return metric_dict

#     except FileNotFoundError:
#         print(f"Error: File '{file_path}' not found.")
#         return {}
#     except pd.errors.EmptyDataError:
#         print(f"Error: File '{file_path}' is empty.")
#         return {}
#     except pd.errors.ParserError:
#         print(f"Error: Unable to parse '{file_path}'. Make sure it's a valid CSV file.")
#         return {}
#     except KeyError as e:
#         print(f"Error: Expected column {e} not found in the CSV file.")
#         return {}
# def calculate_percentile(data, percentile):
#     if isinstance(data, np.ndarray):
#         data = pd.Series(data)
#     return data.quantile(percentile)
# def determine_metric_type(metric, metric_dict):
#     calculation_logic = metric_dict.get(metric, '').lower()

#     if 'average' in calculation_logic or 'latest' in calculation_logic:
#         return 'average'
#     elif 'sum' in calculation_logic:
#         return 'sum'
#     else:
#         # For complex calculations, we'll still use 'sum' as the aggregation method
#         return 'sum'

# # Load the metric list
# METRIC_DICT = load_metric_list()

# def connect_to_db():
#     try:
#         conn = psycopg2.connect(**DB_PARAMS)
#         print("Successfully connected to the database.")
#         return conn
#     except psycopg2.Error as e:
#         print(f"Unable to connect to the database: {e}")
#         return None

# def load_master_table(conn):
#     query = f"SELECT * FROM {SCHEMA_NAME}.master_table_platform_metrics_relationship_new"
#     try:
#         df = pd.read_sql_query(query, conn)
#         print(f"Successfully loaded master table. Shape: {df.shape}")
#         return df
#     except psycopg2.Error as e:
#         print(f"Error querying the master table: {e}")
#         return None

# def fetch_data(conn, table_name, db_metric, brand=None, category=None, start_date=None, end_date=None):
#     base_query = sql.SQL("""
#         SELECT m.*, c.category, c.brand, m.date
#         FROM {schema}.{table} m
#         JOIN {schema}.master_table_category_brand c ON m.unique_id = c.unique_id
#         WHERE 1=1
#     """).format(
#         schema=sql.Identifier(SCHEMA_NAME),
#         table=sql.Identifier(table_name)
#     )

#     conditions = []
#     params = []

#     if category:
#         if isinstance(category, list):
#             conditions.append(sql.SQL("c.category IN %s"))
#             params.append(tuple(category))
#         else:
#             conditions.append(sql.SQL("c.category = %s"))
#             params.append(category)
#     if brand:
#         if isinstance(brand, list):
#             conditions.append(sql.SQL("c.brand IN %s"))
#             params.append(tuple(brand))
#         else:
#             conditions.append(sql.SQL("c.brand = %s"))
#             params.append(brand)
#     if start_date:
#         conditions.append(sql.SQL("m.date >= %s"))
#         params.append(start_date)
#     if end_date:
#         conditions.append(sql.SQL("m.date <= %s"))
#         params.append(end_date)

#     if conditions:
#         where_clause = sql.SQL(" AND ").join(conditions)
#         query = base_query + sql.SQL(" AND ") + where_clause
#     else:
#         query = base_query
#     if table_name == 'seoptimer':

#         query += sql.SQL(" ORDER BY m.date DESC LIMIT 1")    

#     # Convert the query to a string
#     query_str = query.as_string(conn)

#     try:
#         df = pd.read_sql_query(query_str, conn, params=params)
#         return df
#     except psycopg2.Error as e:
#         print(f"Error fetching data: {e}")
#         return None


# def calculate_special_metrics(data, metric):
#     if metric == 'CTR':
#         return (data['clicks'].sum() / data['impressions'].sum())
#     elif metric == 'Engagement %':
#         return (data['Engagement'].sum() / data['Followers'].sum()) * 100
#     # elif metric == 'VTR':
#     #     return (data['completed views'].sum() / data['impressions'].sum()) * 100
#     elif metric == 'Transaction Rate':
#         return (data['transactions'].sum() / data['clicks'].sum()) * 100
#     elif metric == 'Cost per Transaction':
#         return (data['cost'].sum() / data['transactions'].sum())
#     # elif metric == 'AOV':
#     #     return (data['revenue'].sum() / data['transactions'].sum())
#     elif metric == 'Product Views per Session':
#         return (data['product_views'].sum() / data['sessions'].sum())
#     elif metric == 'Sessions to Product Views %':
#         return (data['product_views'].sum() / data['sessions'].sum()) * 100
#     elif metric == 'Product Views to Cart %':
#         return (data['carts'].sum() / data['product_views'].sum()) * 100
#     else:
#         return None




# import pandas as pd



# def process_metric(master_table, conn, platforms, metrics, brands, analysis_type, start_date=None, end_date=None, category=None):
#     results = []

#     # Ensure metrics, platforms, and brands are lists
#     if isinstance(metrics, str):
#         metrics = [metrics]
#     if isinstance(platforms, str):
#         platforms = [platforms]
#     if isinstance(brands, str):
#         brands = [brands]

#     # Iterate through each platform and metric
#     for platform in platforms:
#         for metric in metrics:
#             # Get the metric information from the master_table
#             metric_info = master_table[(master_table['platform'] == platform) & (master_table['metrics'] == metric)].iloc[0]
#             percentile = 0.25 if metric_info['relationship'] == 1 else 0.75

#             if analysis_type == 'Overall':
#                 # Fetch combined data without category filtering
#                 combined_data = fetch_data(conn, metric_info['table_name'], metric_info['db_metrics'], brand=brands, category=None, start_date=start_date, end_date=end_date)

#                 if combined_data is not None and not combined_data.empty:
#                     raw_values = []
#                     if metric in ['CTR', 'Engagement %', 'Transaction Rate', 'Cost per Transaction', 'Product Views per Session', 'Sessions to Product Views %', 'Product Views to Cart %']:
#                         # Handle special metrics by calculating for each brand and aggregating
#                         for brand in brands:
#                             brand_data = combined_data[combined_data['brand'] == brand]
#                             if not brand_data.empty:
#                                 result = calculate_special_metrics(brand_data, metric)
#                                 raw_values.append({'brand': brand, 'metric': metric, 'value': result})
#                         overall_values = pd.DataFrame(raw_values)['value'].values
#                         overall_result = calculate_percentile(overall_values, percentile)
#                     else:
#                         # For non-special metrics, aggregate and calculate percentiles directly
#                         aggregated_data = combined_data.groupby(['brand'])[metric_info['db_metrics']].sum()
#                         overall_values = aggregated_data.values
#                         overall_result = calculate_percentile(overall_values, percentile)
#                         raw_values = [{'brand': brand, 'metric': metric, 'value': value} for brand, value in aggregated_data.items()]

#                     # Append results for "Overall" analysis type
#                     results.append({
#                         "platform": platform,
#                         "metric": metric,
#                         "definition": metric_info['definition'],
#                         "brands": brands,
#                         "analysis_type": list(combined_data['category'].unique()),  # List of unique categories
#                         "category": "Overall",
#                         "percentile": percentile,
#                         "value": overall_result,
#                         "actualValue": {r['brand']: r['value'] for r in raw_values},
#                         "categories": list(combined_data['category'].unique())
#                     })

#             else:
#                 # Handling for specific categories when analysis_type is not "Overall"
#                 categories = analysis_type if isinstance(analysis_type, list) else [analysis_type]

#                 for cat in categories:
#                     combined_data = fetch_data(conn, metric_info['table_name'], metric_info['db_metrics'], brand=brands, category=cat, start_date=start_date, end_date=end_date)

#                     if combined_data is not None and not combined_data.empty:
#                         raw_values = []
#                         if metric in ['CTR', 'Engagement %', 'Transaction Rate', 'Cost per Transaction', 'Product Views per Session', 'Sessions to Product Views %', 'Product Views to Cart %']:
#                             # Handle special metrics by calculating for each brand within the category
#                             for brand in brands:
#                                 brand_data = combined_data[(combined_data['brand'] == brand) & (combined_data['category'] == cat)]
#                                 if not brand_data.empty:
#                                     result = calculate_special_metrics(brand_data, metric)
#                                     raw_values.append({
#                                         'category': cat,
#                                         'brand': brand,
#                                         'metric': metric,
#                                         'value': result
#                                     })
#                             if raw_values:
#                                 raw_values_df = pd.DataFrame(raw_values)
#                                 aggregated_data = raw_values_df.pivot_table(
#                                     index=['category', 'brand'],
#                                     values='value',
#                                     aggfunc='sum'
#                                 )
#                                 result = calculate_percentile(aggregated_data['value'], percentile)
#                                 raw_values = aggregated_data.reset_index().to_dict(orient='records')
#                         else:
#                             # For non-special metrics, aggregate normally
#                             aggregated_data = combined_data.pivot_table(
#                                 index=['category', 'brand'],
#                                 values=metric_info['db_metrics'],
#                                 aggfunc='sum'
#                             )
#                             raw_values_df = aggregated_data.reset_index()
#                             result = calculate_percentile(aggregated_data[metric_info['db_metrics']], percentile)
#                             raw_values = raw_values_df.rename(columns={metric_info['db_metrics']: 'value'}).to_dict(orient='records')
#                             # Ensure 'metric' field is added to each entry
#                             for entry in raw_values:
#                                 entry['metric'] = metric

#                         # Append results for specific categories
#                         results.append({
#                             "platform": platform,
#                             "metric": metric,
#                             "definition": metric_info['definition'],
#                             "brands": brands,
#                             "analysis_type": list(combined_data['category'].unique()),  # List of unique categories
#                             "category": cat,
#                             "percentile": percentile,
#                             "value": result,
#                             "actualValue": {r['brand']: r['value'] for r in raw_values},
#                             "categories": [cat]
#                         })

#     return {"results": results}


# class MetricRequest(BaseModel):
#     platform: Union[str, List[str]]
#     metrics: Union[str, List[str]]
#     brand: Union[str, List[str]]  # Accept either a single brand or a list of brands
#     analysis_type: Union[str, List[str]]  # Accept either a string or a list of strings
#     start_date: Optional[str] = None
#     end_date: Optional[str] = None
#     category: Optional[Union[str, List[str]]] = None  # Added category field

# # @app.post("/process_metric")
# # async def api_process_metric(request: MetricRequest):
# #     conn = connect_to_db()
# #     if conn is None:
# #         raise HTTPException(status_code=500, detail="Unable to connect to the database")

# #     master_table = load_master_table(conn)
# #     if master_table is None:
# #         conn.close()
# #         raise HTTPException(status_code=500, detail="Unable to load master table")

# #     try:
# #         results = process_metric(master_table, conn, request.platform, request.metrics, request.brand, request.analysis_type, request.start_date, request.end_date, request.category)
# #         return {"results": results}
# #     finally:
# #         conn.close()






# from starlette.middleware.cors import CORSMiddleware
# from fastapi import FastAPI, Request, HTTPException

# @app.middleware("http")
# async def log_requests(request: Request, call_next):
#     response = await call_next(request)
#     logger.info(f"{request.method} {request.url} - {response.status_code}")
#     return response

# import logging

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# @app.post("/process_metric/")

# async def api_process_metric(request: MetricRequest):

#     conn = connect_to_db()

#     if conn is None:

#         logger.error("Database connection failed.")

#         raise HTTPException(status_code=500, detail="Unable to connect to the database")



#     master_table = load_master_table(conn)

#     if master_table is None:

#         logger.error("Failed to load master table.")

#         conn.close()

#         raise HTTPException(status_code=500, detail="Unable to load master table")



#     try:

#         results = process_metric(master_table, conn, request.platform, request.metrics, request.brand, request.analysis_type, request.start_date, request.end_date, request.category)

#         logger.info(f"Successfully processed metrics: {results}")

#         return {"results": results}

#     except HTTPException as e:

#         logger.error(f"HTTPException occurred: {e.detail}", exc_info=True)

#         raise

#     except Exception as e:

#         logger.error("An unexpected error occurred", exc_info=True)

#         raise HTTPException(status_code=500, detail="An unexpected error occurred")

#     finally:

#         conn.close()
# # if __name__ == "__main__":
# #     uvicorn.run(app, host="0.0.0.0", port=8000)




# # /process_metric/



# # {
# #     "platform": "Amazon - Search Campaigns",
# #     "metrics": ["Impressions","CTR","ACOS %"],
# #     "brand": ["Pure Sense","Livon"],
# #     "analysis_type": "Overall",
# #     "start_date": "2023-01-01",
# #     "end_date": "2024-12-31"
# # }







import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Union, List
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

from multiprocessing import Pool, cpu_count  # Import necessary modules for multiprocessing
app = FastAPI()

# Add CORS middleware
origins = [
    "http://localhost",
    "http://localhost:3000",
    "https://example.com",
    "*",
    "(*)"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection parameters
DB_PARAMS = {
    'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
    'port': '5434',
    'dbname': 'KIESQUAREDE',
    'user': 'KIESQUAREDE',
    'password': 'KIESQUARE123'
}

# Schema name
SCHEMA_NAME = 'dq'

def to_native(value):
    if isinstance(value, np.generic):
        return value.item()
    return value
# Wrapper function for parallel processing

def parallel_calculation(args):

    df, metric, metric_dict = args

    return calculate_metric(df, metric, metric_dict)



# Function to handle multiprocessing for metric calculations

def calculate_metrics_parallel(df, metrics, metric_dict):

    # Prepare the arguments for parallel processing

    args = [(df, metric, metric_dict) for metric in metrics]

    # Use the Pool to parallelize the calculations

    with Pool(processes=cpu_count()) as pool:

        results = pool.map(parallel_calculation, args)

    return dict(zip(metrics, results))
def calculate_metric(df, metric, metric_dict):
    #print(df.columns)
    spend_column = df['search___spends'] if 'search___spends' in df.columns else \
                         df['spends'] if 'spends' in df.columns else \
                         df['display___spends'] if 'display___spends' in df.columns else None
    add_to_column = df['add_to_cart'] if 'add_to_cart' in df.columns else \
                 df['add_to_basket'] if 'add_to_basket' in df.columns else None
    calculation_logic = metric_dict.get(metric, '').lower()
    if metric.lower() in ['average_ratings', 'average ratings', 'average_ratings', 'avg_ratings','Net_sentiment_of_reviews','Net sentiment','net_sentiment_of_reviews','net_sentiment']:
        return to_native(df['ratings'].mean())

    if 'Average' in calculation_logic:
        return to_native(df[metric].mean())
    elif 'sum' in calculation_logic:
        return to_native(df[metric].sum())

    else:
        # For any remaining complex calculations
        if metric.lower() == 'ctr':
            return to_native((df['clicks'].sum() / df['impressions'].sum()))
        
        elif metric.lower() == 'ACOS %':
            return to_native((df['Spends'].sum() / df['Sales'].sum()) * 100)
        elif metric.lower() == 'engagement %':
            return to_native((df['Engagement'].sum() / df['Followers'].sum()) * 100)
        elif metric.lower() == 'engagement':
            return to_native((df['Engagement'].sum() / df['Followers'].sum()) * 100)
        # elif metric.lower() == 'vtr':
        #     return to_native((df['Completed views'].sum() / df['impressions'].sum()) * 100)
        elif metric.lower() == 'transaction rate':
            return to_native((df['transactions'].sum() / df['clicks'].sum()) * 100)
        elif metric.lower() == 'product views per session':
            return to_native(df['Product views'].sum() / df['Session'].sum())
        elif metric.lower() == 'sessions to product views %':
            return to_native((df['Product Views'].sum() / df['Sessions'].sum()) * 100)
        elif metric.lower() == 'product views to cart %':
            return to_native((df['Add to cart'].sum() / df['Product Views'].sum()) * 100)
        elif metric.lower() == 'cart to checkout %':
            return to_native((df['Checkout'].sum() / df['Add to cart'].sum()) * 100)
        elif metric.lower() == 'check out to transaction %':
            return to_native((df['Orders'].sum() / df['checkout initiated'].sum()) * 100)
        elif metric.lower() == 'overall conversion %':
            return to_native((df['Transactions'].sum() / df['Sessions'].sum()) * 100)
        elif metric.lower() == 'repeat rate %':
            return to_native((1 - (df['customers acquired'].sum() / df['transactions'].sum())) * 100)
        elif metric.lower() == 'transaction_rate':
            if 'transactions' not in df.columns or  'clicks' not in df.columns:
                return None
            return to_native((df['transactions'].sum() / df['clicks'].sum()))
        elif metric.lower() == 'cost_per_transaction':
            if 'transactions' not in df.columns or  'spends' not in df.columns:
                return None
            return to_native((df['transactions'].sum() / df['spends'].sum()))
        elif metric.lower() == 'order_conversion_rate':
            if 'purchases' not in df.columns or  'clicks' not in df.columns:
                return None
            return to_native((df['purchases'].sum() / df['clicks'].sum()))
        elif metric.lower() == 'cpm':
            if spend_column is None or 'impressions' not in df.columns:
                return None
            return to_native((spend_column.sum() / df['impressions'].sum()) * 1000)
        elif metric.lower() == 'cpc':
            if spend_column is None or 'clicks' not in df.columns:
                return None
            return to_native(spend_column.sum() / df['clicks'].sum())
        elif metric.lower() == 'acos__':
            if spend_column is None or 'sales_value' not in df.columns:
                return None
            return to_native(spend_column.sum() / df['sales_value'].sum())
        elif metric.lower() == 'click_to_cart__':
            if 'add_to_cart' not in df.columns or 'clicks' not in df.columns:
                return None
            return to_native(df['add_to_cart'].sum() / df['clicks'].sum())
        elif metric.lower() == 'cart_to_checkout__':
            if add_to_column is None or 'checkout' not in df.columns:
                return None
            return to_native(df['checkout'].sum() / add_to_column.sum())
        # elif metric.lower() == 'aov':
        #     if spend_column is None or 'transactions' not in df.columns:
        #         return None
            return to_native(df['transactions'].sum() / spend_column.sum())
        elif metric.lower() == 'product_views_per_session':
            if 'product_views_per_session' not in df.columns or 'product_views' not in df.columns:
                return None
            return to_native(df['sessions'].sum() / df['product_views'].sum())
        elif metric.lower() == 'product_views_per_session':
            if 'sessions' not in df.columns or 'product_views' not in df.columns:
                return None
            return to_native(df['sessions'].sum() / df['product_views'].sum())
        elif metric.lower() == 'sessions_to_product_views__':
            if 'sessions' not in df.columns or 'product_views' not in df.columns:
                return None
            return to_native(df['product_views'].sum() / df['sessions'].sum())
        elif metric.lower() == 'product_views_to_cart__':
            if add_to_column is None or 'product_views' not in df.columns:
                return None
            return to_native(add_to_column.sum() / df['product_views'].sum())
        elif metric.lower() == 'cart_to_checkout__':
            if add_to_column is None or 'checkout' not in df.columns:
                return None
            return to_native(df['checkout'].sum() / add_to_column.sum())
        elif metric.lower() == 'overall_conversion__':
            if 'transactions' not in df.columns or 'checkout' not in df.columns:
                return None
            return to_native(df['checkout'].sum() / df['transactions'].sum())
        else:
            print(f"Warning: Complex calculation for {metric} not implemented. Returning sum.")
            return to_native(df[metric].sum() if metric in df.columns else np.nan)

def load_metric_list(file_path=r'/Users/saptarshijana/Downloads/dq_api_backup/Metric_List.csv'):
    try:
        df = pd.read_csv(file_path)
        metric_dict = dict(zip(df['Metrics List'], df['Calculation Logic']))
        return metric_dict

    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return {}
    except pd.errors.EmptyDataError:
        print(f"Error: File '{file_path}' is empty.")
        return {}
    except pd.errors.ParserError:
        print(f"Error: Unable to parse '{file_path}'. Make sure it's a valid CSV file.")
        return {}
    except KeyError as e:
        print(f"Error: Expected column {e} not found in the CSV file.")
        return {}
def calculate_percentile(data, percentile):
    if isinstance(data, np.ndarray):
        data = pd.Series(data)
    return data.quantile(percentile)
def determine_metric_type(metric, metric_dict):
    calculation_logic = metric_dict.get(metric, '').lower()
    

    if 'Average' in calculation_logic or 'latest' in calculation_logic:
        return 'average'
    
    elif 'sum' in calculation_logic:
        return 'sum'
    else:
        # For complex calculations, we'll still use 'sum' as the aggregation method
        return 'sum'

# Load the metric list
METRIC_DICT = load_metric_list()

def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        return None

def load_master_table(conn):
    query = f"SELECT * FROM {SCHEMA_NAME}.master_table_platform_metrics_relationship_new"
    try:
        df = pd.read_sql_query(query, conn)
        print(f"Successfully loaded master table. Shape: {df.shape}")
        return df
    except psycopg2.Error as e:
        print(f"Error querying the master table: {e}")
        return None

def fetch_data(conn, table_name, db_metric, brand=None, category=None, start_date=None, end_date=None):
    base_query = sql.SQL("""
        SELECT m.*, c.category, c.brand, m.date
        FROM {schema}.{table} m
        JOIN {schema}.master_table_category_brand c ON m.unique_id = c.unique_id
        WHERE 1=1
    """).format(
        schema=sql.Identifier(SCHEMA_NAME),
        table=sql.Identifier(table_name)
    )

    conditions = []
    params = []

    if category:
        if isinstance(category, list):
            conditions.append(sql.SQL("c.category IN %s"))
            params.append(tuple(category))
        else:
            conditions.append(sql.SQL("c.category = %s"))
            params.append(category)
    if brand:
        if isinstance(brand, list):
            conditions.append(sql.SQL("c.brand IN %s"))
            params.append(tuple(brand))
        else:
            conditions.append(sql.SQL("c.brand = %s"))
            params.append(brand)
    if start_date:
        conditions.append(sql.SQL("m.date >= %s"))
        params.append(start_date)
    if end_date:
        conditions.append(sql.SQL("m.date <= %s"))
        params.append(end_date)

    if conditions:
        where_clause = sql.SQL(" AND ").join(conditions)
        query = base_query + sql.SQL(" AND ") + where_clause
    else:
        query = base_query
    if table_name == 'seoptimer':

        query += sql.SQL(" ORDER BY m.date DESC LIMIT 1")    

    # Convert the query to a string
    query_str = query.as_string(conn)

    try:
        df = pd.read_sql_query(query_str, conn, params=params)
        return df
    except psycopg2.Error as e:
        print(f"Error fetching data: {e}")
        return None


def calculate_special_metrics(data, metric):
    if metric == 'CTR':
        return (data['clicks'].sum() / data['impressions'].sum())
    elif metric == 'Engagement %':
        return (data['Engagement'].sum() / data['Followers'].sum()) * 100
    


    elif metric == 'Net_sentiment_of_reviews':
        return (data['Net_sentiment_of_reviews'].mean()) 
    elif metric == 'net_sentiment_of_reviews':
        return (data['net_sentiment_of_reviews'].mean()) 
    elif metric == 'Average ratings':
        return (data['average_ratings'].mean()) 
    elif metric == 'Average ratings':
        return (data['average ratings'].mean()) 
    
    # elif metric == 'VTR':
    #     return (data['completed views'].sum() / data['impressions'].sum()) * 100
    elif metric == 'Transaction Rate':
        return (data['transactions'].sum() / data['clicks'].sum()) * 100
    elif metric == 'Cost per Transaction':
        return (data['cost'].sum() / data['transactions'].sum())
    # elif metric == 'AOV':
    #     return (data['revenue'].sum() / data['transactions'].sum())
    elif metric == 'Product Views per Session':
        return (data['product_views'].sum() / data['sessions'].sum())
    elif metric == 'Sessions to Product Views %':
        return (data['product_views'].sum() / data['sessions'].sum()) * 100
    elif metric == 'Product Views to Cart %':
        return (data['carts'].sum() / data['product_views'].sum()) * 100
    else:
        return None




import pandas as pd



def process_metric(master_table, conn, platforms, metrics, brands, analysis_type, start_date=None, end_date=None, category=None):
    results = []

    # Ensure metrics, platforms, and brands are lists
    if isinstance(metrics, str):
        metrics = [metrics]
    if isinstance(platforms, str):
        platforms = [platforms]
    if isinstance(brands, str):
        brands = [brands]

    # Iterate through each platform and metric
    for platform in platforms:
        for metric in metrics:
            # Get the metric information from the master_table
            metric_info = master_table[(master_table['platform'] == platform) & (master_table['metrics'] == metric)].iloc[0]
            percentile = 0.25 if metric_info['relationship'] == 1 else 0.75

            if analysis_type == 'Overall':
                # Fetch combined data without category filtering
                combined_data = fetch_data(conn, metric_info['table_name'], metric_info['db_metrics'], brand=brands, category=None, start_date=start_date, end_date=end_date)

                if combined_data is not None and not combined_data.empty:
                    raw_values = []
                    if metric in ['Average ratings','Net_sentiment_of_reviews','CTR', 'Engagement %', 'Transaction Rate', 'Cost per Transaction', 'Product Views per Session', 'Sessions to Product Views %', 'Product Views to Cart %']:
                        # Handle special metrics by calculating for each brand and aggregating
                        for brand in brands:
                            brand_data = combined_data[combined_data['brand'] == brand]
                            if not brand_data.empty:
                                result = calculate_special_metrics(brand_data, metric)
                                raw_values.append({'brand': brand, 'metric': metric, 'value': result})
                        overall_values = pd.DataFrame(raw_values)['value'].values
                        overall_result = calculate_percentile(overall_values, percentile)
                    else:
                        # For non-special metrics, aggregate and calculate percentiles directly
                        aggregated_data = combined_data.groupby(['brand'])[metric_info['db_metrics']].sum()
                        overall_values = aggregated_data.values
                        overall_result = calculate_percentile(overall_values, percentile)
                        raw_values = [{'brand': brand, 'metric': metric, 'value': value} for brand, value in aggregated_data.items()]

                    # Append results for "Overall" analysis type
                    results.append({
                        "platform": platform,
                        "metric": metric,
                        "definition": metric_info['definition'],
                        "brands": brands,
                        "analysis_type": list(combined_data['category'].unique()),  # List of unique categories
                        "category": "Overall",
                        "percentile": percentile,
                        "value": overall_result,
                        "actualValue": {r['brand']: r['value'] for r in raw_values},
                        "categories": list(combined_data['category'].unique())
                    })

            else:
                # Handling for specific categories when analysis_type is not "Overall"
                categories = analysis_type if isinstance(analysis_type, list) else [analysis_type]

                for cat in categories:
                    combined_data = fetch_data(conn, metric_info['table_name'], metric_info['db_metrics'], brand=brands, category=cat, start_date=start_date, end_date=end_date)

                    if combined_data is not None and not combined_data.empty:
                        raw_values = []
                        if metric in ['Average ratings','Net_sentiment_of_reviews','CTR', 'Engagement %', 'Transaction Rate', 'Cost per Transaction', 'Product Views per Session', 'Sessions to Product Views %', 'Product Views to Cart %']:
                            # Handle special metrics by calculating for each brand within the category
                            for brand in brands:
                                brand_data = combined_data[(combined_data['brand'] == brand) & (combined_data['category'] == cat)]
                                if not brand_data.empty:
                                    result = calculate_special_metrics(brand_data, metric)
                                    raw_values.append({
                                        'category': cat,
                                        'brand': brand,
                                        'metric': metric,
                                        'value': result
                                    })
                            if raw_values:
                                raw_values_df = pd.DataFrame(raw_values)
                                aggregated_data = raw_values_df.pivot_table(
                                    index=['category', 'brand'],
                                    values='value',
                                    aggfunc='sum'
                                )
                                result = calculate_percentile(aggregated_data['value'], percentile)
                                raw_values = aggregated_data.reset_index().to_dict(orient='records')
                        else:
                            # For non-special metrics, aggregate normally
                            aggregated_data = combined_data.pivot_table(
                                index=['category', 'brand'],
                                values=metric_info['db_metrics'],
                                aggfunc='sum'
                                
                            )
                            raw_values_df = aggregated_data.reset_index()
                            result = calculate_percentile(aggregated_data[metric_info['db_metrics']], percentile)
                            raw_values = raw_values_df.rename(columns={metric_info['db_metrics']: 'value'}).to_dict(orient='records')
                            # Ensure 'metric' field is added to each entry
                            for entry in raw_values:
                                entry['metric'] = metric

                        # Append results for specific categories
                        results.append({
                            "platform": platform,
                            "metric": metric,
                            "definition": metric_info['definition'],
                            "brands": brands,
                            "analysis_type": list(combined_data['category'].unique()),  # List of unique categories
                            "category": cat,
                            "percentile": percentile,
                            "value": result,
                            "actualValue": {r['brand']: r['value'] for r in raw_values},
                            "categories": [cat]
                        })

    return {"results": results}


class MetricRequest(BaseModel):
    platform: Union[str, List[str]]
    metrics: Union[str, List[str]]
    brand: Union[str, List[str]]  # Accept either a single brand or a list of brands
    analysis_type: Union[str, List[str]]  # Accept either a string or a list of strings
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    category: Optional[Union[str, List[str]]] = None  # Added category field

# @app.post("/process_metric")
# async def api_process_metric(request: MetricRequest):
#     conn = connect_to_db()
#     if conn is None:
#         raise HTTPException(status_code=500, detail="Unable to connect to the database")

#     master_table = load_master_table(conn)
#     if master_table is None:
#         conn.close()
#         raise HTTPException(status_code=500, detail="Unable to load master table")

#     try:
#         results = process_metric(master_table, conn, request.platform, request.metrics, request.brand, request.analysis_type, request.start_date, request.end_date, request.category)
#         return {"results": results}
#     finally:
#         conn.close()






from starlette.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, HTTPException

@app.middleware("http")
async def log_requests(request: Request, call_next):
    response = await call_next(request)
    logger.info(f"{request.method} {request.url} - {response.status_code}")
    return response

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.post("/process_metric/")

async def api_process_metric(request: MetricRequest):

    conn = connect_to_db()

    if conn is None:

        logger.error("Database connection failed.")

        raise HTTPException(status_code=500, detail="Unable to connect to the database")



    master_table = load_master_table(conn)

    if master_table is None:

        logger.error("Failed to load master table.")

        conn.close()

        raise HTTPException(status_code=500, detail="Unable to load master table")



    try:

        results = process_metric(master_table, conn, request.platform, request.metrics, request.brand, request.analysis_type, request.start_date, request.end_date, request.category)

        logger.info(f"Successfully processed metrics: {results}")

        return {"results": results}

    except HTTPException as e:

        logger.error(f"HTTPException occurred: {e.detail}", exc_info=True)

        raise

    except Exception as e:

        logger.error("An unexpected error occurred", exc_info=True)

        raise HTTPException(status_code=500, detail="An unexpected error occurred")

    finally:

        conn.close()
# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)




# /process_metric/



# {
#     "platform": "Amazon - Search Campaigns",
#     "metrics": ["Impressions","CTR","ACOS %"],
#     "brand": ["Pure Sense","Livon"],
#     "analysis_type": "Overall",
#     "start_date": "2023-01-01",
#     "end_date": "2024-12-31"
# }