
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Union, List
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from app.backendApi.auth_middleware import BearerTokenMiddleware

app = FastAPI()

# Add CORS middleware
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
origins = [
    "http://localhost",
    "http://localhost:3000",
    "https://example.com",
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(BearerTokenMiddleware)

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

def calculate_metric(df, metric, metric_dict):
    print(df.columns)
    spend_column = df['search___spends'] if 'search___spends' in df.columns else \
                         df['spends'] if 'spends' in df.columns else \
                         df['display___spends'] if 'display___spends' in df.columns else None
    add_to_column = df['add_to_cart'] if 'add_to_cart' in df.columns else \
                 df['add_to_basket'] if 'add_to_basket' in df.columns else None
    calculation_logic = metric_dict.get(metric, '').lower()
    if 'average' in calculation_logic:
        return to_native(df[metric].mean())
    elif 'sum' in calculation_logic:
        return to_native(df[metric].sum())
   
    else:
        # For any remaining complex calculations
        if metric.lower() == 'ctr':
            return to_native((df['clicks'].sum() / df['impressions'].sum()))
        elif metric.lower() == 'engagement %':
            return to_native((df['Engagement'].sum() / df['Followers'].sum()) * 100)
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
        elif metric.lower() == 'aov':
            if spend_column is None or 'transactions' not in df.columns:
                return None
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

def determine_metric_type(metric, metric_dict):
    calculation_logic = metric_dict.get(metric, '').lower()
   
    if 'average' in calculation_logic or 'latest' in calculation_logic:
        return 'average'
    elif 'sum' in calculation_logic or '/' in calculation_logic or '*' in calculation_logic:
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
    query = f"SELECT * FROM {SCHEMA_NAME}.master_table_platform_metrics_relationship"
    try:
        df = pd.read_sql_query(query, conn)
        print(f"Successfully loaded master table. Shape: {df.shape}")
        return df
    except psycopg2.Error as e:
        print(f"Error querying the master table: {e}")
        return None

def fetch_data(conn, table_name, db_metric, brand=None, category=None, start_date=None, end_date=None):
    base_query = sql.SQL("""
        SELECT m.*, c.category, c.sub_category,c.brand, m.date
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

    query_str = query.as_string(conn)

    try:
        df = pd.read_sql_query(query_str, conn, params=params)
        return df
    except psycopg2.Error as e:
        print(f"Error fetching data: {e}")
        return None

# def process_metric(master_table, conn, platforms, metrics, brands, analysis_type, start_date=None, end_date=None, category=None):
#     results = []

#     if isinstance(metrics, str):
#         metrics = [metrics]

#     if isinstance(platforms, str):
#         platforms = [platforms]

#     if isinstance(brands, str):
#         brands = [brands]

#     for platform in platforms:
#         for metric in metrics:
#             filtered_df = master_table[(master_table['platform'] == platform) & (master_table['metrics'] == metric)]

#             if filtered_df.empty:
#                 continue

#             metric_info = filtered_df.iloc[0]
#             metric_type = determine_metric_type(metric, METRIC_DICT)

#             for current_brand in brands:
#                 combined_data = pd.DataFrame()

#                 if analysis_type == 'Overall':
#                     data = fetch_data(conn, metric_info['table_name'], metric_info['db_metrics'], brand=current_brand, category=category, start_date=start_date, end_date=end_date)
#                     if data is not None and not data.empty:
#                         combined_data = pd.concat([combined_data, data])

#                     if not combined_data.empty:
#                         result = calculate_metric(combined_data, metric_info['db_metrics'], METRIC_DICT)
#                         result = None if pd.isna(result) else float(result)  # Ensure result is numeric
#                         results.append({
#                             "platform": platform,
#                             "metric": metric,
#                             "brand": current_brand,
#                             "result": result
#                         })
#                     else:
#                         results.append({
#                             "platform": platform,
#                             "metric": metric,
#                             "brand": current_brand,
#                             "result": None
#                         })

#                 else:
#                     categories = analysis_type if isinstance(analysis_type, list) else [analysis_type]

#                     for cat in categories:
#                         combined_data = pd.DataFrame()

#                         data = fetch_data(conn, metric_info['table_name'], metric_info['db_metrics'], brand=current_brand, category=cat, start_date=start_date, end_date=end_date)
#                         if data is not None and not data.empty:
#                             combined_data = pd.concat([combined_data, data])

#                         if not combined_data.empty:
#                             result = calculate_metric(combined_data, metric_info['db_metrics'], METRIC_DICT)
#                             result = None if pd.isna(result) else float(result)  # Ensure result is numeric
#                             results.append({
#                                 "platform": platform,
#                                 "metric": metric,
#                                 "brand": current_brand,
#                                 "result": result
#                             })
#                         else:
#                             results.append({
#                                 "platform": platform,
#                                 "metric": metric,
#                                 "brand": current_brand,
#                                 "result": None
#                             })

#     return results
def process_metric(master_table, conn, platforms, metrics, brands, analysis_type, start_date=None, end_date=None, category=None):
    results = []

    if isinstance(metrics, str):
        metrics = [metrics]

    if isinstance(platforms, str):
        platforms = [platforms]

    if isinstance(brands, str):
        brands = [brands]

    for platform in platforms:
        for metric in metrics:
            filtered_df = master_table[(master_table['platform'] == platform) & (master_table['metrics'] == metric)]

            if filtered_df.empty:
                continue

            metric_info = filtered_df.iloc[0]
            metric_type = determine_metric_type(metric, METRIC_DICT)

            for current_brand in brands:
                combined_data = pd.DataFrame()

                if analysis_type == 'Overall':
                    data = fetch_data(conn, metric_info['table_name'], metric_info['db_metrics'], brand=current_brand, category=category, start_date=start_date, end_date=end_date)
                    if data is not None and not data.empty:
                        combined_data = pd.concat([combined_data, data])

                    if not combined_data.empty:
                        result = calculate_metric(combined_data, metric_info['db_metrics'], METRIC_DICT)
                        result = None if pd.isna(result) else float(result)  # Ensure result is numeric
                        results.append({
                            "platform": platform,
                            "metric": metric,
                            "brand": current_brand,
                            "section": metric_info.get('section', 'default_section'),  # Adjust to include the section
                            "result": result,
                            "category": combined_data['category'].iloc[0] if 'category' in combined_data else None,
                            "sub_category": combined_data['sub_category'].iloc[0] if 'sub_category' in combined_data else None
                        })
                    else:
                        results.append({
                            "platform": platform,
                            "metric": metric,
                            "brand": current_brand,
                            "section": metric_info.get('section', 'default_section'),  # Adjust to include the section
                            "result": None,
                            "category": None,
                            "sub_category": None
                        })

                else:
                    categories = analysis_type if isinstance(analysis_type, list) else [analysis_type]

                    for cat in categories:
                        combined_data = pd.DataFrame()

                        data = fetch_data(conn, metric_info['table_name'], metric_info['db_metrics'], brand=current_brand, category=cat, start_date=start_date, end_date=end_date)
                        if data is not None and not data.empty:
                            combined_data = pd.concat([combined_data, data])

                        if not combined_data.empty:
                            result = calculate_metric(combined_data, metric_info['db_metrics'], METRIC_DICT)
                            result = None if pd.isna(result) else float(result)  # Ensure result is numeric
                            results.append({
                                "platform": platform,
                                "metric": metric,
                                "brand": current_brand,
                                "section": metric_info.get('section', 'default_section'),  # Adjust to include the section
                                "result": result
                            })
                        else:
                            results.append({
                                "platform": platform,
                                "metric": metric,
                                "brand": current_brand,
                                "section": metric_info.get('section', 'default_section'),  # Adjust to include the section
                                "result": None
                            })

    return results



    


class MetricRequest(BaseModel):
    platform: Union[str, List[str]]
    metrics: Union[str, List[str]]
    brand: Union[str, List[str]]
    analysis_type: Union[str, List[str]]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    category: Optional[Union[str, List[str]]] = None

@app.post("/analytics_metric/")
async def api_process_metric(request: MetricRequest):
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")

    master_table = load_master_table(conn)
    if master_table is None:
        conn.close()
        raise HTTPException(status_code=500, detail="Unable to load master table")

    try:
        results = process_metric(master_table, conn, request.platform, request.metrics, request.brand, request.analysis_type, request.start_date, request.end_date, request.category)
        return {"results": results}
    finally:
        conn.close()



# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8003)




# /analytics_metric/

# {
#     "platform": ["Amazon - Search Campaigns","Amazon - Display Campaigns"],
#     "metrics": "Impressions",
#     "brand": ["Pure Sense"],
#     "analysis_type": "Overall",
#     "start_date": "2024-01-01",
#     "end_date": "2024-12-31"
# }


#LATEST
# {
#     "platform": ["Amazon - Search Campaigns","Amazon - Display Campaigns"],
#     "metrics": ["Impressions","CTR"],
#     "brand": "Pure Sense",
#     "analysis_type": "Overall",
#     "start_date": "2024-01-01",
#     "end_date": "2024-12-31"
# }

