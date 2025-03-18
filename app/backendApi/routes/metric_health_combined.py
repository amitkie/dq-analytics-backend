# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# import pandas as pd
# import numpy as np
# from typing import List, Optional, Union
# from fastapi.middleware.cors import CORSMiddleware
# from sqlalchemy import create_engine, text
# from sqlalchemy.exc import SQLAlchemyError
# import urllib.parse

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
# DB_USER = 'KIESQUAREDE'
# DB_PASS = urllib.parse.quote_plus('KIESQUARE123')  # URL encode the password
# DB_HOST = 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com'
# DB_PORT = '5434'
# DB_NAME = 'KIESQUAREDE'

# # Create SQLAlchemy engine with proper connection string
# DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# # Schema names
# PUBLIC_SCHEMA = 'public'
# DQ_SCHEMA = 'dq'

# # Pydantic models
# class RequestPayload(BaseModel):
#     project_ids: List[int]

# class MetricRequest(BaseModel):
#     platform: str
#     metrics: List[str]
#     brand: List[str]
#     analysis_type: str = "Overall"
#     start_date: Optional[str] = None
#     end_date: Optional[str] = None
#     category: Optional[Union[str, List[str]]] = None

# def to_native(value):
#     """Convert numpy types to Python native types"""
#     if isinstance(value, np.generic):
#         return value.item()
#     return value

# def execute_query(query: str, params: dict = None) -> pd.DataFrame:
#     """Execute SQL query and return pandas DataFrame"""
#     try:
#         with engine.connect() as connection:
#             if params:
#                 result = connection.execute(text(query), params)
#             else:
#                 result = connection.execute(text(query))
            
#             df = pd.DataFrame(result.fetchall())
#             if not df.empty:
#                 df.columns = result.keys()
#             return df
#     except Exception as e:
#         print(f"Error executing query: {e}")
#         print(f"Query: {query}")
#         print(f"Params: {params}")
#         raise HTTPException(status_code=500, detail=str(e))

# def load_master_table(project_id: int = None) -> pd.DataFrame:
#     """Load master table based on project_id"""
#     try:
#         if project_id is not None:
#             query = f"""
#             SELECT * FROM {PUBLIC_SCHEMA}.normalisedvalue
#             WHERE project_id = :project_id
#             """
#             return execute_query(query, {'project_id': project_id})
#         else:
#             query = f"SELECT * FROM {DQ_SCHEMA}.master_table_platform_metrics_relationship"
#             return execute_query(query)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error querying master table: {str(e)}")

# def load_user_projects(project_id: int) -> pd.DataFrame:
#     """Load user projects data"""
#     try:
#         query = f"""
#         SELECT id, project_name FROM {PUBLIC_SCHEMA}."userProjects"
#         WHERE id = :project_id
#         """
#         return execute_query(query, {'project_id': project_id})
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error querying user projects: {str(e)}")

# def join_table() -> pd.DataFrame:
#     """Join tables for comprehensive data"""
#     try:
#         query = f"""
#         SELECT nv.*, b.name AS brand_name, c.name AS category_name
#         FROM {PUBLIC_SCHEMA}.normalisedvalue nv
#         LEFT JOIN {PUBLIC_SCHEMA}.brands b ON nv.brandName = b.name
#         LEFT JOIN {PUBLIC_SCHEMA}.categories c ON b.category_id = c.id
#         """
#         return execute_query(query)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error querying joined table: {str(e)}")

# def calculate_final_score(group: pd.DataFrame) -> float:
#     """Calculate final score for a group"""
#     weight_sum = group['weights'].sum()
#     if weight_sum == 0:
#         return np.nan
#     score = (group['weights'] * group['normalized']).sum() / weight_sum
#     return score

# def compute_final_scores(df: pd.DataFrame) -> pd.DataFrame:
#     """Compute final scores for all metrics"""
#     overall_results = pd.DataFrame()
#     additional_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']

#     if not df.empty:
#         sum_of_product = df.groupby('brandname').apply(calculate_final_score)
        
#         overall_results = pd.DataFrame({
#             'Brand_Name': sum_of_product.index,
#             'Overall_Final_Score': sum_of_product.values
#         })

#         sectionwise = df.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
#         for section in sectionwise.columns:
#             overall_results[section] = sectionwise[section].values

#         for col in additional_columns:
#             if col not in overall_results.columns:
#                 overall_results[col] = 0

#         overall_results['id'] = range(1, len(overall_results) + 1)
    
#     return overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]

# def calculate_metric(df, metric, metric_dict):
#     """Calculate specific metrics based on type"""
#     print(f"Calculating metric: {metric}")
#     print(f"Available columns: {df.columns}")
    
#     spend_column = df['search___spends'] if 'search___spends' in df.columns else \
#                   df['spends'] if 'spends' in df.columns else \
#                   df['display___spends'] if 'display___spends' in df.columns else None
                  
#     add_to_column = df['add_to_cart'] if 'add_to_cart' in df.columns else \
#                     df['add_to_basket'] if 'add_to_basket' in df.columns else None

#     calculation_logic = metric_dict.get(metric, '').lower()
    
#     if 'average' in calculation_logic:
#         return to_native(df[metric].mean())
#     elif 'sum' in calculation_logic:
#         return to_native(df[metric].sum())
    
#     metric_lower = metric.lower()
#     try:
#         if metric_lower == 'ctr':
#             return to_native((df['clicks'].sum() / df['impressions'].sum()))
#         elif metric_lower == 'engagement %':
#             return to_native((df['Engagement'].sum() / df['Followers'].sum() * 100))
#         elif metric_lower == 'transaction rate':
#             return to_native((df['transactions'].sum() / df['clicks'].sum() * 100))
#         elif metric_lower == 'product views per session':
#             return to_native(df['Product views'].sum() / df['Session'].sum())
#         elif metric_lower == 'sessions to product views %':
#             return to_native((df['Product Views'].sum() / df['Sessions'].sum() * 100))
#         elif metric_lower == 'product views to cart %':
#             return to_native((df['Add to cart'].sum() / df['Product Views'].sum() * 100))
#         elif metric_lower == 'cart to checkout %':
#             return to_native((df['Checkout'].sum() / df['Add to cart'].sum() * 100))
#         elif metric_lower == 'check out to transaction %':
#             return to_native((df['Orders'].sum() / df['checkout initiated'].sum() * 100))
#         elif metric_lower == 'overall conversion %':
#             return to_native((df['Transactions'].sum() / df['Sessions'].sum() * 100))
#         elif metric_lower == 'repeat rate %':
#             return to_native((1 - (df['customers acquired'].sum() / df['transactions'].sum())) * 100)
#         elif metric_lower == 'transaction_rate':
#             if 'transactions' in df.columns and 'clicks' in df.columns:
#                 return to_native(df['transactions'].sum() / df['clicks'].sum())
#         elif metric_lower == 'cost_per_transaction':
#             if 'transactions' in df.columns and 'spends' in df.columns:
#                 return to_native(df['spends'].sum() / df['transactions'].sum())
#         elif metric_lower == 'order_conversion_rate':
#             if 'purchases' in df.columns and 'clicks' in df.columns:
#                 return to_native(df['purchases'].sum() / df['clicks'].sum())
#         elif metric_lower == 'cpm':
#             if spend_column is not None and 'impressions' in df.columns:
#                 return to_native((spend_column.sum() / df['impressions'].sum()) * 1000)
#         elif metric_lower == 'cpc':
#             if spend_column is not None and 'clicks' in df.columns:
#                 return to_native(spend_column.sum() / df['clicks'].sum())
#         elif metric_lower == 'acos__':
#             if spend_column is not None and 'sales_value' in df.columns:
#                 return to_native(spend_column.sum() / df['sales_value'].sum())
#     except (KeyError, ZeroDivisionError, Exception) as e:
#         print(f"Error calculating {metric}: {str(e)}")
    
#     return to_native(df[metric].sum() if metric in df.columns else np.nan)

# def fetch_data(table_name, db_metric, brand=None, category=None, start_date=None, end_date=None) -> pd.DataFrame:
#     """Fetch data with proper SQL parameter binding"""
#     params = {}
#     conditions = []
#     base_query = f"""
#         SELECT m.*, c.category, c.brand, m.date
#         FROM {DQ_SCHEMA}.{table_name} m
#         JOIN {DQ_SCHEMA}.master_table_category_brand c ON m.unique_id = c.unique_id
#         WHERE 1=1
#     """

#     if category:
#         if isinstance(category, list):
#             conditions.append("c.category = ANY(:categories)")
#             params['categories'] = category
#         else:
#             conditions.append("c.category = :category")
#             params['category'] = category

#     if brand:
#         if isinstance(brand, list):
#             conditions.append("c.brand = ANY(:brands)")
#             params['brands'] = brand
#         else:
#             conditions.append("c.brand = :brand")
#             params['brand'] = brand

#     if start_date:
#         conditions.append("m.date >= :start_date")
#         params['start_date'] = start_date

#     if end_date:
#         conditions.append("m.date <= :end_date")
#         params['end_date'] = end_date

#     if conditions:
#         base_query += " AND " + " AND ".join(conditions)

#     if table_name == 'seoptimer':
#         base_query += " ORDER BY m.date DESC LIMIT 1"

#     try:
#         return execute_query(base_query, params)
#     except Exception as e:
#         print(f"Error in fetch_data: {e}")
#         return pd.DataFrame()

# def process_metric(master_table, platforms, metrics, brands, analysis_type, start_date=None, end_date=None, category=None):
#     """Process metrics for given parameters"""
#     results = []
    
#     if isinstance(metrics, str):
#         metrics = [metrics]
#     if isinstance(platforms, str):
#         platforms = [platforms]
#     if isinstance(brands, str):
#         brands = [brands]

#     for platform in platforms:
#         for metric in metrics:
#             filtered_df = master_table[
#                 (master_table['platform'] == platform) & 
#                 (master_table['metrics'] == metric)
#             ]

#             if filtered_df.empty:
#                 continue

#             metric_info = filtered_df.iloc[0]

#             for current_brand in brands:
#                 combined_data = pd.DataFrame()

#                 if analysis_type == 'Overall':
#                     data = fetch_data(
#                         metric_info['table_name'],
#                         metric_info['db_metrics'],
#                         brand=current_brand,
#                         category=category,
#                         start_date=start_date,
#                         end_date=end_date
#                     )

#                     if data is not None and not data.empty:
#                         combined_data = pd.concat([combined_data, data])

#                     result = None
#                     if not combined_data.empty:
#                         result = calculate_metric(combined_data, metric_info['db_metrics'], {})
#                         result = None if pd.isna(result) else float(result)

#                     results.append({
#                         "platform": platform,
#                         "metric": metric,
#                         "brand": current_brand,
#                         "section": metric_info.get('section', 'N/A'),
#                         "result": result
#                     })

#     return results

# @app.post("/health_card/")
# async def health_card(request: MetricRequest):
#     """Endpoint to get health card metrics"""
#     try:
#         master_table = load_master_table()
#         if master_table.empty:
#             raise HTTPException(status_code=500, detail="Unable to load master table")

#         results = process_metric(
#             master_table,
#             [request.platform],
#             request.metrics,
#             request.brand,
#             request.analysis_type,
#             request.start_date,
#             request.end_date,
#             request.category
#         )

#         formatted_results = {}
#         for result in results:
#             section = result.get("section", "N/A")
#             platform = result["platform"]
#             metric = result["metric"]
#             result_value = result["result"]

#             if section not in formatted_results:
#                 formatted_results[section] = {}
#             if platform not in formatted_results[section]:
#                 formatted_results[section][platform] = {}

#             formatted_results[section][platform][metric] = (
#                 round(result_value * 100, 3) if metric.lower() in [" "] else result_value
#             )

#         return {"results": formatted_results}
#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail=f"Error processing health card: {str(e)}"
#         )

# @app.post("/get_multi_data/")
# async def get_multi_data(payload: RequestPayload):
#     """Endpoint to get multiple data points based on project IDs"""
#     try:
#         df = join_table()
#         all_final_scores = []

#         for project_id in payload.project_ids:
#             project_df = load_master_table(project_id)
#             project_name_df = load_user_projects(project_id)

#             if not project_df.empty and not project_name_df.empty:
#                 project_df = pd.merge(
#                     project_df, 
#                     project_name_df[['id', 'project_name']], 
#                     left_on='project_id',
#                     right_on='id',
#                     how='left'
#                 )

#                 final_scores_df = compute_final_scores(project_df)

#                 brands_list = []
#                 for _, row in final_scores_df.iterrows():
#                     brand_data = {
#                         "brand_name": row['Brand_Name'],
#                         "dq_score": {
#                             "Overall_Final_Score": row['Overall_Final_Score'],
#                             "Marketplace": row.get('Marketplace', 0),
#                             "Socialwatch": row.get('Socialwatch', 0),
#                             "Digital Spends": row.get('Digital Spends', 0),
#                             "Organic Performance": row.get('Organic Performance', 0),
#                         }
#                     }
#                     brands_list.append(brand_data)

#                 all_final_scores.append({
#                     "project_id": project_id,
#                     "project_name": project_name_df['project_name'].iloc[0],
#                     "brands": brands_list
#                 })

#         return {"data": all_final_scores if all_final_scores else []}
#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail=f"Error processing data: {str(e)}"
#         )

# # Health check endpoint
# @app.get("/health")
# async def health_check():
#     """Health check endpoint to verify service status"""
#     try:
#         # Test database connection
#         with engine.connect() as connection:
#             connection.execute(text("SELECT 1"))
#         return {
#             "status": "healthy",
#             "timestamp": pd.Timestamp.now().isoformat(),
#             "database": "connected"
#         }
#     except Exception as e:
#         raise HTTPException(
#             status_code=503,
#             detail=f"Service unhealthy: {str(e)}"
#         )

# # Additional error handling middleware
# @app.exception_handler(Exception)
# async def global_exception_handler(request, exc):
#     """Global exception handler for unhandled errors"""
#     error_message = str(exc)
#     return {
#         "error": "Internal Server Error",
#         "detail": error_message,
#         "path": request.url.path
#     }

# # Startup and shutdown events
# @app.on_event("startup")
# async def startup_event():
#     """Execute startup tasks"""
#     print("Starting up the application...")
#     try:
#         # Test database connection on startup
#         with engine.connect() as connection:
#             connection.execute(text("SELECT 1"))
#         print("Database connection successful")
#     except Exception as e:
#         print(f"Error connecting to database: {e}")
#         raise e

# @app.on_event("shutdown")
# async def shutdown_event():
#     """Execute shutdown tasks"""
#     print("Shutting down the application...")
#     engine.dispose()

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(
#         app,
#         host="0.0.0.0",
#         port=8000,
#         log_level="info",
#         reload=True  # Enable auto-reload during development
#     )






import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Union, List
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import urllib.parse
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
DB_USER = 'KIESQUAREDE'
DB_PASS = urllib.parse.quote_plus('KIESQUARE123')  # URL encode the password
DB_HOST = 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com'
DB_PORT = '5434'
DB_NAME = 'KIESQUAREDE'
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Schema name
SCHEMA_NAME = 'dq'






SCHEMANAME = 'public'

# Pydantic model for request payload
class RequestPayload(BaseModel):
    project_ids: List[int]  # Accepting multiple project IDs



# Function to join tables and get data
def join_table(conn) -> pd.DataFrame:
    query = f"""
    SELECT nv.*, b.name AS brand_name, c.name AS category_name
    FROM {SCHEMANAME}.normalisedvalue nv
    LEFT JOIN {SCHEMANAME}.brands b ON nv.brandName = b.name
    LEFT JOIN {SCHEMANAME}.categories c ON b.category_id = c.id
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

# Function to load data with joins
def load_master_table_1(conn, project_id: int) -> pd.DataFrame:
    query = f"""
    SELECT * FROM {SCHEMANAME}.normalisedvalue
    WHERE project_id = {project_id}
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

# Function to calculate final score for each group
def calculate_final_score(group: pd.DataFrame) -> float:
    weight_sum = group['weights'].sum()
    if weight_sum == 0:
        return np.nan
    score = (group['weights'] * group['normalized']).sum() / weight_sum
    return score

# Updated function to compute final scores
def compute_final_scores(df: pd.DataFrame) -> pd.DataFrame:
    # Initialize DataFrames to hold the resulte
    overall_results = pd.DataFrame()
    additional_columns = ['Marketplace','Socialwatch', 'Digital Spends', 'Organic Performance']

    if not df.empty:
        # Calculate overall final scores for each brand
        sum_of_product = df.groupby('brandname').apply(calculate_final_score)
        
        # Format the output for overall scores
        overall_results = pd.DataFrame({
            'Brand_Name': sum_of_product.index,
            'Overall_Final_Score': sum_of_product.values
        })

        # Calculate section-wise final scores for overall type
        sectionwise = df.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
        for section in sectionwise.columns:
            overall_results[section] = sectionwise[section].values
        print(sectionwise)
        # Ensure additional columns exist
        for col in additional_columns:
            if col not in overall_results.columns:
                overall_results[col] = 0

        # Unique id for each row
        overall_results['id'] = range(1, len(overall_results) + 1)

    # Select only the required columns
    final_result = overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]
    print(final_result)
    return final_result








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
        
        # elif metric.lower() == 'ACOS %':
        #     return to_native((df['Spends'].sum() / df['Sales'].sum()) * 100)
        
        elif metric.lower() == 'Amazon Best seller rank':
            return to_native(df['amazon_best_seller_rank'].mean())
        
        elif metric.lower() == 'Net sentiment of reviews':
            return to_native(df['net_sentiment_of_reviews'].mean())
        elif metric.lower() == 'Net sentiment':
            return to_native(df['net_sentiment'].mean())   
        
        elif metric.lower() == 'Mobile page speed insights score':
            return to_native(df['mobile_page_speed_insights_score'].mean())  
        elif metric.lower() == 'Web page speed insights score':
            return to_native(df['web_page_speed_insights_score'].mean())
        elif metric.lower() == 'Largest contentful paint (LCP) - seconds':
            return to_native(df['largest_contentful_paint__lcp____seconds'].mean()) 
        
        elif metric.lower() == 'Organic rank':
            return to_native(df['organic_rank'].mean()) 
        
        elif metric.lower() == 'Availability%':
            return to_native(df['availability_'].mean())
        
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
def load_metric_list(file_path=r'C:\Users\Administrator\Desktop\Git\dq-analytics-backend\app\backendApi\Metric_List.csv'):
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

    query_str = query.as_string(conn)

    try:
        df = pd.read_sql_query(query_str, conn, params=params)
        return df
    except psycopg2.Error as e:
        print(f"Error fetching data: {e}")
        return None


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
                            "section": metric_info.get('section', 'N/A'),  # Include section info
                            "result": result
                        })
                    else:
                        results.append({
                            "platform": platform,
                            "metric": metric,
                            "brand": current_brand,
                            "section": metric_info.get('section', 'N/A'),  # Include section info
                            "result": None
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
                                "section": metric_info.get('section', 'N/A'),  # Include section info
                                "result": result
                            })
                        else:
                            results.append({
                                "platform": platform,
                                "metric": metric,
                                "brand": current_brand,
                                "section": metric_info.get('section', 'N/A'),  # Include section info
                                "result": None
                            })

    return results


class MetricRequest(BaseModel):
    platform: Union[str, List[str]] = ["Amazon","Big Basket Campaigns","Blinkit Campaigns","Gadwords","Facebook","Amazon - Search Campaigns","Flipkart PLA Campaigns","Page Speed Insights" ,"Instagram","Twitter","SEO","seo__seo_optimer_","Amazon - Display Campaigns","DV360","Myntraa Campaigns","Flipkart PCA Campaigns","Nykaa Campaigns","Google Analytics","SEOptimer"]
    metrics: Union[str, List[str]] = ["Purchases","Checkout","Clicks","Time to interact (TTI)- seconds","CAC","Check out to Transaction %","Usability (SEO optimer)","Frequency","Click to cart %","CPC","Availability%","Speed Index - seconds","Cumulative layout shift (CLS)","Reviews","Repeat rate %","Avg. Session Duration (mins)","Search visibility share (Paid)","First input delay (FID) - milli seconds","Transaction rate","Display – Spends","ACOS %","DPV","Click to new purchase","Mobile page speed insights score","AOV","Transactions","ATC","Cost per transaction","Add to cart","Average ratings","Total blocking time (TBT) - milli seconds","Cost per order (new)","Overall conversion %","Mentions","Performance (SEO optimer)","Order Conversion Rate","Cart to checkout","ACOS %(to be calculated )","Add to Basket","Engagement %","% of new purchase rate","Search – Spends","Product Views","SEO (SEO optimer)","ATCR","Sessions","Organic rank","Product views per session","Pages per sessions","CTR","VTR","Impressions","Largest contentful paint (LCP) - seconds","Net sentiment","unit sold","Web page speed insights score","Search - Spends","Reach","DPVR","Search visibility share (Organic)","Unique Visitors","Order","Cart to Checkout %","Load Time (seconds)","CPM","Spends","Sales value","Cost per order","Amazon Best seller rank","Engagement","Net sentiment of reviews","First contentful paint (FCP) - seconds","Social (SEO optimer)","Search - home page banners","Sessions to product views %","Product views to cart %","Cost per new customer (CAC)"]
    brand: Union[str, List[str]]
    #analysis_type: Union[str, List[str]] =["Overall"]
    analysis_type: Union[str, List[str]] = "Overall"

    start_date: Optional[str] = None
    end_date: Optional[str] = None
    category: Optional[Union[str, List[str]]] = None

# @app.post("/health_card")
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

#         # Transform results into the desired format
#         formatted_results = {}
#         for result in results:
#             section = result.get("section", "N/A")
#             platform = result["platform"]
#             metric = result["metric"]
#             result_value = result["result"]

#             if section not in formatted_results:
#                 formatted_results[section] = {}
#             if platform not in formatted_results[section]:
#                 formatted_results[section][platform] = {}

#             formatted_results[section][platform][metric] = round(result_value * 100, 3) if metric.lower() in ["vtr"] else result_value

#         return {"results": formatted_results}
#     finally:
#         conn.close()
@app.post("/health_card/")
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

        # Transform results into the desired format
        formatted_results = {}
        for result in results:
            section = result.get("section", "N/A")
            platform = result["platform"]
            metric = result["metric"]
            result_value = result["result"]

            if section not in formatted_results:
                formatted_results[section] = {}
            if platform not in formatted_results[section]:
                formatted_results[section][platform] = {}

            formatted_results[section][platform][metric] = round(result_value * 100, 3) if metric.lower() in [" "] else result_value

        return {"results": formatted_results}
    finally:
        conn.close()

@app.post("/get_multi_data")
def get_data(payload: RequestPayload):
    conn = connect_to_db()
    try:
        df = join_table(conn)
        all_final_scores = []

        for project_id in payload.project_ids:
            project_df = load_master_table_1(conn, project_id)
            if project_df is not None and not project_df.empty:
                final_scores_df = compute_final_scores(project_df)

                # Transforming the final scores into the desired response structure
                brands_list = []
                for _, row in final_scores_df.iterrows():
                    brand_data = {
                        "brand_name": row['Brand_Name'],
                        "dq_score": {
                            "Overall_Final_Score": row['Overall_Final_Score'],
                            "Marketplace": row.get('Marketplace', 0),
                            "Socialwatch": row.get('Socialwatch', 0),
                            "Digital Spends": row.get('Digital Spends', 0),
                            "Organic Performance": row.get('Organic Performance', 0),
                        }
                    }
                    brands_list.append(brand_data)

                all_final_scores.append({
                    "project_id": project_id,
                    "brands": brands_list
                })

        if all_final_scores:
            return {"data": all_final_scores}
        else:
            return {"data": []}
    finally:
        conn.close()


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)




# # /health_card/

# {
#     "brand": ["Pure Sense"],
#     "analysis_type": "Overall",
#     "start_date": "2024-01-01",
#     "end_date": "2024-12-31"
# }
# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint to verify service status"""
    try:
        # Test database connection
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "timestamp": pd.Timestamp.now().isoformat(),
            "database": "connected"
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Service unhealthy: {str(e)}"
        )

# Additional error handling middleware
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors"""
    error_message = str(exc)
    return {
        "error": "Internal Server Error",
        "detail": error_message,
        "path": request.url.path
    }

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Execute startup tasks"""
    print("Starting up the application...")
    try:
        # Test database connection on startup
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        print("Database connection successful")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise e

@app.on_event("shutdown")
async def shutdown_event():
    """Execute shutdown tasks"""
    print("Shutting down the application...")
    engine.dispose()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=True  # Enable auto-reload during development
    )