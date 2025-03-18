from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import psycopg2
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import requests
from app.backendApi.auth_middleware import BearerTokenMiddleware

# Database connection parameters
DB_PARAMS = {
    'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
    'port': '5434',
    'dbname': 'KIESQUAREDE',
    'user': 'KIESQUAREDE',
    'password': 'KIESQUARE123'
}

# Schema name
SCHEMA_NAME = 'public'

app = FastAPI()
origins = [
    "http://localhost",
    "http://localhost:3000",
    "https://m594bmgj-7033.inc1.devtunnels.ms",  # Add your forwarded URL
    "https://*.devtunnels.ms",  # Allow all devtunnels subdomains
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

class RequestPayload(BaseModel):
    project_ids: List[int]

def connect_to_db():
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        return None

def fetch_table(conn, project_ids: List[int])-> pd.DataFrame:
    if not project_ids:
        raise HTTPException(status_code=400, detail="project_ids must be provided")
    
    project_ids_str = ', '.join(map(str, project_ids))
    query = f'SELECT * FROM {SCHEMA_NAME}."superThemeMetricGroups" WHERE project_id IN ({project_ids_str})'
    try:
        df_metric = pd.read_sql_query(query, conn,)
        
        # Process metric_ids
        metrics_result = []
        for _, row in df_metric.iterrows():
            for metric in row['metric_ids']:
                metrics_result.append({
                    'id': row['id'],
                    'name': row['name'],
                    'project_id': row['project_id'],
                    'metric_ids': str(metric)
                })
        
        # Process metric_group_ids
        metric_groups_result = []
        for _, row in df_metric.iterrows():
                for metric_group in row['metric_group_ids']:
                    metric_groups_result.append({
                        'id': row['id'],
                        'name': row['name'],
                        'project_id': row['project_id'],
                        'metric_group_ids': str(metric_group)
                    })

        metrics_result = pd.DataFrame(metrics_result)
        print(metrics_result)
        metric_groups_result = pd.DataFrame(metric_groups_result)
        print(metric_groups_result)
        return pd.DataFrame(metrics_result), pd.DataFrame(metric_groups_result)

    except psycopg2.Error as e:
        print(f"Error querying the benchmark table: {e}")
        return None, None
def fetch_normalized_data(conn, project_ids: List[int]) -> pd.DataFrame:
    if not project_ids:
        raise HTTPException(status_code=400, detail="project_ids must be provided")
    
    project_ids_str = ', '.join(map(str, project_ids))
    query = f'SELECT * FROM {SCHEMA_NAME}."normalisedvalue" WHERE project_id IN ({project_ids_str})'
    
    try:
        return pd.read_sql_query(query, conn)
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the normalized value table: {e}")
def fetch_group_normalised(conn, project_ids: List[int]) -> pd.DataFrame:
    if not project_ids:
        raise HTTPException(status_code=400, detail="project_ids must be provided")
    
    project_ids_str = ', '.join(map(str, project_ids))
    query = f'SELECT * FROM {SCHEMA_NAME}."groupnormalisedvalue" WHERE project_id IN ({project_ids_str})'
    try:
        return pd.read_sql_query(query, conn)
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the normalized value table: {e}")
def join_metric_and_normalized_data(metrics_result: pd.DataFrame, normalized_data: pd.DataFrame) -> pd.DataFrame:
    metrics_result['metric_ids'] = metrics_result['metric_ids'].astype(str)
    normalized_data['metricid'] = normalized_data['metricid'].astype(str)

    normalized_data = normalized_data.drop_duplicates(subset=['project_id', 'metricid', 'categoryid', 'sectionid', 'platformid', 'weights', 'benchmarkvalue', 'actualvalue', 'normalized', 'brandname', 'sectionname', 'categoryname', 'metricname', 'platformname', 'type'])

    joined_data = metrics_result.merge(
        normalized_data,
        left_on=['metric_ids'],
        right_on=['metricid'],
        how='inner'
    )
    print("joined_data")
    print(joined_data)
    if joined_data.empty:
        raise HTTPException(status_code=404, detail="No matching records found after join")

    # Step 1: Calculate weight sum for each brand
    weight_sum_df = joined_data.groupby(['brandname','name','id','project_id_x','metric_ids','metricname'])['weights'].sum().reset_index(name='weight_sum')
    print("weight_sum_df")
    print(weight_sum_df)
    # Step 2: Calculate new normalized values
    joined_data['new_norm_value'] = joined_data['normalized'] * joined_data['weights']
    print(joined_data)
    # Step 3: Calculate sum of normalized values
    norm_sum_df = joined_data.groupby(['brandname','name','id','project_id_x','metric_ids','metricname'])['new_norm_value'].sum().reset_index(name='normalized_value')
    print("norm_sum_df")
    print(norm_sum_df)
    # Step 4: Combine results
    theme_normalized_value = norm_sum_df.merge(
        weight_sum_df,
        left_on=["brandname","name","id","project_id_x","metric_ids","metricname"],
        right_on=["brandname","name","id","project_id_x","metric_ids","metricname"],
        how='inner'
    ) 
    #norm_sum_df.merge(weight_sum_df, on='brandname')
    theme_normalized_value['normalized_per_weight'] = theme_normalized_value['normalized_value'] / theme_normalized_value['weight_sum']
    
    # with open("group_normalised_value.pkl","wb") as f:
    #     pickle.dump(group_normalized_value,f)
    theme_normalized_data = theme_normalized_value.rename(columns={'project_id_x': 'project_id'})
    print("group")   
    print(theme_normalized_data)
    return theme_normalized_data[['project_id','id','metricname','metric_ids','name','brandname','weight_sum','normalized_per_weight']]
def fetch_group_normalised(conn, project_ids: List[int]) -> pd.DataFrame:
    if not project_ids:
        raise HTTPException(status_code=400, detail="project_ids must be provided")
    
    project_ids_str = ', '.join(map(str, project_ids))
    query = f'SELECT * FROM {SCHEMA_NAME}."groupnormalisedvalue" WHERE project_id IN ({project_ids_str})'
    
    try:
        group_normalised_df = pd.read_sql_query(query, conn)
        
        return group_normalised_df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the group normalized value table: {e}")

def final_data(metric_groups_result: pd.DataFrame, group_normalised_data: pd.DataFrame, theme_normalized_data: pd.DataFrame) -> pd.DataFrame:
    # Ensure proper type conversion
    metric_groups_result['metric_group_ids'] = metric_groups_result['metric_group_ids'].astype(str)
    group_normalised_data['id'] = group_normalised_data['id'].astype(str)
    # Perform the merge with group normalized data
    metric_groups_data = metric_groups_result.merge(
        group_normalised_data,
        left_on=['metric_group_ids'],
        right_on=['id'],
        how='inner'
    )
    print(metric_groups_data)
    # Rename columns as required
    metric_groups_data = metric_groups_data[["id_x","name_x", "name_y","project_id_x", "metric_group_ids", "brandname", "weight_sum", "normalized_per_weight"]]
    metric_groups_data = metric_groups_data.rename(columns={'name_x': 'name','name_y':'metricname', 'project_id_x': 'project_id', 'metric_group_ids': 'metric_group_ids','id_x':"id"})
    print("metric name check")
    print(metric_groups_data)
    # Check if metric_groups_data is empty
    if metric_groups_data.empty:
        raise HTTPException(status_code=404, detail="No matching records found in the metric groups data")

    # Ensure the relevant columns are numeric
    metric_groups_data['weight_sum'] = pd.to_numeric(metric_groups_data['weight_sum'], errors='coerce')
    metric_groups_data['normalized_per_weight'] = pd.to_numeric(metric_groups_data['normalized_per_weight'], errors='coerce')
    # Remove 'normalized_value' from theme_normalized_data and ensure numeric types
    theme_normalized = theme_normalized_data[['id','metric_ids','metricname','name', 'project_id', 'brandname', 'weight_sum', 'normalized_per_weight']]
    print("theme")
    print(theme_normalized_data)
    theme_normalized['weight_sum'] = pd.to_numeric(theme_normalized['weight_sum'], errors='coerce')
    theme_normalized['normalized_per_weight'] = pd.to_numeric(theme_normalized['normalized_per_weight'], errors='coerce')
    
    # Concatenate metric_groups_data and theme_normalized_data
    combined_data = pd.concat([metric_groups_data, theme_normalized], ignore_index=True)
    # Group by and sum the relevant columns
    theme_weight = combined_data.groupby(['brandname', 'name', 'id','project_id'])['weight_sum'].sum().reset_index(name='weight_sum')
    combined_data['theme_new_norm_value'] = combined_data['normalized_per_weight']*combined_data['weight_sum']
    theme_normalized = combined_data.groupby(['brandname', 'name', 'id','project_id'])['theme_new_norm_value'].sum().reset_index(name='theme_new_norm_value')
    # Perform the division
    final_theme_normalized_data = theme_weight.merge(theme_normalized, on=['brandname', 'name', 'id','project_id'])
    final_theme_normalized_data['final_normalized_value'] = final_theme_normalized_data['theme_new_norm_value'] / final_theme_normalized_data['weight_sum']
    final_theme_normalized_data.to_csv("combined_data.csv",index=False)
    print("final_theme_normalized_data")
    print(final_theme_normalized_data)
    #normalised
    theme_normalized_group_data = theme_normalized_data[['id','metric_ids','metricname','name', 'project_id', 'brandname', 'weight_sum', 'normalized_per_weight']]
    print("theme_group")
    print(theme_normalized_group_data)
    theme_normalized_group_data = theme_normalized_group_data[['id','metric_ids','name', 'metricname','project_id', 'brandname', 'weight_sum', 'normalized_per_weight']]
    theme_normalized_group_data['weight_sum'] = pd.to_numeric(theme_normalized_group_data['weight_sum'], errors='coerce')
    theme_normalized_group_data['normalized_per_weight'] = pd.to_numeric(theme_normalized_group_data['normalized_per_weight'], errors='coerce')
    
    metric_groups_data = metric_groups_data.rename(columns={'metric_group_ids': 'metric_ids'})
    print("metric_group_data")
    print(metric_groups_data)
    combined_data_metric = pd.concat([theme_normalized_group_data,metric_groups_data], ignore_index=True)
    print("combinedd")
    print(combined_data_metric)
    combined_data_metric.to_csv("combined_data_metric.csv",index=False)
    final_theme = theme_normalized_group_data.groupby(['id','brandname'])['normalized_per_weight'].mean().reset_index()
    print("normalised_value")
    print(final_theme)
    final_theme_normalised_value = final_theme.rename(columns={'normalized_per_weight': 'mean_normalized_per_weight'})
    final_theme_normalised_value = combined_data_metric.merge(final_theme_normalised_value, 
                                                          left_on = ['id','brandname'],
                                                           right_on = ['id','brandname'], how='inner')
    # Update the normalized_per_weight column with the mean value
    #final_theme['final_theme_normalised'] = final_theme['mean_normalized_per_weight']
    final_theme_normalised_value = final_theme_normalised_value.rename(columns={'id': 'theme_group_d','metric_ids':'metric_group_ids','name':'theme_group_name','metricname':'metric_group_name','mean_normalized_per_weight':'final_theme_norm_value'})
    final_theme_normalised_value.to_csv("final_theme_normalised_value.csv",index=False)
    print(final_theme_normalised_value)
    return final_theme_normalised_value



@app.post("/theme_normalised")
async def fetch_data(payload: RequestPayload):
    conn = connect_to_db()
    try:
        # Fetch metrics and metric groups data
        metrics_data, metric_groups_data = fetch_table(conn, payload.project_ids)
        
        if metrics_data.empty:
            raise HTTPException(status_code=404, detail="No metrics data found for the given project_ids")

        # Fetch normalized data
        normalized_data = fetch_normalized_data(conn, payload.project_ids)
        
        # Fetch group normalized data
        group_normalised_data = fetch_group_normalised(conn, payload.project_ids)
        
        # Join metrics and normalized data
        theme_normalized_data = join_metric_and_normalized_data(metrics_data, normalized_data)
        
        # Combine metric groups and group normalized data
        final_df_output = final_data(metric_groups_data, group_normalised_data, theme_normalized_data)

        return final_df_output.to_dict(orient='records')

    finally:
        if conn:
            conn.close()

