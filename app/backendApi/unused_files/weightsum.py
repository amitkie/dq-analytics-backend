from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import psycopg2
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import requests
from app.backendApi.middlewares.auth_middleware import BearerTokenMiddleware

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
    theme_normalized_data = theme_normalized_data.drop('brandname', axis=1)
    theme_normalized_data = theme_normalized_data.drop_duplicates()
    # Optionally, drop duplicates based on specific columns
    theme_normalized_data = theme_normalized_data.drop_duplicates(subset=['name', 'id', 'metric_ids'])

    # If you want to modify in place
    theme_normalized_data.drop_duplicates(inplace=True)

    # Print the result
    print("theme_normalized_data")
    print(theme_normalized_data)
    return theme_normalized_data[['project_id','id','metricname','metric_ids','name','weight_sum','normalized_per_weight']]
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

import pandas as pd
from fastapi import HTTPException

def final_data(metric_groups_result: pd.DataFrame, group_normalised_data: pd.DataFrame, theme_normalized_data: pd.DataFrame) -> list:
    try:
        # Ensure proper type conversion
        metric_groups_result['metric_group_ids'] = metric_groups_result['metric_group_ids'].astype(str)
        group_normalised_data['id'] = group_normalised_data['id'].astype(str)

        # Check for NaN values
        if metric_groups_result.isnull().values.any() or group_normalised_data.isnull().values.any():
            raise ValueError("DataFrames contain NaN values")

        # Merge metric groups with normalized data
        metric_groups_data = metric_groups_result.merge(
            group_normalised_data,
            left_on='metric_group_ids',
            right_on='id',
            how='inner'
        )
        print("Metric check")
        print(metric_groups_data)

        # Check for an empty DataFrame after merging
        if metric_groups_data.empty:
            raise ValueError("Merged DataFrame is empty. No matching records found.")

        # Rename columns
        # Rename columns first
        metric_groups_data = metric_groups_data.rename(columns={
            'id_x': 'id',
            'name_x': 'name',
            'name_y': 'metricname',
            'project_id_x': 'project_id',
            'metric_group_ids': 'metric_ids'
        })

        # Select only the desired columns
        metric_groups_data = metric_groups_data[["id", "name", "metricname", "project_id","weight_sum","metric_ids"]]
        # Grouping and summing the relevant columns
        # metric_groups_data = metric_groups_data.groupby(['id', 'name', 'metricname', 'project_id', 'metric_ids'], as_index=False).agg({
        #     'weight_sum': 'sum'
        # })
        metric_groups_data = metric_groups_data.drop_duplicates()
        print("metric_groups_data")
        print(metric_groups_data)
        # Prepare theme_normalized_data with a source column
        theme_normalized = theme_normalized_data.copy()
        theme_normalized = theme_normalized.drop_duplicates()
        theme_normalized['source'] = 'theme_group'
        metric_groups_data['source'] = 'group_normalised'

        # Combine both datasets
        combined_data = pd.concat([theme_normalized, metric_groups_data], ignore_index=True)
        combined_data = combined_data.drop_duplicates()
        print("combined_data")
        print(combined_data)
        # Create final output structure
        final_output = []

        for theme_id, group in combined_data.groupby('id'):
            theme_data = {
                "id": int(theme_id),  # Ensure it's a native int
                "Theme_name": group.iloc[0]['name'],
                "weight_sum": float(group['weight_sum'].sum()),  # Convert to float
                "project_id": int(group.iloc[0]['project_id']),  # Ensure it's a native int
                "sources": []  # List to hold sources
            }

            # Create a source entry for theme_group
            theme_source = {
                "source": "theme_group",
                "metric_ids": group[group['source'] == 'theme_group'][["metric_ids", "metricname", "weight_sum"]].apply(lambda x: {
                    "metric_ids": str(x['metric_ids']),
                    "metricname": str(x['metricname']),
                    "weight_sum": float(x['weight_sum'])
                }, axis=1).tolist()
            }

            # Create a source entry for group_normalised
            group_source = {
                "source": "group_normalised",
                "metric_ids": group[group['source'] == 'group_normalised'][["metric_ids", "metricname", "weight_sum"]].apply(lambda x: {
                    "metric_ids": str(x['metric_ids']),
                    "metricname": str(x['metricname']),
                    "weight_sum": float(x['weight_sum'])
                }, axis=1).tolist()
            }

            # Add sources to the theme data
            theme_data["sources"].append(theme_source)
            theme_data["sources"].append(group_source)

            final_output.append(theme_data)
           
    except Exception as e:
        print(f"Error during final_data processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return final_output

@app.post("/weight_sum")
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

        # Return the final output directly as it's already a list of dictionaries
        return final_df_output

    finally:
        if conn:
            conn.close()
