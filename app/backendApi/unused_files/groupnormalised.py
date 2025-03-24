from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import psycopg2
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import json
from app.backendApi.middlewares.auth_middleware import BearerTokenMiddleware

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

class RequestPayload(BaseModel):
    project_ids: List[int]

def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")

def fetch_table(conn, project_ids: List[int]) -> pd.DataFrame:
    if not project_ids:
        raise HTTPException(status_code=400, detail="project_ids must be provided")
    
    project_ids_str = ', '.join(map(str, project_ids))
    query = f'SELECT * FROM {SCHEMA_NAME}."metricGroups" WHERE project_id IN ({project_ids_str})'
    
    try:
        df = pd.read_sql_query(query, conn)
        result = []
        for _, row in df.iterrows():
            for metric in row['metric_ids']:
                result.append({
                    'id': row['id'],
                    'name': row['name'],
                    'project_id': row['project_id'],
                    'metric_ids': str(metric)
                })
        print(df)
        return pd.DataFrame(result)
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

def fetch_normalized_data(conn, project_ids: List[int]) -> pd.DataFrame:
    if not project_ids:
        raise HTTPException(status_code=400, detail="project_ids must be provided")
    
    project_ids_str = ', '.join(map(str, project_ids))
    query = f'SELECT * FROM {SCHEMA_NAME}."normalisedvalue" WHERE project_id IN ({project_ids_str})'
    
    try:
        return pd.read_sql_query(query, conn)
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the normalized value table: {e}")

def join_metric_and_normalized_data(metric_data: pd.DataFrame, normalized_data: pd.DataFrame) -> pd.DataFrame:
    print(metric_data.columns)
    metric_data['metric_ids'] = metric_data['metric_ids'].astype(str)
    normalized_data['metricid'] = normalized_data['metricid'].astype(str)

    normalized_data = normalized_data.drop_duplicates(subset=['project_id', 'metricid', 'categoryid', 'sectionid', 'platformid', 'weights', 'benchmarkvalue', 'actualvalue', 'normalized', 'brandname', 'sectionname', 'categoryname', 'metricname', 'platformname', 'type'])
    joined_data = metric_data.merge(
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
    weight_sum_df = joined_data.groupby(['brandname','name','id','project_id_x'])['weights'].sum().reset_index(name='weight_sum')
    print("weight_sum_df")
    print(weight_sum_df)
    # Step 2: Calculate new normalized values
    joined_data['new_norm_value'] = joined_data['normalized'] * joined_data['weights']
    print(joined_data)
    # Step 3: Calculate sum of normalized values
    norm_sum_df = joined_data.groupby(['brandname','name','id','project_id_x'])['new_norm_value'].sum().reset_index(name='normalized_value')
    print("norm_sum_df")
    print(norm_sum_df)
    # Step 4: Combine results
    group_normalized_value = norm_sum_df.merge(
        weight_sum_df,
        left_on=["brandname","name","id","project_id_x"],
        right_on=["brandname","name","id","project_id_x"],
        how='inner'
    ) 
    #norm_sum_df.merge(weight_sum_df, on='brandname')
    group_normalized_value['normalized_per_weight'] = group_normalized_value['normalized_value'] / group_normalized_value['weight_sum']
    group_normalized_value = group_normalized_value.rename(columns={'project_id_x': 'project_id'})
    group_normalized_value = pd.DataFrame(group_normalized_value)
    # mean_group_normalized_value = group_normalized_value.groupby(['id','brandname'])['normalized_per_weight'].mean().reset_index()
    # mean_normalized_per_weight = mean_group_normalized_value.rename(columns={'normalized_per_weight': 'mean_normalized_per_weight'})
    # group_normalized_value = group_normalized_value.merge(mean_normalized_per_weight, 
    #                                                       left_on = ['id','brandname'],
    #                                                        right_on = ['id','brandname'], how='inner')
    # # Update the normalized_per_weight column with the mean value
    # group_normalized_value['normalized_per_weight'] = group_normalized_value['mean_normalized_per_weight']
    print("group")   
    print(group_normalized_value)
    return group_normalized_value[['project_id','id','name','brandname','weight_sum','normalized_per_weight']]

def insert_data(conn, data: pd.DataFrame):
    # Ensure the DataFrame is not empty
    if data.empty:
        print("No data to insert.")
        return

    # Correct the table name
    create_query = '''
    CREATE TABLE IF NOT EXISTS groupnormalisedvalue (
        id INTEGER,
        project_id INTEGER,
        brandname VARCHAR(255),
        name TEXT,
        weight_sum REAL,
        normalized_per_weight REAL,
        UNIQUE(brandname, name, id, project_id) 
    );
    '''
    
    cur = conn.cursor()
    cur.execute(create_query)

    insert_query = '''
    INSERT INTO groupnormalisedvalue (brandname, name, id, project_id, weight_sum, normalized_per_weight)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (brandname, name, id, project_id) DO NOTHING;
    '''

    # Debugging: Print the DataFrame to insert
    print("Inserting data:")
    print(data)

    for _, row in data.iterrows():
        cur.execute(insert_query, (row['brandname'], row['name'], row['id'], row['project_id'], row['weight_sum'], row['normalized_per_weight']))

    conn.commit()
    cur.close()

# @app.post("/group_normalised")
# async def fetch_data(payload: RequestPayload):
#     conn = connect_to_db()
#     try:
#         # Fetch the metric and normalized data
#         metric_data = fetch_table(conn, payload.project_ids)
#         normalized_data = fetch_normalized_data(conn, payload.project_ids)
        
#         # Join the two dataframes
#         final_df = join_metric_and_normalized_data(metric_data, normalized_data)

#         # Reshape the DataFrame to get the desired output format
#         reshaped_df = final_df.groupby(
#             ['project_id', 'id', 'name', 'brandname']
#         ).agg({
#             'metricname': lambda x: list(x),  # Aggregate metricname into a list
#             'weight_sum': 'first',  # Keep the first weight_sum
#             'normalized_per_weight': 'first'  # Keep the first normalized_per_weight
#         }).reset_index()

#         # Insert the reshaped data into the database
#         insert_data(conn, reshaped_df)

#         return JSONResponse(content=reshaped_df.to_dict(orient="records"))
    
#  finally:
#         conn.close()

@app.post("/group_normalised")
async def fetch_data(payload: RequestPayload):
    conn = connect_to_db()
    try:
        metric_data = fetch_table(conn, payload.project_ids)
        normalized_data = fetch_normalized_data(conn, payload.project_ids)
        final_df = join_metric_and_normalized_data(metric_data, normalized_data)

        # Insert the final DataFrame into the database
        insert_data(conn, final_df)

        return JSONResponse(content=final_df.to_dict(orient="records"))
    
    finally:
        conn.close()