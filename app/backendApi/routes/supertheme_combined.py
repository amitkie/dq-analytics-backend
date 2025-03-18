from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import psycopg2
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from fastapi.responses import JSONResponse
import requests

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
    "https://m594bmgj-7033.inc1.devtunnels.ms",
    "https://*.devtunnels.ms",
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

    theme_normalized_value['normalized_per_weight'] = theme_normalized_value['normalized_value'] / theme_normalized_value['weight_sum']
    theme_normalized_data = theme_normalized_value.rename(columns={'project_id_x': 'project_id'})
    print("group")   
    theme_normalized_data = theme_normalized_data.drop_duplicates()
    theme_normalized_data.drop_duplicates(subset=['name', 'id', 'metric_ids'], inplace=True)
    print("theme_normalized_data")
    print(theme_normalized_data)
    return theme_normalized_data[['project_id','id','metricname','metric_ids','name','brandname','weight_sum','normalized_per_weight']]

def fetch_metric_groups_table(conn, project_ids: List[int]) -> pd.DataFrame:
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

def join_group_metric_and_normalized_data(metric_data: pd.DataFrame, normalized_data: pd.DataFrame) -> pd.DataFrame:
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
    group_normalized_value['normalized_per_weight'] = group_normalized_value['normalized_value'] / group_normalized_value['weight_sum']
    group_normalized_value = group_normalized_value.rename(columns={'project_id_x': 'project_id'})
    print("group")   
    print(group_normalized_value)
    return group_normalized_value[['project_id','id','name','brandname','weight_sum','normalized_per_weight']]

def insert_group_normalised_data(conn, data: pd.DataFrame):
    if data.empty:
        print("No data to insert.")
        return

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

    print("Inserting data:")
    print(data)

    for _, row in data.iterrows():
        cur.execute(insert_query, (row['brandname'], row['name'], row['id'], row['project_id'], row['weight_sum'], row['normalized_per_weight']))

    conn.commit()
    cur.close()

def final_data_theme(metric_groups_result: pd.DataFrame, group_normalised_data: pd.DataFrame, theme_normalized_data: pd.DataFrame) -> pd.DataFrame:
    metric_groups_result['metric_group_ids'] = metric_groups_result['metric_group_ids'].astype(str)
    group_normalised_data['id'] = group_normalised_data['id'].astype(str)
    
    metric_groups_data = metric_groups_result.merge(
        group_normalised_data,
        left_on=['metric_group_ids'],
        right_on=['id'],
        how='inner'
    )
    print("Metric check")
    print(metric_groups_data)

    metric_groups_data = metric_groups_data[["id_x", "name_x", "name_y", "project_id_x", "metric_group_ids", "brandname", "weight_sum", "normalized_per_weight"]]
    metric_groups_data = metric_groups_data.rename(columns={
        'name_x': 'name',
        'name_y': 'metricname',
        'project_id_x': 'project_id',
        'metric_group_ids': 'metric_ids',
        'id_x': 'id'
    })
    print("metric name check")
    print(metric_groups_data)

    theme_normalized_group_data = theme_normalized_data[['id', 'metric_ids', 'metricname', 'name', 'project_id', 'brandname', 'weight_sum', 'normalized_per_weight']]
    theme_normalized_group_data = theme_normalized_group_data[['id', 'metric_ids', 'name', 'metricname', 'project_id', 'brandname', 'weight_sum', 'normalized_per_weight']]
    theme_normalized_group_data['weight_sum'] = pd.to_numeric(theme_normalized_group_data['weight_sum'], errors='coerce')
    theme_normalized_group_data['normalized_per_weight'] = pd.to_numeric(theme_normalized_group_data['normalized_per_weight'], errors='coerce')
    
    print("metric_group_data")
    print(metric_groups_data)
    combined_data_metric = pd.concat([theme_normalized_group_data, metric_groups_data], ignore_index=True)
    print("combinedd")
    print(combined_data_metric)
    combined_data_metric.to_csv("combined_data_metric.csv", index=False)
    
    final_theme = theme_normalized_group_data.groupby(['id', 'brandname'])['normalized_per_weight'].mean().reset_index()
    print("normalised_value")
    print(final_theme)
    
    final_theme_normalised_value = final_theme.rename(columns={'normalized_per_weight': 'mean_normalized_per_weight'})
    final_theme_normalised_value = combined_data_metric.merge(
        final_theme_normalised_value,
        left_on=['id', 'brandname'],
        right_on=['id', 'brandname'],
        how='inner'
    )

    final_theme_normalised_value = final_theme_normalised_value.rename(columns={
        'id': 'theme_group_d',
        'metric_ids': 'metric_group_ids',
        'name': 'theme_group_name',
        'metricname': 'metric_group_name',
        'mean_normalized_per_weight': 'final_theme_norm_value'
    })
    final_theme_normalised_value.to_csv("final_theme_normalised_value.csv", index=False)
    print(final_theme_normalised_value)
    return final_theme_normalised_value

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

        # Rename columns first
        metric_groups_data = metric_groups_data.rename(columns={
            'id_x': 'id',
            'name_x': 'name',
            'name_y': 'metricname',
            'project_id_x': 'project_id',
            'metric_group_ids': 'metric_ids'
        })

        # Select only the desired columns
        metric_groups_data = metric_groups_data[["id", "name", "metricname", "project_id", "weight_sum", "metric_ids"]]
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
async def fetch_weight_sum_data(payload: RequestPayload):
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

@app.post("/group_normalised")
async def create_group_normalised(payload: RequestPayload):
    conn = connect_to_db()
    try:
        metric_data = fetch_metric_groups_table(conn, payload.project_ids)
        normalized_data = fetch_normalized_data(conn, payload.project_ids)
        final_df = join_group_metric_and_normalized_data(metric_data, normalized_data)

        # Insert the final DataFrame into the database
        insert_group_normalised_data(conn, final_df)
        return JSONResponse(content=final_df.to_dict(orient="records"))
    
    finally:
        if conn:
            conn.close()

@app.post("/theme_normalised")
async def get_theme_normalised(payload: RequestPayload):
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
        final_df_output = final_data_theme(metric_groups_data, group_normalised_data, theme_normalized_data)

        return final_df_output.to_dict(orient='records')

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)