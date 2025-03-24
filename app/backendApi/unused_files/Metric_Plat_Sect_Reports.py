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

# Request payload for accepting project IDs
class RequestPayload(BaseModel):
    project_ids: List[int]
    brandname: str

# Function to connect to the PostgreSQL database
def connect_to_db():
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        return None

# Function to fetch normalized data from the database
def fetch_normalized_data(conn, project_ids: List[int]) -> pd.DataFrame:
    if not project_ids:
        raise HTTPException(status_code=400, detail="project_ids must be provided")
    
    project_ids_str = ', '.join(map(str, project_ids))
    query = f'SELECT * FROM {SCHEMA_NAME}."normalisedvalue" WHERE project_id IN ({project_ids_str})'
    
    try:
        return pd.read_sql_query(query, conn)
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the normalized value table: {e}")

# Function to get brand information from external API
def get_brand_info(project_id: int, brandname: str):
    url = f"https://m594bmgj-8018.inc1.devtunnels.ms/brands/{brandname}/project_id/{project_id}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        combined_data = []
        
        # Add main brand data
        main_brand_data = data['main_brand']
        main_brand_data['project_id'] = project_id
        main_brand_data['type'] = 'main_brand'
        combined_data.append(main_brand_data)
        
        # Add competitors data
        competitors_data = data['competitors']
        for competitor in competitors_data:
            competitor['project_id'] = project_id
            competitor['type'] = 'competitor'
            combined_data.append(competitor)
        return pd.DataFrame(combined_data)
    else:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve brand info. Status code: {response.status_code}")

# Function to join normalized data and brand info
def join_data(normalized_data: pd.DataFrame, project_ids: List[int], brandname: str) -> pd.DataFrame:
    results = []
    
    for project_id in project_ids:
        try:
            brand_info_df = get_brand_info(project_id, brandname)  # Pass both project_id and brandname
            merged_data = normalized_data.merge(
                brand_info_df,
                left_on=['project_id', 'brandname'],
                right_on=['project_id', 'brand'],
                how='inner'
            )
            results.append(merged_data)
        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing brand data for project_id {project_id}: {str(e)}")
    
    return pd.concat(results, ignore_index=True)

# Function to compare normalized values and assign 'above' and 'below' brands
def compare_normalized(group):
    # Get the rows with the maximum and minimum 'normalized' values
    max_row = group.loc[group['normalized'].idxmax()]
    min_row = group.loc[group['normalized'].idxmin()]

    # Assign 'above' and 'below' columns
    group['above'] = f"{max_row['brandname']} - {max_row['normalized']}"
    group['below'] = f"{min_row['brandname']} - {min_row['normalized']}"

    # Attempt to find the main brand row
    main_brand_rows = group[group['type_y'] == 'main_brand']
    
    if not main_brand_rows.empty:
        # If there are main brand rows, select the first one
        main_brand_row = main_brand_rows.iloc[0]
        group['brand'] = f"{main_brand_row['brandname']} - {main_brand_row['normalized']}"
    else:
        # If no 'main_brand' row is found, handle this case (default value or skip)
        group['brand'] = 'No main brand found'

    return group

# @app.post endpoint with calculation logic inside
@app.post("/metric_report")
async def fetch_data(payload: RequestPayload):
    """Fetch data based on project IDs, and merge with brand data."""
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    # Fetch normalized data from the database
    normalized_data = fetch_normalized_data(conn, payload.project_ids)
    
    # Join the normalized data with brand info (now passing the brandname)
    results = join_data(normalized_data, payload.project_ids, payload.brandname)
    
    # Calculation logic inside the endpoint
    # Apply the comparison function
    above_below = results.groupby(['platformname', 'metricname', 'sectionname'], group_keys=False).apply(compare_normalized)
    above_below = above_below[['project_id','sectionname', 'metricname', 'platformname', 'brandname', 'normalized', 'above', 'below','type_y',"benchmarkvalue","brand"]]

    # Filter to only include main brands
    above_below = above_below[above_below['type_y'] == 'main_brand']
    
    # Group and aggregate by section, metric, and platform
    weight_norm = results.groupby(['sectionname', 'metricname', 'platformname'], as_index=False).agg(
        weights_sum=('weights', 'mean'),
        normalized_avg=('normalized', 'mean'),
        benchmarkvalue = ('benchmarkvalue','mean')
    )
    
    # Merge the above_below and weight_norm DataFrames
    final_result = above_below.merge(weight_norm, 
                                     left_on=['sectionname', 'metricname', 'platformname'], 
                                 right_on=['sectionname', 'metricname', 'platformname'], how='inner')
    
    # Return the combined results as a list of dictionaries (or JSON-friendly format)
    return {"data": final_result.to_dict(orient="records")}


@app.post("/platform_report")
async def fetch_data(payload: RequestPayload):
    """Fetch data based on project IDs, and merge with brand data."""
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    # Fetch normalized data from the database
    normalized_data = fetch_normalized_data(conn, payload.project_ids)
    
    # Join the normalized data with brand info
    results = join_data(normalized_data, payload.project_ids, payload.brandname)
    
    # Perform calculations and return the final result
    # Apply the comparison function to get 'above', 'below', and the correct 'brand'
    above_below = results.groupby(['sectionname', 'platformname'], group_keys=False).apply(compare_normalized)
    
    # Select necessary columns
    above_below = above_below[['project_id', 'sectionname', 'platformname', 'normalized', 'above', 'below', 'type_y', 'brand']]

    # Filter to only include the main brands
    above_below = above_below[above_below['type_y'] == 'main_brand']
    print("Above and Below data preview:")
    #  # Print the first few rows of `above_below`
    print("result")
    
    weight_unique = results.groupby('platformname')['metricname'].nunique()
    weight_avg = results.groupby(['sectionname', 'platformname'])["weights"].mean()
    weight_sum = weight_unique * weight_avg
    print(results)
  
    #weight_norm = weight_unique * weight_avg
    print(weight_unique)
    # Group and aggregate by section, metric, and platform
    weight_norm = results.groupby(['sectionname', 'platformname'], as_index=False).agg(
        normalized_avg=('normalized', 'mean')
    )
    weight_norm['weights_sum'] = weight_sum.reset_index(drop=True)
    print(weight_norm)

    # Merge the above_below and weight_norm DataFrames
    final_result = above_below.merge(weight_norm, 
                                     left_on=['sectionname', 'platformname'], 
                                     right_on=['sectionname', 'platformname'], 
                                     how='inner')
    
    # Select the final columns to return
    final_result = final_result[["project_id", "sectionname", "platformname", "above", "below", "weights_sum", "normalized_avg", "brand"]]

    # Drop duplicates based on the necessary columns
    final_result = final_result.drop_duplicates(subset=["project_id", "sectionname", "platformname", "above", "below", "brand"])

    print("Final results:")
    print(final_result)
    
    # Return the combined results as a list of dictionaries (or JSON-friendly format)
    return {"data": final_result.to_dict(orient="records")}


@app.post("/sectional_report")
async def sectional_report(payload: RequestPayload):
    """Fetch data based on project IDs, and merge with brand data."""
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    # Fetch normalized data from the database
    normalized_data = fetch_normalized_data(conn, payload.project_ids)
    
    # Join the normalized data with brand info
    results = join_data(normalized_data, payload.project_ids, payload.brandname)

    # Apply comparison logic and calculations
    # Step 1: Apply the comparison function
    above_below = results.groupby(['sectionname'], group_keys=False).apply(compare_normalized)
    above_below = above_below[['project_id','sectionname','above', 'below','type_y',"brand"]]

    # Filter to only include main brands
    above_below = above_below[above_below['type_y'] == 'main_brand']
    results = results[results['type_y'] == 'main_brand']
    
    # Step 2: Perform weight-related calculations
    weight_unique = results.groupby('sectionname')['metricname'].count()
    weight_avg = results.groupby('sectionname')["weights"].mean()
    weight_sum = weight_unique * weight_avg
    
    # Group and aggregate by section, metric, and platform
    weight_norm = results.groupby(['sectionname'], as_index=False).agg(
        weights_sum=('weights', 'sum'),
        normalized_avg=('normalized', 'mean')
    )
    weight_norm['weights_sum'] = weight_sum.reset_index(drop=True)

    # Step 3: Merge the comparison and weight data
    final_result = above_below.merge(weight_norm, 
                                     left_on=['sectionname'], 
                                     right_on=['sectionname'], 
                                     how='inner')
    final_result = final_result.drop_duplicates()

    # Return the final result as a list of dictionaries (or JSON-friendly format)
    return {"data": final_result.to_dict(orient="records")}