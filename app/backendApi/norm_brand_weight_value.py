from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import psycopg2
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import numpy as np

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

# CORS Middleware for frontend access
origins = ["http://localhost", "http://localhost:3000", "https://example.com", "*"]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# Request model for accepting project IDs
class RequestPayload(BaseModel):
    project_ids: List[int]

# Function to connect to the PostgreSQL database
def connect_to_db():
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

# Function to load user project data
def load_user_projects(conn, project_id: int) -> pd.DataFrame:
    query = f"""
    SELECT id, project_name FROM {SCHEMA_NAME}."userProjects"
    WHERE id = {project_id}
    """
    try:
        df_name = pd.read_sql_query(query, conn)
        return df_name
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the user projects table: {e}")

# Function to clean data
def clean_data(results: pd.DataFrame) -> pd.DataFrame:
    # Replace NaN, Infinity, and -Infinity with None or a valid value
    results.replace([np.inf, -np.inf], np.nan, inplace=True)
    results.fillna(0, inplace=True)  # Replace NaN with None (or could use other placeholders)
    return results

# Function to perform the final calculation for normalized weight values
def calculation_weight_value(project_df: pd.DataFrame) -> dict:
    metrics = {}
    # Group by 'sectionname' and 'platformname'
    for (section, platform), group in project_df.groupby(['sectionname', 'platformname']):
        metrics_for_group = {}
        # Iterate through the grouped rows to extract the metrics
        for _, row in group.iterrows():
            metrics_for_group[row['metricname']] = row['weights']
        
        # Store the metrics for each section and platform
        if section not in metrics:
            metrics[section] = {}
        metrics[section][platform] = metrics_for_group
    
    return metrics

# Function to perform the normalized data comparison and categorization
def compare_normalized(group):
    Above_80 = group.loc[(group['normalized'] > 80) & (group['normalized'] <= 100), ['project_id','project_name','brandname', 'normalized']]
    Above_60 = group.loc[(group['normalized'] > 60) & (group['normalized'] <= 80), ['project_id','project_name','brandname', 'normalized']]
    Above_50 = group.loc[(group['normalized'] > 50) & (group['normalized'] <= 60), ['project_id','project_name','brandname', 'normalized']]
    Above_20 = group.loc[(group['normalized'] > 20) & (group['normalized'] <= 50), ['project_id','project_name','brandname', 'normalized']]
    Below_20 = group.loc[(group['normalized'] > 0) & (group['normalized'] <= 19), ['project_id','project_name','brandname', 'normalized']]

    # Create concatenated strings for each category
    group["Above81_100"] = ", ".join(Above_80.apply(lambda row: f"{row['project_id']}_{row['project_name']}-{row['brandname']} - {row['normalized']}", axis=1))
    group["Between61_80"] = ", ".join(Above_60.apply(lambda row: f"{row['brandname']} - {row['normalized']}", axis=1))
    group["Between51_60"] = ", ".join(Above_50.apply(lambda row: f"{row['brandname']} - {row['normalized']}", axis=1))
    group["Between20_50"] = ", ".join(Above_20.apply(lambda row: f"{row['brandname']} - {row['normalized']}", axis=1))
    group["Below0_19"] = ", ".join(Below_20.apply(lambda row: f"{row['brandname']} - {row['normalized']}", axis=1))

    return group

# Function to perform the final calculation for normalized data categorization
def calculation_normalized(project_df: pd.DataFrame) -> pd.DataFrame:
    grouped_data = project_df.groupby(['sectionname','platformname','metricname'], group_keys=False).apply(compare_normalized)
    # Select only necessary columns
    final_result = grouped_data[['sectionname','platformname','metricname',
                                 'Above81_100', 'Between61_80', 
                                 'Between51_60', 'Between20_50', 
                                 'Below0_19']]
    # Clean the data to remove NaN and extreme values
    cleaned_data = clean_data(final_result)
    return cleaned_data

@app.post("/norm_weight_value")
async def fetch_weight_data(payload: RequestPayload):
    """Fetch normalized weight values based on project IDs."""
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    results = []  # List to hold results for each project
    for project_id in payload.project_ids:
        # Fetch data for the specific project_id
        project_df = fetch_normalized_data(conn, [project_id])
        project_name_df = load_user_projects(conn, project_id)

        if project_df is not None and not project_df.empty and project_name_df is not None and not project_name_df.empty:
            # Merge project name with data
            project_merge_df = pd.merge(project_df, project_name_df[['id', 'project_name']], left_on='project_id', right_on='id', how='left')

            # Clean the data
            project_df = clean_data(project_merge_df)

            # Perform calculations to extract metrics, grouped by sectionname and platformname
            metrics = calculation_weight_value(project_df)

            # Append the result for this project, grouped by sectionname and platformname
            for section, platforms in metrics.items():
                for platform, platform_metrics in platforms.items():
                    results.append({
                        "project_id": project_id,
                        "project_name": project_name_df.iloc[0]['project_name'],
                        "sectionname": section,
                        "platformname": platform,
                        "metrics": platform_metrics
                    })
    
    # Return the final results
    return {"data": results}


@app.post("/norm_brand_value")
async def fetch_normalized_data_with_brand(payload: RequestPayload):
    """Fetch normalized data and categorize it based on specific thresholds."""
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    results = []  # List to hold results for each project
    for project_id in payload.project_ids:
        # Fetch data for the specific project_id
        project_df = fetch_normalized_data(conn, [project_id])
        project_name_df = load_user_projects(conn, project_id)

        if project_df is not None and not project_df.empty and project_name_df is not None and not project_name_df.empty:
            # Merge project name with data
            project_merge_df = pd.merge(project_df, project_name_df[['id', 'project_name']], left_on='project_id', right_on='id', how='left')

            # Clean the data
            project_df = clean_data(project_merge_df)

            # Perform the normalized value calculation and categorization
            final_result = calculation_normalized(project_df)

            # Append the result for this project
            results.append({
                "project_id": project_id,
                "project_name": project_name_df.iloc[0]['project_name'],
                "normalized_data": final_result.to_dict(orient="records")
            })
    
    # Return the final results
    return {"data": results}
