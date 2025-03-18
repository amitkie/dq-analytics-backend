from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import numpy as np
import psycopg2
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from app.backendApi.auth_middleware import BearerTokenMiddleware

app = FastAPI()

# CORS configuration
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

app.add_middleware(BearerTokenMiddleware)

# Database connection parameters
DB_PARAMS = {
    'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
    'port': '5434',
    'dbname': 'KIESQUAREDE',
    'user': 'KIESQUAREDE',
    'password': 'KIESQUARE123'
}
SCHEMA_NAME = 'public'

# Request Payload Schema
class RequestPayload(BaseModel):
    project_ids: List[int]  # List of project IDs

# Function to connect to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        raise HTTPException(status_code=500, detail="Unable to connect to the database")

# Function to load data based on project_ids
def load_master_table(conn, project_ids: List[int]) -> pd.DataFrame:
    query = f"""
    SELECT * FROM {SCHEMA_NAME}.normalisedvalue
    WHERE project_id IN %s
    """
    try:
        df = pd.read_sql_query(query, conn, params=(tuple(project_ids),))
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

# Function to compute final scores and organize data by project_id
def compute_final_scores(df: pd.DataFrame) -> dict:
    project_results = {}
    additional_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']
    
    if not df.empty:
        # Group by project_id
        for project_id, project_group in df.groupby('project_id'):
            overall_results = pd.DataFrame()

            # Calculate overall final scores for each brand in the project
            sum_of_product = project_group.groupby('brandname').apply(calculate_final_score)
            overall_results = pd.DataFrame({
                'Brand_Name': sum_of_product.index,
                'Overall_Final_Score': sum_of_product.values
            })

            # Calculate section-wise final scores
            sectionwise = project_group.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
            for section in sectionwise.columns:
                overall_results[section] = sectionwise[section].values

            # Ensure additional columns exist
            for col in additional_columns:
                if col not in overall_results.columns:
                    overall_results[col] = 0

            # Add an id column
            overall_results['id'] = range(1, len(overall_results) + 1)

            # Select only the required columns
            final_result = overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]
            
            # Store the result grouped by project_id
            project_results[project_id] = final_result.to_dict(orient="records")
    
    return project_results

# FastAPI endpoint to get data
@app.post("/multiple_get_brand_data")
def get_data(payload: RequestPayload):
    conn = connect_to_db()
    try:
        # Load data for all brands present in the project_ids
        df = load_master_table(conn, payload.project_ids)
        
        if df is not None and not df.empty:
            # Compute final scores and organize by project_id
            project_scores = compute_final_scores(df)
            # Return the data grouped by project_id
            return {"data": project_scores}
        else:
            return {"data": {}}
    finally:
        conn.close()

# Main entry point for testing FastAPI locally
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
