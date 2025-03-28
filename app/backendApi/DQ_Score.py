from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import pandas as pd
import numpy as np
import psycopg2
from typing import List

from fastapi.middleware.cors import CORSMiddleware
app = FastAPI()
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
SCHEMA_NAME = 'public'

# Pydantic model for request payload
class RequestPayload(BaseModel):
    project_id: int



# Function to connect to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        raise HTTPException(status_code=500, detail="Unable to connect to the database")

# Function to join tables and get data
def join_table(conn) -> pd.DataFrame:
    query = f"""
    SELECT nv.*, b.name AS brand_name, c.name AS category_name
    FROM {SCHEMA_NAME}.normalisedvalue nv
    LEFT JOIN {SCHEMA_NAME}.brands b ON nv.brandName = b.name
    LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
    """
    try:
        df = pd.read_sql_query(query, conn)
        print(df)
        return df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

# Function to load data with joins
def load_master_table(conn, project_id: int) -> pd.DataFrame:
    query = f"""
    SELECT * FROM {SCHEMA_NAME}.normalisedvalue
    WHERE project_id = {project_id}
    """
    try:
        df = pd.read_sql_query(query, conn)
        print(df)
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
        sum_of_product = df.groupby(['brandname','categoryname']).apply(calculate_final_score)
        
        # Format the output for overall scores
        overall_results = pd.DataFrame({
            'Brand_Name': sum_of_product.index.get_level_values('brandname'),
            'Category_Name': sum_of_product.index.get_level_values('categoryname'),
            'Overall_Final_Score': sum_of_product.values
        })

        # Calculate section-wise final scores for overall type
        sectionwise = df.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
        for section in sectionwise.columns:
            overall_results[section] = sectionwise[section].values

        # Ensure additional columns exist
        for col in additional_columns:
            if col not in overall_results.columns:
                overall_results[col] = 0

        # Unique id for each row
        overall_results['id'] = range(1, len(overall_results) + 1)

    # Select only the required columns
    final_result = overall_results[['Brand_Name', 'Overall_Final_Score','Category_Name'] + additional_columns]

    return final_result

# FastAPI endpoint to get data
@app.post("/get_data")
def get_data(payload: RequestPayload):
    conn = connect_to_db()
    try:
        df = join_table(conn)
        print(df)
        df = load_master_table(conn, payload.project_id)
        print(df)
        if df is not None and not df.empty:
            # Compute final scores
            final_scores_df = compute_final_scores(df)
            print(final_scores_df)
            # Convert to dictionary format
            result = final_scores_df.to_dict(orient='records')
            print(result)
            return {"data": result}
        else:
            return {"data": []}
    finally:
        conn.close()


# Main entry point for testing FastAPI locally
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
