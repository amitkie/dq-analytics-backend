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
    project_ids: List[int]  # Accepting multiple project IDs


# Function to connect to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        raise HTTPException(status_code=500, detail="Unable to connect to the database")

# Function to load data with joins
def load_master_table(conn, project_id: int) -> pd.DataFrame:
    query = f"""
    SELECT * FROM {SCHEMA_NAME}.normalisedvalue
    WHERE project_id = {project_id}
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")
def load_user_projects(conn, project_id: int) -> pd.DataFrame:
    query = f"""
    SELECT id, project_name FROM {SCHEMA_NAME}."userProjects"
    WHERE id = {project_id}
    """
    try:
        df_name = pd.read_sql_query(query, conn)
        return df_name
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

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


# FastAPI endpoint to get data (modified to perform the join)
@app.post("/get_multi_data")
def get_data(payload: RequestPayload):
    conn = connect_to_db()
    try:
        df = join_table(conn)
        all_final_scores = []

        for project_id in payload.project_ids:
            project_df = load_master_table(conn, project_id)
            project_name_df = load_user_projects(conn, project_id)

            if project_df is not None and not project_df.empty and project_name_df is not None and not project_name_df.empty:
                # Perform the join to add project name to project_df
                project_df = pd.merge(project_df, project_name_df[['id', 'project_name']], left_on='project_id', right_on='id', how='left')

                # Compute final scores
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
                    "project_name": project_name_df['project_name'].iloc[0],  # Add project name to the response
                    "brands": brands_list
                })

        if all_final_scores:
            return {"data": all_final_scores}
        else:
            return {"data": []}
    finally:
        conn.close()
