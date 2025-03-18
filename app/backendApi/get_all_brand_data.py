# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# import pandas as pd
# import numpy as np
# import psycopg2
# from typing import List
# from fastapi.middleware.cors import CORSMiddleware

# app = FastAPI()

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
# DB_PARAMS = {
#     'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
#     'port': '5434',
#     'dbname': 'KIESQUAREDE',
#     'user': 'KIESQUAREDE',
#     'password': 'KIESQUARE123'
# }
# SCHEMA_NAME = 'public'

# class RequestPayload(BaseModel):
#     project_ids: List[int]  # Now only project_ids

# # Function to connect to the database
# def connect_to_db():
#     try:
#         conn = psycopg2.connect(**DB_PARAMS)
#         return conn
#     except psycopg2.Error as e:
#         print(f"Unable to connect to the database: {e}")
#         raise HTTPException(status_code=500, detail="Unable to connect to the database")

# # Function to load data for the given project_ids
# def load_master_table(conn, project_ids: List[int]) -> pd.DataFrame:
#     # Modify the query to fetch data for all projects in the provided project_ids
#     query = f"""
#     SELECT nv.*, b.name AS brand_name, c.name AS category_name
#     FROM {SCHEMA_NAME}.normalisedvalue nv
#     LEFT JOIN {SCHEMA_NAME}.brands b ON nv.brandName = b.name
#     LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
#     WHERE nv.project_id IN %s
#     """
#     try:
#         df = pd.read_sql_query(query, conn, params=(tuple(project_ids),))
#         return df
#     except psycopg2.Error as e:
#         raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

# # Function to calculate final score for each group
# def calculate_final_score(group: pd.DataFrame) -> float:
#     weight_sum = group['weights'].sum()
#     if weight_sum == 0:
#         return np.nan
#     score = (group['weights'] * group['normalized']).sum() / weight_sum
#     return score

# def compute_final_scores(df: pd.DataFrame) -> pd.DataFrame:
#     overall_results = pd.DataFrame()
#     additional_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']

#     if not df.empty:
#         # Compute overall final score for each (project_id, brand_name) pair
#         sum_of_product = df.groupby(['project_id', 'brand_name']).apply(calculate_final_score)
#         overall_results = pd.DataFrame({
#             'Project_ID': sum_of_product.index.get_level_values(0),
#             'Brand_Name': sum_of_product.index.get_level_values(1),
#             'Overall_Final_Score': sum_of_product.values
#         })

#         # Compute section-wise scores for each (project_id, brand_name) pair
#         sectionwise = df.groupby(['project_id', 'brand_name', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
#         for section in sectionwise.columns:
#             overall_results[section] = sectionwise[section].values

#         # Add additional columns if they are missing
#         for col in additional_columns:
#             if col not in overall_results.columns:
#                 overall_results[col] = 0

#         # Add an ID column
#         overall_results['id'] = range(1, len(overall_results) + 1)

#     final_result = overall_results[['Project_ID', 'Brand_Name', 'Overall_Final_Score'] + additional_columns]
#     return final_result

# # FastAPI endpoint to get data based on project_ids
# @app.post("/get_project_brand_data")
# def get_data(payload: RequestPayload):
#     conn = connect_to_db()
#     try:
#         # Get the data for the provided project_ids
#         df = load_master_table(conn, payload.project_ids)
#         if df is not None and not df.empty:
#             final_scores_df = compute_final_scores(df)
#             return {"data": final_scores_df.to_dict(orient="records")}
#         else:
#             return {"data": []}
#     finally:
#         conn.close()

# # Main entry point for testing FastAPI locally
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)





from fastapi import FastAPI, HTTPException
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

class RequestPayload(BaseModel):
    project_ids: List[int]  # Now only project_ids

# Function to connect to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        raise HTTPException(status_code=500, detail="Unable to connect to the database")

# Function to load data for the given project_ids
# def load_master_table(conn, project_ids: List[int]) -> pd.DataFrame:
#     # Modify the query to fetch data for all projects in the provided project_ids
#     query = f"""
#     SELECT nv.*, b.name AS brand_name, c.name AS category_name
#     FROM {SCHEMA_NAME}.normalisedvalue nv
#     LEFT JOIN {SCHEMA_NAME}.brands b ON nv.brandName = b.name
#     LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
#     WHERE nv.project_id IN %s
#     """
#     try:
#         df = pd.read_sql_query(query, conn, params=(tuple(project_ids),))
#         return df
#     except psycopg2.Error as e:
#         raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")
# Function to load data for the given project_ids, including project_name
def load_master_table(conn, project_ids: List[int]) -> pd.DataFrame:
    # Modify the query to include the project_name from "userProjects"
    query = f"""
    SELECT nv.*, b.name AS brand_name, c.name AS category_name, up.project_name
    FROM {SCHEMA_NAME}.normalisedvalue nv
    LEFT JOIN {SCHEMA_NAME}.brands b ON nv.brandName = b.name
    LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
    LEFT JOIN {SCHEMA_NAME}."userProjects" up ON nv.project_id = up.id
    WHERE nv.project_id IN %s
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

# def compute_final_scores(df: pd.DataFrame) -> pd.DataFrame:
#     overall_results = pd.DataFrame()
#     additional_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']

#     if not df.empty:
#         # Compute overall final score for each (project_id, brand_name) pair
#         sum_of_product = df.groupby(['project_id', 'brand_name']).apply(calculate_final_score)
#         overall_results = pd.DataFrame({
#             'Project_ID': sum_of_product.index.get_level_values(0),
#             'Brand_Name': sum_of_product.index.get_level_values(1),
#             'Overall_Final_Score': sum_of_product.values
#         })

#         # Compute section-wise scores for each (project_id, brand_name) pair
#         sectionwise = df.groupby(['project_id', 'brand_name', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
#         for section in sectionwise.columns:
#             overall_results[section] = sectionwise[section].values

#         # Add additional columns if they are missing
#         for col in additional_columns:
#             if col not in overall_results.columns:
#                 overall_results[col] = 0

#         # Add an ID column
#         overall_results['id'] = range(1, len(overall_results) + 1)

#     final_result = overall_results[['Project_ID', 'Brand_Name', 'Overall_Final_Score'] + additional_columns]
#     return final_result
def compute_final_scores(df: pd.DataFrame) -> pd.DataFrame:
    overall_results = pd.DataFrame()
    additional_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']

    if not df.empty:
        # Compute overall final score for each (project_id, brand_name) pair
        sum_of_product = df.groupby(['project_id', 'brand_name']).apply(calculate_final_score)
        overall_results = pd.DataFrame({
            'Project_ID': sum_of_product.index.get_level_values(0),
            'Brand_Name': sum_of_product.index.get_level_values(1),
            'Overall_Final_Score': sum_of_product.values
        })

        # Add project_name to the overall results
        project_names = df.groupby(['project_id'])['project_name'].first()  # Assuming project_name is unique per project_id
        overall_results['Project_Name'] = overall_results['Project_ID'].map(project_names)

        # Compute section-wise scores for each (project_id, brand_name) pair
        sectionwise = df.groupby(['project_id', 'brand_name', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
        for section in sectionwise.columns:
            overall_results[section] = sectionwise[section].values

        # Add additional columns if they are missing
        for col in additional_columns:
            if col not in overall_results.columns:
                overall_results[col] = 0

        # Add an ID column
        overall_results['id'] = range(1, len(overall_results) + 1)

    final_result = overall_results[['Project_ID', 'Project_Name', 'Brand_Name', 'Overall_Final_Score'] + additional_columns]
    return final_result

# FastAPI endpoint to get data based on project_ids
@app.post("/get_project_brand_data")
def get_data(payload: RequestPayload):
    conn = connect_to_db()
    try:
        # Get the data for the provided project_ids
        df = load_master_table(conn, payload.project_ids)
        if df is not None and not df.empty:
            final_scores_df = compute_final_scores(df)
            return {"data": final_scores_df.to_dict(orient="records")}
        else:
            return {"data": []}
    finally:
        conn.close()

# Main entry point for testing FastAPI locally
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
