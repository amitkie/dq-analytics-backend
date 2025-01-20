from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import psycopg2
import json
import numpy as np
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
    project_ids: List[int]

# Function to connect to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Unable to connect to the database: {e}")

# Function to load the master table
def load_master_table(conn, project_ids: List[int]):
    ids_str = ','.join(map(str, project_ids))
    query = f"""
    SELECT * FROM {SCHEMA_NAME}.project_benchmarks
    WHERE project_id IN ({ids_str})
    """
    
    # Debug: Print the query
    print("Executing Query:", query)
    
    try:
        df = pd.read_sql_query(query, conn)
        print(df.dtypes)
        return df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

def create_normalised_value_table(conn):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.normalisedvalue (
        project_id INTEGER,
        categoryId INTEGER,
        sectionId INTEGER,
        metricId INTEGER,
        platformId INTEGER,
        weights FLOAT,
        benchmarkValue INTEGER,
        actualValue INTEGER,
        normalized INTEGER,
        brandName VARCHAR(255),
        sectionName VARCHAR(255),
        categoryName VARCHAR(255),
        metricname VARCHAR(255),
        platformname VARCHAR(255),
        type VARCHAR(50),      -- New column for type
        
        -- Unique constraint on the combination of these columns
        UNIQUE (
            project_id, categoryId, sectionId, metricId, platformId, 
            weights, benchmarkValue, actualValue, normalized, brandName, 
            sectionName, categoryName, metricname, platformname, type
        )
    )
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(create_table_query)
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Error creating table: {e}")


def insert_normalised_value(conn, df):
    insert_query = f"""
    INSERT INTO {SCHEMA_NAME}.normalisedvalue 
    (project_id, categoryId, sectionId, metricId, platformId, weights, benchmarkValue, actualValue, normalized, brandName, sectionName, categoryName, metricname, platformname, type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (project_id, categoryId, sectionId, metricId, platformId, 
                 weights, benchmarkValue, actualValue, normalized, brandName, 
                 sectionName, categoryName, metricname, platformname, type)
    DO NOTHING
    """
    
    with conn.cursor() as cursor:
        try:
            for _, row in df.iterrows():
                # Ensure each value is a scalar and correctly typed
                row_values = (
                    int(row['project_id']), 
                    int(row['categoryid']), 
                    int(row['sectionid']), 
                    int(row['metricid']), 
                    int(row['platformid']), 
                    float(row['weights']), 
                    int(row['benchmarkValue']), 
                    int(row['actualValue']), 
                    int(row['normalized']),
                    str(row['brandName']),
                    str(row['sectionName']),
                    str(row['categoryName']),
                    str(row['metricname']),
                    str(row['platformname']),
                    str(row['type'])
                )

                # Insert using ON CONFLICT to skip duplicates
                cursor.execute(insert_query, row_values)
                print(f"Inserting row or skipping duplicate: {row_values}")  # Debug: Print row action
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f'Error inserting data: {e}')

def parse_benchmark(df):
    output_data = []

    for _, row in df.iterrows():
        try:
            benchmarks = json.loads(row["benchmarks"])
            print(benchmarks)

            for benchmark in benchmarks:
                category_id = benchmark["categoryId"]
                category_name = benchmark["categoryName"]
                percentile = benchmark.get("percentile", np.nan)
                benchmark_value = benchmark.get("benchmarkValue", benchmark.get("overallValue", np.nan))
                section_name = benchmark["sectionName"]

                # Handle the actualValue field
                actual_value_dict = benchmark.get("actualValue", {})

                if isinstance(actual_value_dict, dict) and actual_value_dict:
                    for brand_name, actual_value in actual_value_dict.items():
                        if actual_value is None or actual_value == "":
                            continue
                        # Determine the type based on isCategory and isOverall
                        if row["isCategory"] == True:
                            type_value = 'category'
                        elif row["isOverall"] == True:
                            type_value = 'overall'
                        else:
                            type_value = 'unknown'  # Default value if neither is True

                        output_row = {
                            "project_id": row["project_id"],
                            "categoryid": category_id,
                            "sectionid": row["sectionId"],
                            "metricid": row["metricId"],
                            "platformid": row["platformId"],
                            "weights": row["weights"],
                            "percentile": percentile,
                            "benchmarkValue": benchmark_value,
                            "actualValue": actual_value,
                            "normalized": None,
                            "brandName": brand_name,
                            "sectionName": section_name,
                            "categoryName": category_name,
                            'metricName':section_name,
                            "platformName":section_name,
                            "type": type_value   # Set the type value
                    
                        }
                        if output_row not in output_data:
                            output_data.append(output_row)
                # else:
                #     continue            
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Error decoding JSON: {e}")

    df_output = pd.DataFrame(output_data)
    print(df_output)
    df_output = df_output.drop_duplicates()
    return df_output

def normalised_value(df):
    df['direct'] = np.nan
    df['indirect'] = np.nan

    direct_condition = df['actualValue'] < df['benchmarkValue']
    max_actual_value = df["actualValue"].max()
    
    df.loc[direct_condition, 'direct'] = df['actualValue'] / df['benchmarkValue'] * 90
    df.loc[df['actualValue'] >= df['benchmarkValue'], 'direct'] = df['actualValue'].apply(
        lambda x: 100 if x == max_actual_value else 90)
    df.loc[df['weights'] == 0, 'direct'] = 0

    indirect_condition = df['actualValue'] >= df['benchmarkValue']
    min_actual_value = df["actualValue"].min()

    df.loc[indirect_condition, 'indirect'] = df['benchmarkValue'] / df['actualValue'] * 90
    df.loc[~indirect_condition, 'indirect'] = df['actualValue'].apply(
        lambda y: 100 if y == min_actual_value else 90)
    df.loc[df['weights'] == 0, 'indirect'] = 0

    df['normalized'] = np.where(df['percentile'] == 0.75, df['direct'], df['indirect'])
    
    df.drop(columns=['direct', 'indirect'], inplace=True)
    df['normalized'] = np.floor(df['normalized']/10)*10

    # Ensure no NaN or infinite values
    df['normalized'] = df['normalized'].replace([np.inf, -np.inf], np.nan).fillna(0)

    return df

@app.post("/normalized_value")
def normalize_benchmarks(payload: RequestPayload):
    conn = connect_to_db()
    
    try:
        # Load the master table for all project_ids
        df = load_master_table(conn, payload.project_ids)
        print(df.dtypes)
        if df is None or df.empty:
            raise HTTPException(status_code=404, detail="No data found for the given project_ids")
        
        # Parse and normalize the benchmarks
        df_output = parse_benchmark(df)
        print(df_output.columns)
        if not all(col in df_output.columns for col in ['actualValue','benchmarkValue','percentile']):
            print("skipping normalisation due to missing required columns.")
            return {"message": "Skipping normalization; required values are missing."}
        else:
            df_output = normalised_value(df_output)
            print(df_output.columns)
        brands_query = f"SELECT name as brand_name, id as brand_ids, category_id FROM {SCHEMA_NAME}.brands"
        categories_query = f"SELECT name as category_name,id FROM {SCHEMA_NAME}.categories"
        metrics_query = f"SELECT name as metricname,id FROM {SCHEMA_NAME}.metrics"
        platforms_query = f"SELECT name as platformname, id FROM {SCHEMA_NAME}.platforms"
     
        # Read the tables into DataFrames
        brands_df = pd.read_sql_query(brands_query, conn)
        print(brands_df)
        print(brands_df.dtypes)
        categories_df = pd.read_sql_query(categories_query, conn)
        print(categories_df)
        metrics_df = pd.read_sql_query(metrics_query, conn)
        platforms_df = pd.read_sql_query(platforms_query, conn)

        # Perform the joins with the existing DataFrame df
        joined_df = df_output.merge(brands_df, left_on='brandName', right_on='brand_name', how='left', suffixes=('', '_brand'))
        columns_to_drop = ['categoryid','category_name']
        joined_df_cat = joined_df.merge(categories_df, left_on='category_id', right_on='id', how='left', suffixes=('', '_category'))
        joined_df_plat = joined_df_cat.merge(platforms_df, left_on='platformid', right_on='id', how='left', suffixes=('', '_platform'))
        joined_df = joined_df_plat.merge(metrics_df, left_on='metricid', right_on='id', how='left', suffixes=('', '_metric'))
        # Rename columns if needed
        joined_df.rename(columns={
           'name_brand': 'brand_name',
            'name_category': 'category_name',
            'name_metric': 'metricname',
            'name_platform': 'platformname'
        }, inplace=True)
    
        print("CHECKKKKKK")
        print(joined_df)

        columns_to_drop = ['categoryid','categoryname','brandName','platformName','metricName','id_platform','id_metric','id','categoryName']
        joined_df = joined_df.drop(columns=columns_to_drop, errors='ignore')
        print(joined_df.dtypes)
        # Ensure correct column names and types
        joined_df = joined_df.rename(columns={
            'benchmarkvalue': 'benchmarkValue',
            'actualvalue': 'actualValue',
            'brand_name': 'brandName',
            'sectionname': 'sectionName',
            'category_name': 'categoryName',
            'category_id': 'categoryid',
            'platformName': 'platformName',
            'metricName':'metricName'
        })
        # Remove duplicate rows if any
        joined_df = joined_df.drop_duplicates()
        print("joined_df")
        print(joined_df)
        create_normalised_value_table(conn)

        #existing_ids = check_existing_records(conn, joined_df)
        insert_normalised_value(conn, joined_df)
        # Clean DataFrame before converting to JSON
        joined_df = joined_df.replace([np.inf, -np.inf], np.nan).fillna(0)
        print(joined_df)
        print(joined_df.dtypes)
        # Convert to dictionary and return as JSON response
        return joined_df.to_dict(orient='records')
    
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    
    finally:
        conn.close()





















# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# import pandas as pd
# import numpy as np
# import psycopg2
# from typing import List
# from fastapi.middleware.cors import CORSMiddleware

# app = FastAPI()

# # CORS configuration
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

# # Request Payload Schema
# class RequestPayload(BaseModel):
#     project_ids: List[int]  # List of project IDs

# # Function to connect to the database
# def connect_to_db():
#     try:
#         conn = psycopg2.connect(**DB_PARAMS)
#         return conn
#     except psycopg2.Error as e:
#         print(f"Unable to connect to the database: {e}")
#         raise HTTPException(status_code=500, detail="Unable to connect to the database")

# # Function to load data based on project_ids
# def load_master_table(conn, project_ids: List[int]) -> pd.DataFrame:
#     query = f"""
#     SELECT * FROM {SCHEMA_NAME}.normalisedvalue
#     WHERE project_id IN %s
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

# # Function to compute final scores and organize data by project_id
# def compute_final_scores(df: pd.DataFrame) -> dict:
#     project_results = {}
#     additional_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']
    
#     if not df.empty:
#         # Group by project_id
#         for project_id, project_group in df.groupby('project_id'):
#             overall_results = pd.DataFrame()

#             # Calculate overall final scores for each brand in the project
#             sum_of_product = project_group.groupby('brandname').apply(calculate_final_score)
#             overall_results = pd.DataFrame({
#                 'Brand_Name': sum_of_product.index,
#                 'Overall_Final_Score': sum_of_product.values
#             })

#             # Calculate section-wise final scores
#             sectionwise = project_group.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
#             for section in sectionwise.columns:
#                 overall_results[section] = sectionwise[section].values

#             # Ensure additional columns exist
#             for col in additional_columns:
#                 if col not in overall_results.columns:
#                     overall_results[col] = 0

#             # Add an id column
#             overall_results['id'] = range(1, len(overall_results) + 1)

#             # Select only the required columns
#             final_result = overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]
            
#             # Store the result grouped by project_id
#             project_results[project_id] = final_result.to_dict(orient="records")
    
#     return project_results

# # FastAPI endpoint to get data
# @app.post("/multiple_get_brand_data")
# def get_data(payload: RequestPayload):
#     conn = connect_to_db()
#     try:
#         # Load data for all brands present in the project_ids
#         df = load_master_table(conn, payload.project_ids)
        
#         if df is not None and not df.empty:
#             # Compute final scores and organize by project_id
#             project_scores = compute_final_scores(df)
#             # Return the data grouped by project_id
#             return {"data": project_scores}
#         else:
#             return {"data": {}}
#     finally:
#         conn.close()

# # Main entry point for testing FastAPI locally
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)