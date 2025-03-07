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
# from typing import List

# class RequestPayload(BaseModel):
#     brand_name: str
#     project_ids: List[int]  # Assuming project_ids are integers


# # Function to connect to the database
# def connect_to_db():
#     try:
#         conn = psycopg2.connect(**DB_PARAMS)
#         return conn
#     except psycopg2.Error as e:
#         print(f"Unable to connect to the database: {e}")
#         raise HTTPException(status_code=500, detail="Unable to connect to the database")

# # Function to join tables and get data
# def join_table(conn) -> pd.DataFrame:
#     query = f"""
#     SELECT nv.*, b.name AS brand_name, c.name AS category_name
#     FROM {SCHEMA_NAME}.normalisedvalue nv
#     LEFT JOIN {SCHEMA_NAME}.brands b ON nv.brandName = b.name
#     LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
#     """
#     try:
#         df = pd.read_sql_query(query, conn)
#         print(df)
#         return df
#     except psycopg2.Error as e:
#         raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

# # Function to load data with joins
# # Function to load data with joins
# def load_master_table(conn, brand_name: str, project_ids: List[int]) -> pd.DataFrame:
#     query = f"""
#     SELECT * FROM {SCHEMA_NAME}.normalisedvalue
#     WHERE brandName = %s AND project_id IN %s
#     """
#     try:
#         df = pd.read_sql_query(query, conn, params=(brand_name, tuple(project_ids)))
#         print(df)
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
#     # Initialize a DataFrame to hold the results
#     overall_results = pd.DataFrame()
#     additional_columns = ['Marketplace','Socialwatch', 'Digital Spends', 'Organic Performance']
#     print(additional_columns)
#     if not df.empty:
#         # Calculate overall final scores for each brand
#         sum_of_product = df.groupby('brandname').apply(calculate_final_score)
        
#         # Format the output for overall scores
#         overall_results = pd.DataFrame({
#             'Brand_Name': sum_of_product.index,
#             'Overall_Final_Score': sum_of_product.values
#         })
#         print(overall_results)
#         # Calculate section-wise final scores for overall type
#         sectionwise = df.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
#         for section in sectionwise.columns:
#             overall_results[section] = sectionwise[section].values

#         print(sectionwise)    
#         # Ensure additional columns exist
#         for col in additional_columns:
#             if col not in overall_results.columns:
#                 overall_results[col] = 0

#         # Unique id for each row
#         overall_results['id'] = range(1, len(overall_results) + 1)
#         print(overall_results)

#     # Select only the required columns
#     final_result = overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]

#     return final_result
# # FastAPI endpoint to get data
# @app.post("/get_brand_data")
# def get_data(payload: RequestPayload):
#     conn = connect_to_db()
#     try:
#         df = join_table(conn)
#         df = load_master_table(conn, payload.brand_name, payload.project_ids)
#         if df is not None and not df.empty:
#             # Compute final scores
#             final_scores_df = compute_final_scores(df)
#             # Convert the DataFrame to a dictionary format
#             return {"data": final_scores_df.to_dict(orient="records")}
#         else:
#             return {"data": []}
#     finally:
#         conn.close()

# # Main entry point for testing FastAPI locally
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)


# # # # from fastapi import FastAPI, HTTPException
# # # # from pydantic import BaseModel
# # # # import pandas as pd
# # # # import numpy as np
# # # # import psycopg2
# # # # from typing import List
# # # # from fastapi.middleware.cors import CORSMiddleware

# # # # app = FastAPI()

# # # # # CORS configuration
# # # # origins = [
# # # #     "http://localhost",
# # # #     "http://localhost:3000",
# # # #     "https://example.com",
# # # #     "*",
# # # #     "(*)"
# # # # ]

# # # # app.add_middleware(
# # # #     CORSMiddleware,
# # # #     allow_origins=origins,
# # # #     allow_credentials=True,
# # # #     allow_methods=["*"],
# # # #     allow_headers=["*"],
# # # # )

# # # # # Database connection parameters
# # # # DB_PARAMS = {
# # # #     'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
# # # #     'port': '5434',
# # # #     'dbname': 'KIESQUAREDE',
# # # #     'user': 'KIESQUAREDE',
# # # #     'password': 'KIESQUARE123'
# # # # }
# # # # SCHEMA_NAME = 'public'

# # # # # Request Payload Schema
# # # # class RequestPayload(BaseModel):
# # # #     project_ids: List[int]  # List of project IDs

# # # # # Function to connect to the database
# # # # def connect_to_db():
# # # #     try:
# # # #         conn = psycopg2.connect(**DB_PARAMS)
# # # #         return conn
# # # #     except psycopg2.Error as e:
# # # #         print(f"Unable to connect to the database: {e}")
# # # #         raise HTTPException(status_code=500, detail="Unable to connect to the database")

# # # # # Function to load data based on project_ids
# # # # def load_master_table(conn, project_ids: List[int]) -> pd.DataFrame:
# # # #     query = f"""
# # # #     SELECT * FROM {SCHEMA_NAME}.normalisedvalue
# # # #     WHERE project_id IN %s
# # # #     """
# # # #     try:
# # # #         df = pd.read_sql_query(query, conn, params=(tuple(project_ids),))
# # # #         return df
# # # #     except psycopg2.Error as e:
# # # #         raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

# # # # # Function to calculate final score for each group
# # # # def calculate_final_score(group: pd.DataFrame) -> float:
# # # #     weight_sum = group['weights'].sum()
# # # #     if weight_sum == 0:
# # # #         return np.nan
# # # #     score = (group['weights'] * group['normalized']).sum() / weight_sum
# # # #     return score

# # # # # Function to compute final scores and organize data by project_id
# # # # def compute_final_scores(df: pd.DataFrame) -> dict:
# # # #     project_results = {}
# # # #     additional_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']
    
# # # #     if not df.empty:
# # # #         # Group by project_id
# # # #         for project_id, project_group in df.groupby('project_id'):
# # # #             overall_results = pd.DataFrame()

# # # #             # Calculate overall final scores for each brand in the project
# # # #             sum_of_product = project_group.groupby('brandname').apply(calculate_final_score)
# # # #             overall_results = pd.DataFrame({
# # # #                 'Brand_Name': sum_of_product.index,
# # # #                 'Overall_Final_Score': sum_of_product.values
# # # #             })

# # # #             # Calculate section-wise final scores
# # # #             sectionwise = project_group.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
# # # #             for section in sectionwise.columns:
# # # #                 overall_results[section] = sectionwise[section].values

# # # #             # Ensure additional columns exist
# # # #             for col in additional_columns:
# # # #                 if col not in overall_results.columns:
# # # #                     overall_results[col] = 0

# # # #             # Add an id column
# # # #             overall_results['id'] = range(1, len(overall_results) + 1)

# # # #             # Select only the required columns
# # # #             final_result = overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]
            
# # # #             # Store the result grouped by project_id
# # # #             project_results[project_id] = final_result.to_dict(orient="records")
    
# # # #     return project_results

# # # # # FastAPI endpoint to get data
# # # # @app.post("/multiple_get_brand_data")
# # # # def get_data(payload: RequestPayload):
# # # #     conn = connect_to_db()
# # # #     try:
# # # #         # Load data for all brands present in the project_ids
# # # #         df = load_master_table(conn, payload.project_ids)
        
# # # #         if df is not None and not df.empty:
# # # #             # Compute final scores and organize by project_id
# # # #             project_scores = compute_final_scores(df)
# # # #             # Return the data grouped by project_id
# # # #             return {"data": project_scores}
# # # #         else:
# # # #             return {"data": {}}
# # # #     finally:
# # # #         conn.close()

# # # # # Main entry point for testing FastAPI locally
# # # # if __name__ == "__main__":
# # # #     import uvicorn
# # # #     uvicorn.run(app, host="0.0.0.0", port=8000)


# # # from fastapi import FastAPI, HTTPException
# # # from pydantic import BaseModel
# # # import pandas as pd
# # # import numpy as np
# # # import psycopg2
# # # from typing import List
# # # from fastapi.middleware.cors import CORSMiddleware

# # # app = FastAPI()

# # # # CORS configuration
# # # origins = [
# # #     "http://localhost",
# # #     "http://localhost:3000",
# # #     "https://example.com",
# # #     "*",
# # #     "(*)"
# # # ]

# # # app.add_middleware(
# # #     CORSMiddleware,
# # #     allow_origins=origins,
# # #     allow_credentials=True,
# # #     allow_methods=["*"],
# # #     allow_headers=["*"],
# # # )

# # # # Database connection parameters
# # # DB_PARAMS = {
# # #     'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
# # #     'port': '5434',
# # #     'dbname': 'KIESQUAREDE',
# # #     'user': 'KIESQUAREDE',
# # #     'password': 'KIESQUARE123'
# # # }
# # # SCHEMA_NAME = 'public'

# # # # Request Payload Schema
# # # class RequestPayload(BaseModel):
# # #     project_ids: List[int]  # List of project IDs

# # # # Function to connect to the database
# # # def connect_to_db():
# # #     try:
# # #         conn = psycopg2.connect(**DB_PARAMS)
# # #         return conn
# # #     except psycopg2.Error as e:
# # #         print(f"Unable to connect to the database: {e}")
# # #         raise HTTPException(status_code=500, detail="Unable to connect to the database")

# # # # Function to load data based on project_ids
# # # def load_master_table(conn, project_ids: List[int]) -> pd.DataFrame:
# # #     query = f"""
# # #     SELECT * FROM {SCHEMA_NAME}.normalisedvalue
# # #     WHERE project_id IN %s
# # #     """
# # #     try:
# # #         df = pd.read_sql_query(query, conn, params=(tuple(project_ids),))
# # #         return df
# # #     except psycopg2.Error as e:
# # #         raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

# # # # Function to calculate final score for each group
# # # def calculate_final_score(group: pd.DataFrame) -> float:
# # #     weight_sum = group['weights'].sum()
# # #     if weight_sum == 0:
# # #         return np.nan
# # #     score = (group['weights'] * group['normalized']).sum() / weight_sum
# # #     return score

# # # # Function to compute final scores and organize data by project_id
# # # def compute_final_scores(df: pd.DataFrame) -> dict:
# # #     project_results = {}
# # #     additional_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']
    
# # #     if not df.empty:
# # #         # Group by project_id
# # #         for project_id, project_group in df.groupby('project_id'):
# # #             overall_results = pd.DataFrame()

# # #             # Calculate overall final scores for each brand in the project
# # #             sum_of_product = project_group.groupby('brandname').apply(calculate_final_score)
# # #             overall_results = pd.DataFrame({
# # #                 'Brand_Name': sum_of_product.index,
# # #                 'Overall_Final_Score': sum_of_product.values
# # #             })

# # #             # Calculate section-wise final scores
# # #             sectionwise = project_group.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
# # #             for section in sectionwise.columns:
# # #                 overall_results[section] = sectionwise[section].values

# # #             # Ensure additional columns exist
# # #             for col in additional_columns:
# # #                 if col not in overall_results.columns:
# # #                     overall_results[col] = 0

# # #             # Add an id column
# # #             overall_results['id'] = range(1, len(overall_results) + 1)

# # #             # Select only the required columns
# # #             final_result = overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]
            
# # #             # Store the result grouped by project_id
# # #             project_results[project_id] = final_result.to_dict(orient="records")
    
# # #     return project_results

# # # # FastAPI endpoint to get data
# # # @app.post("/multiple_get_brand_data")
# # # def get_data(payload: RequestPayload):
# # #     conn = connect_to_db()
# # #     try:
# # #         # Load data for all brands present in the project_ids
# # #         df = load_master_table(conn, payload.project_ids)
        
# # #         if df is not None and not df.empty:
# # #             # Compute final scores and organize by project_id
# # #             project_scores = compute_final_scores(df)
# # #             # Return the data grouped by project_id
# # #             return {"data": project_scores}
# # #         else:
# # #             return {"data": {}}
# # #     finally:
# # #         conn.close()

# # # # Main entry point for testing FastAPI locally
# # # if __name__ == "__main__":
# # #     import uvicorn
# # #     uvicorn.run(app, host="0.0.0.0", port=8000)




# # from fastapi import FastAPI, HTTPException
# # from pydantic import BaseModel
# # import pandas as pd
# # import numpy as np
# # import psycopg2
# # from typing import List
# # from fastapi.middleware.cors import CORSMiddleware

# # app = FastAPI()

# # origins = [
# #     "http://localhost",
# #     "http://localhost:3000",
# #     "https://example.com",
# #     "*",
# #     "(*)"
# # ]

# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=origins,
# #     allow_credentials=True,
# #     allow_methods=["*"],
# #     allow_headers=["*"],
# # )

# # # Database connection parameters
# # DB_PARAMS = {
# #     'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
# #     'port': '5434',
# #     'dbname': 'KIESQUAREDE',
# #     'user': 'KIESQUAREDE',
# #     'password': 'KIESQUARE123'
# # }
# # SCHEMA_NAME = 'public'
# # from typing import List

# # class RequestPayload(BaseModel):
# #     brand_name: str
# #     project_ids: List[int]  # Assuming project_ids are integers


# # # Function to connect to the database
# # def connect_to_db():
# #     try:
# #         conn = psycopg2.connect(**DB_PARAMS)
# #         return conn
# #     except psycopg2.Error as e:
# #         print(f"Unable to connect to the database: {e}")
# #         raise HTTPException(status_code=500, detail="Unable to connect to the database")

# # # Function to join tables and get data
# # def join_table(conn) -> pd.DataFrame:
# #     query = f"""
# #     SELECT nv.*, b.name AS brand_name, c.name AS category_name
# #     FROM {SCHEMA_NAME}.normalisedvalue nv
# #     LEFT JOIN {SCHEMA_NAME}.brands b ON nv.brandName = b.name
# #     LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
# #     """
# #     try:
# #         df = pd.read_sql_query(query, conn)
# #         print(df)
# #         return df
# #     except psycopg2.Error as e:
# #         raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

# # # Function to load data with joins
# # # Function to load data with joins
# # def load_master_table(conn, brand_name: str, project_ids: List[int]) -> pd.DataFrame:
# #     query = f"""
# #     SELECT * FROM {SCHEMA_NAME}.normalisedvalue
# #     WHERE brandName = %s AND project_id IN %s
# #     """
# #     try:
# #         df = pd.read_sql_query(query, conn, params=(brand_name, tuple(project_ids)))
# #         print(df)
# #         return df
# #     except psycopg2.Error as e:
# #         raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

# # # Function to calculate final score for each group
# # def calculate_final_score(group: pd.DataFrame) -> float:
# #     weight_sum = group['weights'].sum()
# #     if weight_sum == 0:
# #         return np.nan
# #     score = (group['weights'] * group['normalized']).sum() / weight_sum
# #     return score

# # def compute_final_scores(df: pd.DataFrame) -> pd.DataFrame:
# #     # Initialize a DataFrame to hold the results
# #     overall_results = pd.DataFrame()
# #     additional_columns = ['Marketplace','Socialwatch', 'Digital Spends', 'Organic Performance']
# #     print(additional_columns)
# #     if not df.empty:
# #         # Calculate overall final scores for each brand
# #         sum_of_product = df.groupby('brandname').apply(calculate_final_score)
        
# #         # Format the output for overall scores
# #         overall_results = pd.DataFrame({
# #             'Brand_Name': sum_of_product.index,
# #             'Overall_Final_Score': sum_of_product.values
# #         })
# #         print(overall_results)
# #         # Calculate section-wise final scores for overall type
# #         sectionwise = df.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
# #         for section in sectionwise.columns:
# #             overall_results[section] = sectionwise[section].values

# #         print(sectionwise)    
# #         # Ensure additional columns exist
# #         for col in additional_columns:
# #             if col not in overall_results.columns:
# #                 overall_results[col] = 0

# #         # Unique id for each row
# #         overall_results['id'] = range(1, len(overall_results) + 1)
# #         print(overall_results)

# #     # Select only the required columns
# #     final_result = overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]

# #     return final_result
# # # FastAPI endpoint to get data
# # @app.post("/get_brand_data")
# # def get_data(payload: RequestPayload):
# #     conn = connect_to_db()
# #     try:
# #         df = join_table(conn)
# #         df = load_master_table(conn, payload.brand_name, payload.project_ids)
# #         if df is not None and not df.empty:
# #             # Compute final scores
# #             final_scores_df = compute_final_scores(df)
# #             # Convert the DataFrame to a dictionary format
# #             return {"data": final_scores_df.to_dict(orient="records")}
# #         else:
# #             return {"data": []}
# #     finally:
# #         conn.close()

# # # Main entry point for testing FastAPI locally
# # if __name__ == "__main__":
# #     import uvicorn
# #     uvicorn.run(app, host="0.0.0.0", port=8000)
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
#     brand_name: str
#     project_ids: List[int]  # Assuming project_ids are integers

# # Function to connect to the database
# def connect_to_db():
#     try:
#         conn = psycopg2.connect(**DB_PARAMS)
#         return conn
#     except psycopg2.Error as e:
#         print(f"Unable to connect to the database: {e}")
#         raise HTTPException(status_code=500, detail="Unable to connect to the database")

# # Function to get category_name for the given brand
# def get_category_name(conn, brand_name: str) -> str:
#     query = f"""
#     SELECT c.name
#     FROM {SCHEMA_NAME}.brands b
#     LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
#     WHERE b.name = %s
#     """
#     try:
#         result = pd.read_sql_query(query, conn, params=(brand_name,))
#         if result.empty:
#             raise HTTPException(status_code=404, detail="Brand not found")
#         return result.iloc[0]["name"]
#     except psycopg2.Error as e:
#         raise HTTPException(status_code=500, detail=f"Error querying the category name: {e}")

# # Function to load data for the specified brand and its competition brands in the same category
# def load_master_table(conn, brand_name: str, project_ids: List[int]) -> pd.DataFrame:
#     category_name = get_category_name(conn, brand_name)
    
#     query = f"""
#     SELECT nv.*, b.name AS brand_name, c.name AS category_name
#     FROM {SCHEMA_NAME}.normalisedvalue nv
#     LEFT JOIN {SCHEMA_NAME}.brands b ON nv.brandName = b.name
#     LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
#     WHERE (b.name = %s OR c.name = %s) AND nv.project_id IN %s
#     """
#     try:
#         df = pd.read_sql_query(query, conn, params=(brand_name, category_name, tuple(project_ids)))
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
#     additional_columns = ['Marketplace','Socialwatch', 'Digital Spends', 'Organic Performance']

#     if not df.empty:
#         sum_of_product = df.groupby('brandname').apply(calculate_final_score)
#         overall_results = pd.DataFrame({
#             'Brand_Name': sum_of_product.index,
#             'Overall_Final_Score': sum_of_product.values
#         })

#         sectionwise = df.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
#         for section in sectionwise.columns:
#             overall_results[section] = sectionwise[section].values

#         for col in additional_columns:
#             if col not in overall_results.columns:
#                 overall_results[col] = 0

#         overall_results['id'] = range(1, len(overall_results) + 1)

#     final_result = overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]
#     return final_result

# # FastAPI endpoint to get data
# @app.post("/get_brand_data")
# def get_data(payload: RequestPayload):
#     conn = connect_to_db()
#     try:
#         df = load_master_table(conn, payload.brand_name, payload.project_ids)
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
from typing import List, Dict, Any
from fastapi.middleware.cors import CORSMiddleware
from app.backendApi.auth_middleware import BearerTokenMiddleware

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

class RequestPayload(BaseModel):
    brand_name: str
    project_ids: List[int]  # Assuming project_ids are integers

# Function to connect to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        raise HTTPException(status_code=500, detail="Unable to connect to the database")

# Function to get category_name for the given brand
def get_category_name(conn, brand_name: str) -> str:
    query = f"""
    SELECT c.name
    FROM {SCHEMA_NAME}.brands b
    LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
    WHERE b.name = %s
    """
    try:
        result = pd.read_sql_query(query, conn, params=(brand_name,))
        if result.empty:
            raise HTTPException(status_code=404, detail="Brand not found")
        return result.iloc[0]["name"]
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the category name: {e}")

# Function to load data for the specified brand and its competition brands in the same category
def load_master_table(conn, brand_name: str, project_ids: List[int]) -> pd.DataFrame:
    category_name = get_category_name(conn, brand_name)
    
    query = f"""
    SELECT nv.*, b.name AS brand_name, c.name AS category_name
    FROM {SCHEMA_NAME}.normalisedvalue nv
    LEFT JOIN {SCHEMA_NAME}.brands b ON nv.brandName = b.name
    LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
    WHERE (b.name = %s OR c.name = %s) AND nv.project_id IN %s
    """
    try:
        df = pd.read_sql_query(query, conn, params=(brand_name, category_name, tuple(project_ids)))
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

def compute_final_scores(df: pd.DataFrame) -> pd.DataFrame:
    overall_results = pd.DataFrame()
    additional_columns = ['Marketplace','Socialwatch', 'Digital Spends', 'Organic Performance']

    if not df.empty:
        sum_of_product = df.groupby('brandname').apply(calculate_final_score)
        overall_results = pd.DataFrame({
            'Brand_Name': sum_of_product.index,
            'Overall_Final_Score': sum_of_product.values
        })

        sectionwise = df.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
        for section in sectionwise.columns:
            overall_results[section] = sectionwise[section].values

        for col in additional_columns:
            if col not in overall_results.columns:
                overall_results[col] = 0

        overall_results['id'] = range(1, len(overall_results) + 1)

    final_result = overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]
    return final_result

# Function to compute statistical metrics
def compute_statistical_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute statistical metrics for score values
    
    Args:
        df (pd.DataFrame): DataFrame containing brand scores
    
    Returns:
        Dict with statistical metrics
    """
    if df.empty:
        return {}
    
    # Compute statistical metrics for Overall Final Score
    score_stats = {
        
        '50th_percentile': df['Overall_Final_Score'].quantile(0.50),
        '75th_percentile': df['Overall_Final_Score'].quantile(0.75),
        
    }
    
    # Compute section-wise statistics
    section_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']
    section_stats = {}
    
    for section in section_columns:
        if section in df.columns:
            section_stats[section] = {
               
                '50th_percentile': df[section].quantile(0.50),
                '75th_percentile': df[section].quantile(0.75)
                
            }
    
    return {
        'overall_score_stats': score_stats,
        'section_stats': section_stats
    }

# FastAPI endpoint to get data

# def get_data(payload: RequestPayload):
#     conn = connect_to_db()
#     try:
#         df = load_master_table(conn, payload.brand_name, payload.project_ids)
#         if df is not None and not df.empty:
#             final_scores_df = compute_final_scores(df)
#             statistical_metrics = compute_statistical_metrics(final_scores_df)
            
#             return {
#                 "data": final_scores_df.to_dict(orient="records"),
#                 "statistics": statistical_metrics
#             }
#         else:
#             return {"data": [], "statistics": {}}
#     finally:
#         conn.close()
@app.post("/get_brand_data")
def get_data(payload: RequestPayload):
    conn = connect_to_db()
    try:
        # Load the master table for the given brand and project ids
        df = load_master_table(conn, payload.brand_name, payload.project_ids)
        
        # Initialize the response data
        response_data = {
            "data": [],
            "statistics": {}
            
        }
        
        # If the data is not empty, compute final scores and statistics
        if df is not None and not df.empty:
            # Calculate final scores for all brands
            final_scores_df = compute_final_scores(df)
            
            # Compute the statistics for the final scores and section scores
            statistical_metrics = compute_statistical_metrics(final_scores_df)
            
            # Store the final scores and statistics
            response_data["information"] = final_scores_df.to_dict(orient="records")
            response_data["statistics"] = statistical_metrics
            
            # Now, get the data specific to the requested brand
            brand_data = final_scores_df[final_scores_df['Brand_Name'] == payload.brand_name]
            
            if not brand_data.empty:
                # If data for the brand is found, convert it to a dictionary
                # response_data["data"] = brand_data.to_dict(orient="records")[0]
                response_data["data"] = brand_data.to_dict(orient="records")
        
        return response_data
    
    finally:
        conn.close()

# Main entry point for testing FastAPI locally
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)