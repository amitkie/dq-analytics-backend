from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
import psycopg2
from typing import List
import numpy as np
from app.backendApi.config.db import DB_PARAMS
from app.backendApi.routes.brand_sub_image_defination import get_competitors_for_project_get
# Schema name
SCHEMA_NAME = 'public'

app = APIRouter()

# Request models
class MetricReportPayload(BaseModel):
    project_ids: List[int]
    brandname: str

class NormWeightPayload(BaseModel):
    project_ids: List[int]

# Database connection function
def connect_to_db():
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        return None

# Data fetching functions
def fetch_normalized_data(conn, project_ids: List[int]) -> pd.DataFrame:
    if not project_ids:
        raise HTTPException(status_code=400, detail="project_ids must be provided")
    
    project_ids_str = ', '.join(map(str, project_ids))
    query = f'SELECT * FROM {SCHEMA_NAME}."normalisedvalue" WHERE project_id IN ({project_ids_str})'
    
    try:
        return pd.read_sql_query(query, conn)
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the normalized value table: {e}")

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

# Brand info functions
async def get_brand_info(project_id: int, brandname: str):
    try:
        data = await get_competitors_for_project_get(brand_name=brandname, project_id=project_id)
        combined_data = []
        
        main_brand_data = data['main_brand']
        main_brand_data['project_id'] = project_id
        main_brand_data['type'] = 'main_brand'
        combined_data.append(main_brand_data)
        
        competitors_data = data['competitors']
        for competitor in competitors_data:
            competitor['project_id'] = project_id
            competitor['type'] = 'competitor'
            combined_data.append(competitor)
        return pd.DataFrame(combined_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve brand info. Status code: {str(e)}")

async def join_data(normalized_data: pd.DataFrame, project_ids: List[int], brandname: str) -> pd.DataFrame:
    results = []
    
    for project_id in project_ids:
        try:
            brand_info_df = await get_brand_info(project_id, brandname)
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

# Utility functions
def clean_data(results: pd.DataFrame) -> pd.DataFrame:
    results.replace([np.inf, -np.inf], np.nan, inplace=True)
    results.fillna(0, inplace=True)
    return results

def compare_normalized(group):
    max_row = group.loc[group['normalized'].idxmax()]
    min_row = group.loc[group['normalized'].idxmin()]

    group['above'] = f"{max_row['brandname']} - {max_row['normalized']}"
    group['below'] = f"{min_row['brandname']} - {min_row['normalized']}"

    main_brand_rows = group[group['type_y'] == 'main_brand']
    
    if not main_brand_rows.empty:
        main_brand_row = main_brand_rows.iloc[0]
        group['brand'] = f"{main_brand_row['brandname']} - {main_brand_row['normalized']}"
    else:
        group['brand'] = 'No main brand found'

    return group

def compare_normalized_categories(group):
    Above_80 = group.loc[(group['normalized'] > 80) & (group['normalized'] <= 100), ['project_id','project_name','brandname', 'normalized']]
    Above_60 = group.loc[(group['normalized'] > 60) & (group['normalized'] <= 80), ['project_id','project_name','brandname', 'normalized']]
    Above_50 = group.loc[(group['normalized'] > 50) & (group['normalized'] <= 60), ['project_id','project_name','brandname', 'normalized']]
    Above_20 = group.loc[(group['normalized'] > 20) & (group['normalized'] <= 50), ['project_id','project_name','brandname', 'normalized']]
    Below_20 = group.loc[(group['normalized'] > 0) & (group['normalized'] <= 19), ['project_id','project_name','brandname', 'normalized']]

    group["Above81_100"] = ", ".join(Above_80.apply(lambda row: f"{row['project_id']}_{row['project_name']}-{row['brandname']} - {row['normalized']}", axis=1))
    group["Between61_80"] = ", ".join(Above_60.apply(lambda row: f"{row['brandname']} - {row['normalized']}", axis=1))
    group["Between51_60"] = ", ".join(Above_50.apply(lambda row: f"{row['brandname']} - {row['normalized']}", axis=1))
    group["Between20_50"] = ", ".join(Above_20.apply(lambda row: f"{row['brandname']} - {row['normalized']}", axis=1))
    group["Below0_19"] = ", ".join(Below_20.apply(lambda row: f"{row['brandname']} - {row['normalized']}", axis=1))

    return group

def calculation_weight_value(project_df: pd.DataFrame) -> dict:
    metrics = {}
    for (section, platform), group in project_df.groupby(['sectionname', 'platformname']):
        metrics_for_group = {}
        for _, row in group.iterrows():
            metrics_for_group[row['metricname']] = row['weights']
        
        if section not in metrics:
            metrics[section] = {}
        metrics[section][platform] = metrics_for_group
    
    return metrics

def calculation_normalized(project_df: pd.DataFrame) -> pd.DataFrame:
    grouped_data = project_df.groupby(['sectionname','platformname','metricname'], group_keys=False).apply(compare_normalized_categories)
    final_result = grouped_data[['sectionname','platformname','metricname',
                                'Above81_100', 'Between61_80', 
                                'Between51_60', 'Between20_50', 
                                'Below0_19']]
    cleaned_data = clean_data(final_result)
    return cleaned_data

# Endpoints
@app.post("/metric_report")
async def metric_report(payload: MetricReportPayload):
    """Fetch data based on project IDs, and merge with brand data."""
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    normalized_data = fetch_normalized_data(conn, payload.project_ids)
    results = join_data(normalized_data, payload.project_ids, payload.brandname)
    
    above_below = results.groupby(['platformname', 'metricname', 'sectionname'], group_keys=False).apply(compare_normalized)
    above_below = above_below[['project_id','sectionname', 'metricname', 'platformname', 'brandname', 'normalized', 'above', 'below','type_y',"benchmarkvalue","brand"]]
    above_below = above_below[above_below['type_y'] == 'main_brand']
    
    weight_norm = results.groupby(['sectionname', 'metricname', 'platformname'], as_index=False).agg(
        weights_sum=('weights', 'mean'),
        normalized_avg=('normalized', 'mean'),
        benchmarkvalue = ('benchmarkvalue','mean')
    )
    
    final_result = above_below.merge(weight_norm, 
                                   left_on=['sectionname', 'metricname', 'platformname'], 
                                   right_on=['sectionname', 'metricname', 'platformname'], 
                                   how='inner')
    
    return {"data": final_result.to_dict(orient="records")}

@app.post("/platform_report")
async def platform_report(payload: MetricReportPayload):
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    normalized_data = fetch_normalized_data(conn, payload.project_ids)
    results = join_data(normalized_data, payload.project_ids, payload.brandname)
    
    above_below = results.groupby(['sectionname', 'platformname'], group_keys=False).apply(compare_normalized)
    above_below = above_below[['project_id', 'sectionname', 'platformname', 'normalized', 'above', 'below', 'type_y', 'brand']]
    above_below = above_below[above_below['type_y'] == 'main_brand']
    
    weight_unique = results.groupby('platformname')['metricname'].nunique()
    weight_avg = results.groupby(['sectionname', 'platformname'])["weights"].mean()
    weight_sum = weight_unique * weight_avg
    
    weight_norm = results.groupby(['sectionname', 'platformname'], as_index=False).agg(
        normalized_avg=('normalized', 'mean')
    )
    weight_norm['weights_sum'] = weight_sum.reset_index(drop=True)

    final_result = above_below.merge(weight_norm, 
                                   left_on=['sectionname', 'platformname'], 
                                   right_on=['sectionname', 'platformname'], 
                                   how='inner')
    
    final_result = final_result[["project_id", "sectionname", "platformname", "above", "below", "weights_sum", "normalized_avg", "brand"]]
    final_result = final_result.drop_duplicates(subset=["project_id", "sectionname", "platformname", "above", "below", "brand"])
    
    return {"data": final_result.to_dict(orient="records")}

@app.post("/sectional_report")
async def sectional_report(payload: MetricReportPayload):
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    normalized_data = fetch_normalized_data(conn, payload.project_ids)
    results = join_data(normalized_data, payload.project_ids, payload.brandname)

    above_below = results.groupby(['sectionname'], group_keys=False).apply(compare_normalized)
    above_below = above_below[['project_id','sectionname','above', 'below','type_y',"brand"]]
    above_below = above_below[above_below['type_y'] == 'main_brand']
    results = results[results['type_y'] == 'main_brand']
    
    weight_unique = results.groupby('sectionname')['metricname'].count()
    weight_avg = results.groupby('sectionname')["weights"].mean()
    weight_sum = weight_unique * weight_avg
    
    weight_norm = results.groupby(['sectionname'], as_index=False).agg(
        weights_sum=('weights', 'sum'),
        normalized_avg=('normalized', 'mean')
    )
    weight_norm['weights_sum'] = weight_sum.reset_index(drop=True)

    final_result = above_below.merge(weight_norm, 
                                   left_on=['sectionname'], 
                                   right_on=['sectionname'], 
                                   how='inner')
    final_result = final_result.drop_duplicates()

    return {"data": final_result.to_dict(orient="records")}

@app.post("/norm_weight_value")
async def norm_weight_value(payload: NormWeightPayload):
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    results = []
    for project_id in payload.project_ids:
        project_df = fetch_normalized_data(conn, [project_id])
        project_name_df = load_user_projects(conn, project_id)

        if project_df is not None and not project_df.empty and project_name_df is not None and not project_name_df.empty:
            project_merge_df = pd.merge(project_df, project_name_df[['id', 'project_name']], left_on='project_id', right_on='id', how='left')
            project_df = clean_data(project_merge_df)
            metrics = calculation_weight_value(project_df)

            for section, platforms in metrics.items():
                for platform, platform_metrics in platforms.items():
                    results.append({
                        "project_id": project_id,
                        "project_name": project_name_df.iloc[0]['project_name'],
                        "sectionname": section,
                        "platformname": platform,
                        "metrics": platform_metrics
                    })
    
    return {"data": results}

@app.post("/norm_brand_value")
async def norm_brand_value(payload: NormWeightPayload):
    """Fetch normalized data and categorize it based on specific thresholds."""
    conn = connect_to_db()
    if conn is None:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    results = []
    for project_id in payload.project_ids:
        project_df = fetch_normalized_data(conn, [project_id])
        project_name_df = load_user_projects(conn, project_id)

        if project_df is not None and not project_df.empty and project_name_df is not None and not project_name_df.empty:
            project_merge_df = pd.merge(project_df, project_name_df[['id', 'project_name']], 
                                      left_on='project_id', right_on='id', how='left')
            project_df = clean_data(project_merge_df)
            final_result = calculation_normalized(project_df)

            results.append({
                "project_id": project_id,
                "project_name": project_name_df.iloc[0]['project_name'],
                "normalized_data": final_result.to_dict(orient="records")
            })
    
    return {"data": results}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)