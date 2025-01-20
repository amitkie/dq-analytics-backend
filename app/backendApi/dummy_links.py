

#uvicorn normalisation_api:app --reload --port 8003
uvicorn app.backendApi.normalisation_api:app --reload --port 8003

#https://m594bmgj-8003.inc1.devtunnels.ms/normalized_value
# multiple_get_brand_data (hashed)

uvicorn app.backendApi.DQ_Score_Brand:app --reload --port 8001
#https://m594bmgj-8001.inc1.devtunnels.ms/get_brand_data

uvicorn app.backendApi.DQ_Score:app --reload --port 8000
#https://m594bmgj-8000.inc1.devtunnels.ms/get_data





uvicorn app.backendApi.DQ_Score_multi:app --reload --port 8006
#https://m594bmgj-8006.inc1.devtunnels.ms/get_multi_data




uvicorn app.backendApi.health_card_updated:app --reload --port 8017
# https://m594bmgj-8017.inc1.devtunnels.ms/health_card/



#Brand_image metrics_defination and sub_category combined

uvicorn app.backendApi.brand_sub_image_defination:app --reload --port 8018
#GET
#https://m594bmgj-8018.inc1.devtunnels.ms/brand-images/Livon
#https://m594bmgj-8018.inc1.devtunnels.ms/brands/Livon
#https://m594bmgj-8018.inc1.devtunnels.ms/definition/?platform_name=Amazon - Display Campaigns&metric_name=ACOS %
#https://m594bmgj-8018.inc1.devtunnels.ms/top_5/
#https://m594bmgj-8018.inc1.devtunnels.ms/brands/competitors             
#https://m594bmgj-8018.inc1.devtunnels.ms/dq_filter_data         
#https://m594bmgj-8018.inc1.devtunnels.ms/brands/Livon/project_id/257

uvicorn app.backendApi.category_wise:app --reload --port 8027
#https://m594bmgj-8027.inc1.devtunnels.ms/process_metric/

uvicorn app.backendApi.analytics_kpi_section_updated:app --reload --port 8025
# https://m594bmgj-8025.inc1.devtunnels.ms/analytics_metric/



# uvicorn groupnormalised:app --reload --port 8031
# #https://m594bmgj-8031.inc1.devtunnels.ms

# uvicorn superthemenorm:app --reload --port 8032
# #https://m594bmgj-8032.inc1.devtunnels.ms

# uvicorn weightsum:app --reload --port 8033
# #https://m594bmgj-8033.inc1.devtunnels.ms/weight_sum




uvicorn norm_brand_weight_value:app --reload --port 8034
https://m594bmgj-8034.inc1.devtunnels.ms/
uvicorn Metric_Plat_Sect_Reports:app --reload --port 8035
https://m594bmgj-8035.inc1.devtunnels.ms/



#

























# #not required for now==============================
# --

# --uvicorn metrics_defination:app --reload --port 8021
# # GET
# # https://m594bmgj-8021.inc1.devtunnels.ms/definition/?platform_name=Amazon - Display Campaigns&metric_name=ACOS %

# --uvicorn Brand_image:app --reload --port 8019
# #GET
# # https://m594bmgj-8019.inc1.devtunnels.ms/brand-images/

# --uvicorn sub_category:app --reload --port 8022
# #https://m594bmgj-8022.inc1.devtunnels.ms/brands/Park Avenue



# uvicorn performing:app --reload --port 8026
# # https://m594bmgj-8026.inc1.devtunnels.ms/top_5/