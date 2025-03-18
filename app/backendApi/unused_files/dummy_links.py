

#uvicorn normalisation_api:app --reload --port 8003
uvicorn app.backendApi.normalisation_api:app --reload --port 8003 --done

#https://m594bmgj-8003.inc1.devtunnels.ms/normalized_value
# multiple_get_brand_data (hashed)

uvicorn app.backendApi.DQ_Score_Brand:app --reload --port 8001 --done
#https://m594bmgj-8001.inc1.devtunnels.ms/get_brand_data

uvicorn app.backendApi.DQ_Score:app --reload --port 8000 --done
#https://m594bmgj-8000.inc1.devtunnels.ms/get_data





uvicorn app.backendApi.DQ_Score_multi:app --reload --port 8006 --done
#https://m594bmgj-8006.inc1.devtunnels.ms/get_multi_data




uvicorn app.backendApi.health_card_updated:app --reload --port 8017 --done
# https://m594bmgj-8017.inc1.devtunnels.ms/health_card/



#Brand_image metrics_defination and sub_category combined

uvicorn app.backendApi.brand_sub_image_defination:app --reload --port 8018



uvicorn app.backendApi.category_wise:app --reload --port 8027
#https://m594bmgj-8027.inc1.devtunnels.ms/process_metric/

uvicorn app.backendApi.analytics_kpi_section_updated:app --reload --port 8025
# https://m594bmgj-8025.inc1.devtunnels.ms/analytics_metric/



# uvicorn groupnormalised:app --reload

uvicorn app.backendApi.norm_brand_weight_value:app --reload --port 8034
https://m594bmgj-8034.inc1.devtunnels.ms/
uvicorn app.backendApi.Metric_Plat_Sect_Reports:app --reload --port 8035
http

uvicorn app.backendApi.weightsum:app --reload --port 8031 --done
uvicorn app.backendApi.groupnormalised:app --reload --port 8032 --done
uvicorn app.backendApi.superthemenorm:app --reload --port 8033 --done

























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




uvicorn app.backendApi.dq_norm_combined:app --reload --port 8001 ok
uvicorn app.backendApi.supertheme_combined:app --reload --port 8002  ok

uvicorn app.backendApi.metric_health_combined:app --reload --port 8004 ok

uvicorn app.backendApi.m_p_s_report_norm_brand_weight_combined:app --reload --port 8003 ok

uvicorn app.backendApi.brand_sub_image_defination:app --reload --port 8018 
uvicorn app.backendApi.analytics_kpi_section_updated:app --reload --port 8025
uvicorn app.backendApi.category_wise:app --reload --port 8027 
uvicorn app.backendApi.get_all_brand_data:app --reload --port 8036 