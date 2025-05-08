{{ config(
    materialized='table',
    
) }}

WITH ins AS 
(
    SELECT 
        0 AS industry_type_id, 
        'Yellow' AS industry_type 
    
    UNION ALL 

    SELECT 
        1 AS industry_type_id, 
        'Green' AS industry_type 
    
    UNION ALL 

    SELECT 
        2 AS industry_type_id, 
        'FHV' AS industry_type 
    
    UNION ALL 

    SELECT 
        3 AS industry_type_id, 
        'FHVHV' AS industry_type
    
)
SELECT * FROM ins 
