-- For trips table, I used DBT tool for the transfromation
-- In dbt, I used jinja function for this transformation

{{
    config(
        materialized='incremental'
    )

    unique_key = "trip_id"
}}


{% set cols = ['trip_id', 'vehicle_id', 'customer_id', 'driver_id', 'trip_start_time', 'trip_end_time', 'distance_km', 'last_updated_timestamp'] %}

SELECT 
    {% for col in cols %}
         {{ col }}
         {% if not loop.last %}
         ,
         {% endif %}
         {% endfor %}
from {{source("source_bronze", "trips")}}

{% if is_increamental%}

WHERE last_updated_date > (select colesce(max(last_updated_timestamp), '1900-01-01') from {{ this }}) 

{% endif %}
