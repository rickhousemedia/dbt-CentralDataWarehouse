-- models/staging/stg_cad__tag_clusters.sql
-- This model cleans and standardizes the tag_clusters table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'tag_clusters') }}

),

renamed as (

    select
        -- IDs
        id as cad_tag_cluster_id,
        cluster_code,
        cluster_history_id,

        -- Cluster Information
        cluster_name,
        cluster_description,
        cluster_version,
        
        -- Metadata
        created_at,
        created_by,
        
        -- Source identifier
        'cad' as source_system,
        
        -- Load metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 