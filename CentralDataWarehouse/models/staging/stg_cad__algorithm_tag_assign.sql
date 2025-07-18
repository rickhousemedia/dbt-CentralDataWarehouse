-- models/staging/stg_cad__algorithm_tag_assign.sql
-- This model cleans and standardizes the algorithm_tag_assign table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'algorithm_tag_assign') }}

),

renamed as (

    select
        -- IDs
        id as cad_algorithm_tag_assign_id,
        unique_tags_id as cad_unique_tag_id,
        cluster_id as cad_tag_cluster_id,
        tag_history_id,

        -- Assignment Information
        probability,
        assigned_at,
        assigned_by,
        
        -- Source identifier
        'cad' as source_system,
        
        -- Load metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 