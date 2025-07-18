-- models/staging/stg_cad__ad_tags.sql
-- This model cleans and standardizes the ad_tags table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'ad_tags') }}

),

renamed as (

    select
        -- IDs
        id as cad_ad_tag_id,
        unique_tags_id as cad_unique_tag_id,
        ads_meta_id,

        -- Source identifier
        'cad' as source_system,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 