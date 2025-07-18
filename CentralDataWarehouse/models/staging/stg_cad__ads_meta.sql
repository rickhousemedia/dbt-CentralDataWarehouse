-- models/staging/stg_cad__ads_meta.sql
-- This model cleans and standardizes the ads_meta table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'ads_meta') }}

),

renamed as (

    select
        -- IDs
        id as ads_meta_id,
        ad_id,

        -- Ad Configuration
        link_ad_settings,
        ad_format_asset,
        action_video_type,
        delivery,
        product_id,
        video_asset,
        placement,
        objectives,
        
        -- Status
        status,
        effective_status,
        
        -- Dates
        reporting_starts,
        reporting_ends,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 