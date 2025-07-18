-- models/staging/stg_cad__ads_creatives_meta.sql
-- This model cleans and standardizes the ads_creatives_meta table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'ads_creatives_meta') }}

),

renamed as (

    select
        -- IDs
        id as ad_creative_id,
        ad_creative_meta_id as meta_ad_creative_id,
        ads_meta_id,
        page_id,

        -- Creative Content
        body,
        headline,
        title,
        
        -- Asset Information
        video_id,
        duration,
        ad_creative_type,
        is_asset,
        
        -- URLs
        s3_image_url,
        s3_video_url,
        meta_image_url,
        meta_video_url,
        website_url,
        thumbnail_url,
        
        -- Technical
        image_hash,
        adcreative,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 