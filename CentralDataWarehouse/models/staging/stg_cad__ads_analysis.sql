-- models/staging/stg_cad__ads_analysis.sql
-- This model cleans and standardizes the ads_analysis table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'ads_analysis') }}

),

renamed as (

    select
        -- IDs
        id as ad_analysis_id,
        ads_meta_id,
        ad_creative_meta_id as ad_creative_id,

        -- Analysis Content
        audio_transcription,
        visual_transcription,
        aspect_ratio,
        production_type,
        concept,
        
        -- Structured Data
        product,
        video_keyframes,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 