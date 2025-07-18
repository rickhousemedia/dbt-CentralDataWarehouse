-- models/staging/stg_review__ad_creative_analysis.sql
-- This model cleans and standardizes the ad_creative_analysis table from the review tool source.
-- This contains creative analysis with persona attributes from the review perspective.

with source as (

    select * from {{ source('raw_review_public', 'ad_creative_analysis') }}

),

renamed as (

    select
        -- IDs
        id as review_creative_analysis_id,
        account_id as ad_account_id,
        ad_id,

        -- Creative Content
        product_name,
        aspect_ratio,
        audio_transcript_text,
        visual_transcript_text,
        video_duration,
        
        -- Structured Analysis (JSON fields)
        audio_transcript_segments,
        visual_transcript_segments,
        visual_elements,
        concept_types,
        production_type,
        
        -- Persona & Hook Analysis
        persona_attributes,
        hook_persona_attributes,
        hook_audio_transcript_text,
        hook_visual_transcript_text,
        hook_visual_elements,
        hook_types,
        hook_visual_types,
        awareness_level,
        
        -- Value Propositions
        value_propositions,
        
        -- Technical/Design
        sound_design,
        audio_profile,
        
        -- URLs
        rh_image_url,
        rh_video_url,
        iconik_url,
        
        -- Metadata
        created_at,
        current_timestamp as loaded_at

    from source

)

select * from renamed 