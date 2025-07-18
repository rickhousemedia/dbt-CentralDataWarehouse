-- models/staging/stg_cad__creative_analysis.sql
-- This model cleans and standardizes the creative_analysis table from the CAD tool source.
-- This table contains key persona attributes that will be used for gap analysis.

with source as (

    select * from {{ source('raw_cad_public', 'creative_analysis') }}

),

renamed as (

    select
        -- IDs
        id as cad_creative_analysis_id,
        ad_analysis_id,

        -- Persona & Targeting
        persona_attributes,
        core_persona,
        hook_persona_attributes,
        awareness_level,
        talent_demographics,
        
        -- Value Propositions & Messaging
        value_propositions,
        concept_summary,
        problems,
        barriers,
        strengths,
        weaknesses,
        interesting_details,
        
        -- Creative Structure
        concept_types,
        structure,
        hook_types,
        hook_visual_types,
        hook_visual_description,
        situation,
        
        -- Content Analysis
        visual_transcript_segments,
        audio_transcript_segments,
        visual_elements,
        production_type,
        hook_audio_transcript_text,
        hook_visual_transcript_text,
        hook_visual_elements,
        
        -- Audio/Visual Design
        music,
        sound_design,
        voiceover,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 