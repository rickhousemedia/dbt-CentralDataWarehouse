-- models/staging/stg_review__ad_tags.sql
-- This model cleans and standardizes the ad_tags table from the review tool source.

with source as (

    select * from {{ source('raw_review_public', 'ad_tags') }}

),

renamed as (

    select
        -- IDs
        id as review_ad_tag_id,
        account_id as ad_account_id,
        ad_id,
        tag_id as review_unique_tag_id,

        -- Tag Information
        text_tag,
        tag_type,
        
        -- Source identifier
        'review' as source_system,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 