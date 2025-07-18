-- models/staging/stg_review__unique_tags.sql
-- This model cleans and standardizes the unique_tags table from the review tool source.

with source as (

    select * from {{ source('raw_review_public', 'unique_tags') }}

),

renamed as (

    select
        -- IDs
        id as review_unique_tag_id,
        account_id as ad_account_id,

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