-- models/staging/stg_review__reviews.sql
-- This model cleans and standardizes the reviews table from the review tool source.
-- This table contains key persona attributes extracted from customer reviews.

with source as (

    select * from {{ source('raw_review_public', 'reviews') }}

),

renamed as (

    select
        -- IDs
        id as review_id,
        account_id as ad_account_id,

        -- Review Content
        review_content,
        review_author,
        review_date,
        
        -- Persona & Customer Insights (JSON fields)
        r_persona_attributes,
        r_value_propositions,
        r_problems,
        r_barriers,
        
        -- Metadata
        created,
        current_timestamp as loaded_at

    from source

)

select * from renamed 