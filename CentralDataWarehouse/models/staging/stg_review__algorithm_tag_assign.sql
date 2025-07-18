-- models/staging/stg_review__algorithm_tag_assign.sql
-- This model cleans and standardizes the algorithm_tag_assign table from the review tool source.

with source as (

    select * from {{ source('raw_review_public', 'algorithm_tag_assign') }}

),

renamed as (

    select
        -- IDs
        id as review_algorithm_tag_assign_id,
        account_id as ad_account_id,
        tag_id as review_unique_tag_id,
        cluster_id as review_tag_cluster_id,

        -- Tag Information
        tag_type,
        
        -- Assignment Information
        probability,
        assigned_at,
        assigned_by,
        is_active,
        
        -- Source identifier
        'review' as source_system,
        
        -- Load metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 