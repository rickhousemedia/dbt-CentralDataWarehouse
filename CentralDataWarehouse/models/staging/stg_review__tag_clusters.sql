-- models/staging/stg_review__tag_clusters.sql
-- This model cleans and standardizes the tag_clusters table from the review tool source.

with source as (

    select * from {{ source('raw_review_public', 'tag_clusters') }}

),

renamed as (

    select
        -- IDs
        id as review_tag_cluster_id,
        account_id as ad_account_id,
        cluster_code,

        -- Cluster Information
        cluster_name,
        tag_type,
        cluster_version,
        
        -- Status
        is_active,
        
        -- Metadata
        created_at,
        created_by,
        
        -- Source identifier
        'review' as source_system,
        
        -- Load metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 