-- models/staging/stg_cad__unique_tags.sql
-- This model cleans and standardizes the unique_tags table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'unique_tags') }}

),

renamed as (

    select
        -- IDs
        id as cad_unique_tag_id,
        ad_accounts_id as ad_account_id,

        -- Tag Information
        text_tag,
        tag_type,
        
        -- Source identifier
        'cad' as source_system,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 