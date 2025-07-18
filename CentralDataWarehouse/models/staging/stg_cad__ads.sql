-- models/staging/stg_cad__ads.sql
-- This model cleans and standardizes the ads table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'ads') }}

),

renamed as (

    select
        -- IDs
        id as ad_id,
        ad_meta_id as meta_ad_id,
        ad_accounts_id as ad_account_id,
        ad_set_meta_id as ad_set_id,

        -- Ad Details
        ad_name,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 