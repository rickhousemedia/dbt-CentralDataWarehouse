-- models/staging/stg_cad__campaigns_meta.sql
-- This model cleans and standardizes the campaigns_meta table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'campaigns_meta') }}

),

renamed as (

    select
        -- IDs
        id as campaign_id,
        campaign_meta_id as meta_campaign_id,
        ad_accounts_id as ad_account_id,

        -- Campaign Details
        campaign_name,
        special_ad_category,
        status,
        effective_status,
        
        -- Dates
        start_time,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 