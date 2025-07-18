-- models/staging/stg_cad__ad_set_meta.sql
-- This model cleans and standardizes the ad_set_meta table from the CAD tool source.

with source as (

    select * from {{ source('raw_cad_public', 'ad_set_meta') }}

),

renamed as (

    select
        -- IDs
        id as ad_set_id,
        meta_ad_set_id,
        campaigns_meta_id as campaign_id,
        ad_accounts_id as ad_account_id,

        -- Ad Set Details
        ad_set_name,
        status,
        effective_status,
        billing_event,
        
        -- Targeting & Config
        source_adset,
        targeting,
        
        -- Dates
        start_time,
        
        -- Metadata
        current_timestamp as loaded_at

    from source

)

select * from renamed 