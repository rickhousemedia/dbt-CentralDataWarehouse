-- models/intermediate/int_review_value_propositions.sql
-- This model normalizes JSON value propositions from customer reviews into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['value_proposition'], 'type': 'btree'},
        {'columns': ['review_date'], 'type': 'btree'}
    ]
) }}

with review_value_props as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        r_value_propositions,
        loaded_at
    from {{ ref('stg_review__reviews') }}
    where r_value_propositions is not null
),

-- Extract value propositions from JSON array
normalized_value_props as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        trim(json_extract_path_text(value_prop.value, 'proposition')) as value_proposition,
        trim(json_extract_path_text(value_prop.value, 'importance')) as importance_score,
        trim(json_extract_path_text(value_prop.value, 'sentiment')) as sentiment,
        loaded_at
    from review_value_props rvp
    cross join json_array_elements(rvp.r_value_propositions) as value_prop
    where trim(json_extract_path_text(value_prop.value, 'proposition')) != ''
),

-- Clean and standardize value propositions
final as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        lower(trim(value_proposition)) as value_proposition,
        case 
            when importance_score ~ '^[0-9]+\.?[0-9]*$' 
            then importance_score::numeric 
            else null 
        end as importance_score,
        lower(trim(sentiment)) as sentiment,
        'review' as source_system,
        loaded_at
    from normalized_value_props
    where value_proposition is not null
      and value_proposition != ''
      and value_proposition != 'null'
)

select * from final 