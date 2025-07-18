-- models/intermediate/int_review_barriers.sql
-- This model normalizes JSON barriers from customer reviews into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['barrier'], 'type': 'btree'},
        {'columns': ['review_date'], 'type': 'btree'}
    ]
) }}

with review_barriers as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        r_barriers,
        loaded_at
    from {{ ref('stg_review__reviews') }}
    where r_barriers is not null
),

-- Extract barriers from JSON array
normalized_barriers as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        trim(json_extract_path_text(barrier.value, 'barrier')) as barrier,
        trim(json_extract_path_text(barrier.value, 'impact')) as impact_score,
        trim(json_extract_path_text(barrier.value, 'type')) as barrier_type,
        loaded_at
    from review_barriers rb
    cross join json_array_elements(rb.r_barriers) as barrier
    where trim(json_extract_path_text(barrier.value, 'barrier')) != ''
),

-- Clean and standardize barriers
final as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        lower(trim(barrier)) as barrier,
        case 
            when impact_score ~ '^[0-9]+\.?[0-9]*$' 
            then impact_score::numeric 
            else null 
        end as impact_score,
        lower(trim(barrier_type)) as barrier_type,
        'review' as source_system,
        loaded_at
    from normalized_barriers
    where barrier is not null
      and barrier != ''
      and barrier != 'null'
)

select * from final 