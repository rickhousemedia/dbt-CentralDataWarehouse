-- models/intermediate/int_review_problems.sql
-- This model normalizes JSON problems from customer reviews into structured rows

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ad_account_id'], 'type': 'btree'},
        {'columns': ['problem'], 'type': 'btree'},
        {'columns': ['review_date'], 'type': 'btree'}
    ]
) }}

with review_problems as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        r_problems,
        loaded_at
    from {{ ref('stg_review__reviews') }}
    where r_problems is not null
),

-- Extract problems from JSON array
normalized_problems as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        problem_text as problem,
        '3' as severity_score,  -- Default severity
        'general' as problem_category,  -- Default category
        loaded_at
    from review_problems rp
    cross join jsonb_array_elements_text(
        case 
            when rp.r_problems is null then '[]'::jsonb
            when jsonb_typeof(rp.r_problems) = 'array' then rp.r_problems
            else '[]'::jsonb
        end
    ) as problem_text
    where problem_text is not null 
      and trim(problem_text) != ''
      and trim(problem_text) != 'null'
),

-- Clean and standardize problems
final as (
    select
        review_id,
        ad_account_id,
        review_date,
        review_author,
        lower(trim(problem)) as problem,
        case 
            when severity_score ~ '^[0-9]+\.?[0-9]*$' 
            then severity_score::numeric 
            else null 
        end as severity_score,
        lower(trim(problem_category)) as problem_category,
        'review' as source_system,
        loaded_at
    from normalized_problems
    where problem is not null
      and problem != ''
      and problem != 'null'
)

select * from final 