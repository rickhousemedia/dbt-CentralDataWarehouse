# Staging Layer - Persona Gap Analysis

## Overview

This staging layer unifies data from two separate PostgreSQL databases:
- **CAD Tool (`raw_cad_public`)**: Ad performance, creative analysis, and tagging system
- **Review Tool (`raw_review_public`)**: Customer reviews with persona insights and creative analysis

## Primary Objective

**Identify gaps where certain personas mentioned in reviews aren't well-served by current ads**

## Key Models for Persona Gap Analysis

### 🎯 Critical Models
- **`stg_review__reviews`**: Contains `r_persona_attributes`, `r_value_propositions`, `r_problems`, `r_barriers` from actual customer reviews
- **`stg_cad__creative_analysis`**: Contains `persona_attributes`, `value_propositions`, `problems`, `barriers` detected in existing ads
- **`stg_cad__ads_insights_meta`**: Contains performance metrics (`roas`, `ctr`, `amount_spent`) to identify well-performing ads

### 🔗 Linking Models
- **`stg_cad__ad_accounts`**: Links both sources via `ad_account_id`
- **`stg_cad__ads`** + **`stg_cad__ads_meta`**: Core ad hierarchy for performance analysis

## Data Architecture

### Source Prefixes
- **CAD models**: `stg_cad__*` - Authoritative source for ad performance data
- **Review models**: `stg_review__*` - Authoritative source for customer insights

### Tagging Systems
Both sources maintain independent tagging systems:
- CAD: `stg_cad__unique_tags`, `stg_cad__ad_tags`, `stg_cad__tag_clusters`
- Review: `stg_review__unique_tags`, `stg_review__ad_tags`, `stg_review__tag_clusters`

## Key JSON Fields for Analysis

### From Reviews (`stg_review__reviews`)
- `r_persona_attributes`: Customer personas extracted from reviews
- `r_value_propositions`: What customers value most
- `r_problems`: Problems customers face
- `r_barriers`: Barriers preventing purchase/engagement

### From Ad Creative Analysis (`stg_cad__creative_analysis`)
- `persona_attributes`: Personas targeted by current ads
- `value_propositions`: Value props promoted in ads
- `problems`: Problems addressed by ads
- `barriers`: Barriers tackled by ads

## Analysis Approach

1. **Extract Personas**: Parse JSON fields to identify distinct persona attributes
2. **Performance Mapping**: Link high-performing ads to their targeted personas
3. **Gap Identification**: Find review personas not adequately covered by well-performing ads
4. **Opportunity Scoring**: Prioritize gaps based on review volume and sentiment

## Next Steps

This staging layer feeds into intermediate models that will:
- Normalize JSON persona attributes into structured tables
- Create persona-performance scorecards
- Generate gap analysis reports
- Suggest creative strategies for underserved personas

## Time Partitioning

Models are designed to support monthly and yearly analysis with potential for date-based partitioning in the intermediate layer.

## Usage

```sql
-- Example: Find personas in reviews not covered by high-performing ads
SELECT DISTINCT 
    review_personas.persona_attribute,
    COUNT(r.review_id) as review_count
FROM stg_review__reviews r
CROSS JOIN JSON_EACH(r.r_persona_attributes) as review_personas
WHERE review_personas.persona_attribute NOT IN (
    SELECT DISTINCT ad_personas.persona_attribute
    FROM stg_cad__creative_analysis ca
    JOIN stg_cad__ads_insights_meta ai ON ca.ads_meta_id = ai.ads_meta_id
    CROSS JOIN JSON_EACH(ca.persona_attributes) as ad_personas
    WHERE ai.roas > 2.0  -- High performing ads
)
GROUP BY review_personas.persona_attribute
ORDER BY review_count DESC;
``` 