# Intermediate Models - Persona Gap Analysis

## Overview

This intermediate layer transforms the staged data into structured analytics tables that identify persona gaps - where customer personas mentioned in reviews aren't well-served by current ads.

## Data Flow

```
Staging Layer → JSON Normalization → Unified Analysis → Gap Identification → Actionable Insights
```

## Model Categories

### 1. JSON Normalization Models
**Purpose**: Extract structured data from JSON fields in reviews and ads

#### Review Models (Source: Customer Reviews)
- **`int_review_persona_attributes`**: Persona attributes from customer reviews
- **`int_review_value_propositions`**: Value propositions customers mention
- **`int_review_problems`**: Problems customers face
- **`int_review_barriers`**: Barriers preventing purchase/engagement

#### Ad Models (Source: Creative Analysis)
- **`int_ad_persona_attributes`**: Persona attributes targeted by ads
- **`int_ad_value_propositions`**: Value propositions promoted in ads
- **`int_ad_problems`**: Problems addressed by ads
- **`int_ad_barriers`**: Barriers tackled by ads

### 2. Unified Analysis Models
**Purpose**: Combine and analyze data from both sources

#### `int_unified_persona_attributes`
- Combines persona attributes from both reviews and ads
- Identifies coverage gaps (`review_only`, `ad_only`, `both`)
- Provides comprehensive persona mapping

#### `int_persona_performance_scorecard`
- Links persona attributes to ad performance metrics
- Creates performance tiers (top_performer → poor_performer)
- Calculates composite performance scores
- Identifies gap categories (underperforming_persona, well_served_persona, etc.)

#### `int_persona_gap_analysis` 🎯
- **PRIMARY OUTPUT MODEL**
- Identifies actionable persona gaps
- Provides priority scoring and recommendations
- Focuses on critical and high-priority opportunities

## Key Analysis Dimensions

### Gap Types
- **`completely_unaddressed`**: Persona mentioned in reviews but no ads target it
- **`underperforming`**: Persona targeted by ads but poor performance
- **`moderate_opportunity`**: Persona with decent performance but room for improvement
- **`well_served`**: Persona with strong ad performance

### Priority Levels
- **`critical`**: Completely unaddressed personas with high review mentions
- **`high`**: Underperforming personas with high opportunity
- **`medium`**: Moderate opportunities or underperforming with medium opportunity
- **`low`**: Well-served personas

### Performance Tiers
- **`top_performer`**: High ROAS + High/Medium CTR
- **`strong_performer`**: High ROAS or (Medium ROAS + High CTR)
- **`moderate_performer`**: Medium ROAS
- **`weak_performer`**: Low ROAS
- **`poor_performer`**: Very low ROAS

## Usage Examples

### Find Critical Persona Gaps
```sql
SELECT 
    ad_account_name,
    persona_attribute,
    gap_type,
    review_count,
    recommendation,
    priority_score
FROM {{ ref('int_persona_gap_analysis') }}
WHERE gap_priority = 'critical'
ORDER BY priority_score DESC;
```

### Identify Top Performance Opportunities
```sql
SELECT 
    persona_attribute,
    gap_type,
    review_count,
    ad_count,
    avg_roas,
    opportunity_level,
    recommendation
FROM {{ ref('int_persona_gap_analysis') }}
WHERE gap_priority IN ('critical', 'high')
  AND opportunity_level = 'high_opportunity'
ORDER BY priority_score DESC;
```

### Compare Review vs Ad Personas
```sql
SELECT 
    persona_attribute,
    coverage_type,
    total_review_mentions,
    total_ad_mentions,
    avg_review_confidence,
    avg_ad_confidence
FROM {{ ref('int_unified_persona_attributes') }}
WHERE coverage_type = 'review_only'
  AND total_review_mentions >= 3
ORDER BY total_review_mentions DESC;
```

## Business Value

### For Marketing Teams
- **Identify underserved customer segments**
- **Prioritize new campaign development**
- **Optimize existing ad performance**
- **Align ad messaging with customer needs**

### For Product Teams
- **Understand customer pain points**
- **Validate product-market fit**
- **Identify feature gaps**
- **Inform product roadmap**

### For Analytics Teams
- **Measure persona coverage effectiveness**
- **Track campaign performance by persona**
- **Provide data-driven recommendations**
- **Monitor customer sentiment trends**

## Next Steps

These intermediate models feed into mart models that provide:
- Executive dashboards with persona gap summaries
- Campaign planning tools with opportunity scoring
- Performance monitoring for persona-based campaigns
- Trend analysis for customer persona evolution

## Data Quality Notes

- JSON extraction assumes consistent field naming
- Confidence scores help filter low-quality extractions
- Performance metrics require ad spend > $0 for meaningful ROAS
- Time-based analysis supported through date partitioning
- Cross-source linking depends on `ad_account_id` consistency 