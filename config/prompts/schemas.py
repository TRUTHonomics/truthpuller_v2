# JSON Schemas for LLM Structured Output
# Used with LM Studio's response_format parameter for guaranteed valid JSON
#
# Deze schemas worden gebruikt in:
# - scripts/analyze_posts_phase1.py
# - scripts/analyze_clusters_phase2.py
#
# Documentatie: https://lmstudio.ai/docs/app/api/endpoints/post-chat-completions

# =============================================================================
# Phase 1: Per-Post Analysis Schema
# =============================================================================

POST_ANALYSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "impact_likelihood": {
            "type": "number",
            "description": "Probability that this post moves TRUMP price (0.00-1.00)"
        },
        "suspicion_score": {
            "type": "number",
            "description": "How coded or unusual the post seems (0.00-1.00)"
        },
        "sentiment_intensity": {
            "type": "number",
            "description": "Emotional intensity regardless of direction (0.00-1.00)"
        },
        "news_value": {
            "type": "number",
            "description": "Contains new market-relevant information (0.00-1.00)"
        },
        "mechanism_a_score": {
            "type": "number",
            "description": "Public market reaction probability (0.00-1.00)"
        },
        "mechanism_b_score": {
            "type": "number",
            "description": "Coordinated/insider signal probability (0.00-1.00)"
        },
        "mechanism_c_score": {
            "type": "number",
            "description": "No market impact / noise probability (0.00-1.00)"
        },
        "likely_mechanisms": {
            "type": "string",
            "enum": ["A", "B", "C", "AB"],
            "description": "Dominant mechanism(s)"
        },
        "suspicious_elements": {
            "type": ["string", "null"],
            "description": "Description of suspicious features, or null"
        },
        "detected_numbers": {
            "type": ["string", "null"],
            "description": "Specific numbers/dates/times found, or null"
        },
        "unusual_phrases": {
            "type": ["string", "null"],
            "description": "Unusual word choices or patterns, or null"
        },
        "reasoning": {
            "type": "string",
            "description": "1-3 short sentences explaining the scores"
        },
        "confidence_calibration": {
            "type": "number",
            "description": "Model confidence in its own labels (0.00-1.00)"
        }
    },
    "required": [
        "impact_likelihood",
        "suspicion_score",
        "sentiment_intensity",
        "news_value",
        "mechanism_a_score",
        "mechanism_b_score",
        "mechanism_c_score",
        "likely_mechanisms",
        "reasoning",
        "confidence_calibration"
    ],
    "additionalProperties": False
}


# =============================================================================
# Phase 2: Cluster Comparison Schema
# =============================================================================

# Sub-schema for post rankings
POST_RANKING_SCHEMA = {
    "type": "object",
    "properties": {
        "position": {
            "type": "integer",
            "description": "1-based position in cluster"
        },
        "post_id": {
            "type": "string",
            "description": "Post ID"
        },
        "score": {
            "type": "number",
            "description": "Trigger likelihood score (0.00-1.00)"
        },
        "reason": {
            "type": "string",
            "description": "Brief reason for ranking"
        }
    },
    "required": ["position", "post_id", "score", "reason"]
}


CLUSTER_COMPARISON_SCHEMA = {
    "type": "object",
    "properties": {
        "selected_position": {
            "type": "integer",
            "description": "1-based index of the selected trigger post"
        },
        "selected_post_id": {
            "type": "string",
            "description": "Post ID of the selected trigger"
        },
        "confidence": {
            "type": "number",
            "description": "Confidence in selecting the correct trigger post (0.00-1.00)"
        },
        "rankings": {
            "type": "array",
            "items": POST_RANKING_SCHEMA,
            "description": "All posts ranked from most to least likely trigger"
        },
        "likely_mechanism": {
            "type": "string",
            "enum": ["A", "B", "C"],
            "description": "Most plausible mechanism for the price movement"
        },
        "reasoning": {
            "type": "string",
            "description": "1-3 short sentences explaining the selection"
        },
        "suspicious_elements": {
            "type": ["string", "null"],
            "description": "Notable suspicious patterns, or null"
        },
        "causation_likelihood": {
            "type": "number",
            "description": "Probability that posts caused the price movement (0.00-1.00)"
        },
        "runner_up_position": {
            "type": ["integer", "null"],
            "description": "1-based index of second-best candidate, or null"
        },
        "combined_effect": {
            "type": "boolean",
            "description": "Whether multiple posts together caused the movement"
        }
    },
    "required": [
        "selected_position",
        "selected_post_id",
        "confidence",
        "rankings",
        "likely_mechanism",
        "reasoning",
        "causation_likelihood",
        "combined_effect"
    ],
    "additionalProperties": False
}


# =============================================================================
# Helper Functions
# =============================================================================

def get_response_format(schema_name: str) -> dict:
    """
    Get the response_format dict for LM Studio API.
    
    Args:
        schema_name: "post_analysis" or "cluster_comparison"
        
    Returns:
        Dict ready to use as response_format parameter
        
    Example:
        response_format = get_response_format("post_analysis")
        # Use in API call:
        # {"response_format": response_format}
    """
    schemas = {
        "post_analysis": POST_ANALYSIS_SCHEMA,
        "cluster_comparison": CLUSTER_COMPARISON_SCHEMA
    }
    
    if schema_name not in schemas:
        raise ValueError(f"Unknown schema: {schema_name}. Use: {list(schemas.keys())}")
    
    return {
        "type": "json_schema",
        "json_schema": {
            "name": schema_name,
            "strict": True,
            "schema": schemas[schema_name]
        }
    }


# =============================================================================
# Test / Validation
# =============================================================================

if __name__ == "__main__":
    import json
    
    print("=== Phase 1: Post Analysis Schema ===")
    print(json.dumps(POST_ANALYSIS_SCHEMA, indent=2))
    
    print("\n=== Phase 2: Cluster Comparison Schema ===")
    print(json.dumps(CLUSTER_COMPARISON_SCHEMA, indent=2))
    
    print("\n=== Response Format (post_analysis) ===")
    print(json.dumps(get_response_format("post_analysis"), indent=2))

