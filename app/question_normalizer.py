# question_normalizer.py
"""
Utility for normalizing survey question text across different surveys
"""

QUESTION_NORMALIZATION = {
    # Remove year suffixes from financial questions
    "PROJECT APPROPRIATION 2024": "PROJECT APPROPRIATION",
    "AMOUNT RELEASED 2024": "AMOUNT RELEASED",
    "AMOUNT UTILIZED 2024": "AMOUNT UTILIZED",
    "TOTAL COST OF PROJECT PLANNED 2024": "TOTAL COST OF PROJECT PLANNED"
}

STANDARD_QUESTION_FORMS = {
    # Project Details
    "PROJECT NAME": "project_name",
    "NAME OF MDA": "mda_name",
    "SUB-PROJECT/ACTIVITY": "sub_projects",
    "STRATEGIC OBJECTIVES IN ACCORDANCE WITH NDP": "strategic_objective",
    "KEY PERFORMANCE INDICATORS": "key_performance_indicators",
    "PROJECT TYPE": "project_type",
    "PROJECT DELIVERABLES": "project_deliverables",
    "PROJECT EXECUTION": "execution_method",
    "CONTRACTOR RC NUMBERS": "contractor_rc_numbers",
    "CONTRACTOR NAME": "contractor_name",
    "CERTIFICATE OF AWARD": "award_certificate",
    "PROJECT CATEGORIZATION": "project_category",

    # Financial Details
    "PROJECT APPROPRIATION": "appropriation_amount",
    "AMOUNT RELEASED": "amount_released",
    "AMOUNT UTILIZED": "amount_utilized",
    "TOTAL COST OF PROJECT PLANNED": "total_cost_planned",
    "TOTAL FINANCIAL COMMITMENT SINCE INCEPTION": "total_financial_commitment",

    # Media / Documents
    "PROJECT PICTURES": "project_pictures",
    "OTHER RELEVANT DOCUMENTS": "other_documents",

    # Certificates
    "JOB COMPLETION CERTIFICATE ISSUED": "completion_cert_issued",
    "JOB COMPLETION CERTIFICATE": "job_completion_certificate",
    "TOTAL AMOUNT IN APPROVED PROJECT COMPLETION CERTIFICATE": "completion_cert_amount",

    # Implementation Details
    "PROJECT STATUS": "project_status",
    "START DATE": "start_date",
    "END DATE": "end_date",
    "PERCENTAGE COMPLETED": "percentage_completed",
    "LIST PROJECT ACHIEVEMENTS": "project_achievements",
    "GEOLOCATIONS": "geolocations",
    "STATE": "state",
    "LGA": "lga",
    "WARD": "ward",

    # Narrative
    "WHAT ARE THE CHALLENGES AND RECOMMENDATIONS": "challenges_recommendations",
}



def normalize_question_text(question_text: str) -> str:
    """
        Normalize question text to standard form
    
        Args:
            question_text: Original question text from API
        
        Returns:
            Normalized question text
    """
    if not question_text:
        return ""
    
    # Check for direct match
    if question_text in QUESTION_NORMALIZATION:
        return QUESTION_NORMALIZATION[question_text]
    
    # Check for case-insensitive match
    normalized_text = question_text
    for key, value in QUESTION_NORMALIZATION.items():
        if key.lower() == question_text.lower():
            normalized_text = value
            break
    
    return normalized_text


def get_field_name_for_question(question_text: str) -> str:
    """
        Get the database field name for a question
        
        Args:
            question_text: Original question text
            
        Returns:
            Database field name or None if not mapped
    """
    normalized = normalize_question_text(question_text)
    return STANDARD_QUESTION_FORMS.get(normalized)


def extract_answer_by_normalized_text(answers: list, target_field: str):
    """
    Extract answer by normalized question text, safe against malformed data.
    """

    # 1️⃣ Validate answers list
    if not isinstance(answers, list):
        return None

    # 2️⃣ Determine standard text looked up for this field
    standard_text = None
    for text, field in STANDARD_QUESTION_FORMS.items():
        if field == target_field:
            standard_text = text
            break

    if not standard_text:
        return None

    # 3️⃣ Iterate through answers safely
    for answer in answers:
        # Skip non-dicts
        if not isinstance(answer, dict):
            continue

        question_data = answer.get("question") or {}
        if not isinstance(question_data, dict):
            # Some responses have question=None
            continue

        question_text = question_data.get("text") or ""
        question_type = question_data.get("question_type") or ""

        # Normalize defensively: normalize_question_text must accept empty string
        normalized = normalize_question_text(question_text) or ""

        if normalized == standard_text:
            # Special case 1: MDA (which is structured)
            if question_type == "mda":
                verbose_body = answer.get("verbose_body")
                if verbose_body and isinstance(verbose_body, list) and len(verbose_body) > 0:
                    return verbose_body[0].get("name")
            
            # Special case 2: File uploads
            if question_type == "file":
                return answer.get("files")

            # Default case
            return answer.get("body")

    return None
