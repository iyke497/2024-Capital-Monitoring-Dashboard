# question_normalizer.py
"""
Utility for normalizing survey question text across different surveys
"""

QUESTION_NORMALIZATION = {
    # ===== SURVEY 1 TO STANDARD =====
    "Project Stautus": "Project Status",

    # Remove question marks from location questions
    "STATE?": "STATE",
    "LGA?": "LGA",
    "WARD?": "WARD",

    # Remove year suffixes from financial questions
    "PROJECT APPROPRIATION 2024": "PROJECT APPROPRIATION",
    "AMOUNT RELEASED 2024": "AMOUNT RELEASED",
    "AMOUNT UTILIZED 2024": "AMOUNT UTILIZED",

    # Standardize case
    "What are the challenges and recommendations": 
        "WHAT ARE THE CHALLENGES AND RECOMMENDATIONS",

    # ===== SURVEY 2 TO STANDARD =====
    "Project Execution": "PROJECT EXECUTION",

    # ===== BIDIRECTIONAL STANDARD FORMS =====
    "Project Status": "Project Status",
    "PROJECT EXECUTION": "PROJECT EXECUTION",
    "STATE": "STATE",
    "LGA": "LGA",
    "WARD": "WARD",
    "PROJECT APPROPRIATION": "PROJECT APPROPRIATION",
    "AMOUNT RELEASED": "AMOUNT RELEASED",
    "AMOUNT UTILIZED": "AMOUNT UTILIZED",
    "WHAT ARE THE CHALLENGES AND RECOMMENDATIONS":
        "WHAT ARE THE CHALLENGES AND RECOMMENDATIONS",
}


STANDARD_QUESTION_FORMS = {
    # Project Details
    "PROJECT NAME": "project_name",
    "Name of MDA": "mda_name",
    "SUB-PROJECT/ACTIVITY": "sub_projects",
    "STRATEGIC OBJECTIVES IN ACCORDANCE WITH NDP": "strategic_objectives",
    "Key Performance Indicators": "kpis",
    "Project Type": "project_type",
    "Project Deliverables": "deliverables",
    "PROJECT EXECUTION": "execution_method",
    "CONTRACTOR RC NUMBERS": "contractor_rc_numbers",
    "CONTRACTOR NAME": "contractor_name",
    "CERTIFICATE OF AWARD": "award_certificate",
    "Project Categorisation": "project_category",

    # Financial Details
    "PROJECT APPROPRIATION": "appropriation_amount",
    "AMOUNT RELEASED": "amount_released",
    "AMOUNT UTILIZED": "amount_utilized",
    "TOTAL COST OF PROJECT PLANNED": "total_planned_cost",
    "TOTAL FINANCIAL COMMITMENT SINCE INCEPTION": "total_financial_commitment",

    # Media / Documents
    "PROJECT PICTURES": "project_pictures",
    "OTHER RELEVANT DOCUMENTS": "other_documents",

    # Certificates
    "JOB COMPLETION CERTIFICATE ISSUED": "completion_certificate_issued",
    "JOB COMPLETION CERTIFICATE": "completion_certificate",
    "TOTAL AMOUNT IN APPROVED PROJECT COMPLETION CERTIFICATE": "completion_certificate_amount",

    # Implementation Details
    "Project Status": "project_status",
    "START DATE": "start_date",
    "END DATE": "end_date",
    "PERCENTAGE COMPLETED %": "percentage_completed",
    "List Project Achievements": "achievements",
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

        # Normalize defensively: normalize_question_text must accept empty string
        normalized = normalize_question_text(question_text) or ""

        if normalized == standard_text:
            return answer.get("body")

    return None
