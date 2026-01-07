# app/routes/api.py
from flask import Blueprint, jsonify, request
from ..models import SurveyResponse, SurveyMetadata
from ..data_fetcher import DataFetcher, ComplianceMetrics
from ..data_cleaner import DataCleaner

api_bp = Blueprint("api", __name__)


@api_bp.post("/fetch/survey1")
def fetch_survey1():
    """API endpoint to fetch survey 1 data"""
    try:
        count = DataFetcher.fetch_and_store_survey("survey1")
        return jsonify(
            {
                "success": True,
                "message": f"Successfully fetched {count} new responses from Survey 1",
                "count": count,
            }
        )
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "message": f"Error fetching Survey 1: {str(e)}",
                }
            ),
            500,
        )


@api_bp.post("/fetch/survey2")
def fetch_survey2():
    """API endpoint to fetch survey 2 data"""
    try:
        count = DataFetcher.fetch_and_store_survey("survey2")
        return jsonify(
            {
                "success": True,
                "message": f"Successfully fetched {count} new responses from Survey 2",
                "count": count,
            }
        )
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "message": f"Error fetching Survey 2: {str(e)}",
                }
            ),
            500,
        )


@api_bp.post("/fetch/all")
def fetch_all_surveys():
    """API endpoint to fetch both surveys"""
    try:
        count1 = DataFetcher.fetch_and_store_survey("survey1")
        count2 = DataFetcher.fetch_and_store_survey("survey2")

        return jsonify(
            {
                "success": True,
                "message": f"Successfully fetched {count1} from Survey 1 and {count2} from Survey 2",
                "survey1_count": count1,
                "survey2_count": count2,
                "total_count": count1 + count2,
            }
        )
    except Exception as e:
        return (
            jsonify(
                {"success": False, "message": f"Error fetching surveys: {str(e)}"}
            ),
            500,
        )


@api_bp.get("/stats")
def get_stats():
    """Get statistics about stored data"""
    total_responses = SurveyResponse.query.count()
    survey1_count = SurveyResponse.query.filter_by(survey_type="survey1").count()
    survey2_count = SurveyResponse.query.filter_by(survey_type="survey2").count()

    metadata = SurveyMetadata.query.all()

    return jsonify(
        {
            "total_responses": total_responses,
            "survey1_count": survey1_count,
            "survey2_count": survey2_count,
            "metadata": [
                {
                    "survey_name": m.survey_name,
                    "survey_type": m.survey_type,
                    "total_responses": m.total_responses,
                    "last_fetched": m.last_fetched.isoformat()
                    if m.last_fetched
                    else None,
                }
                for m in metadata
            ],
        }
    )


@api_bp.get("/responses")
def get_responses():
    # 1. Capture DataTables specific parameters
    draw = request.args.get("draw", type=int) # Unique ID for each request
    start = request.args.get("start", 0, type=int) # Starting record index
    length = request.args.get("length", 10, type=int) # Number of records per page
    search_value = request.args.get("search[value]", "") # Global search term
    
    # Calculate current page for SQLAlchemy pagination
    page = (start // length) + 1

    query = SurveyResponse.query

    # 2. Add Global Filtering
    if search_value:
        query = query.filter(
            (SurveyResponse.project_name.ilike(f"%{search_value}%")) |
            (SurveyResponse.mda_name.ilike(f"%{search_value}%")) |
            (SurveyResponse.ergp_code.ilike(f"%{search_value}%"))
        )

    # 3. Get total counts for DataTables metadata
    records_total = SurveyResponse.query.count()
    records_filtered = query.count()

    paginated = query.paginate(page=page, per_page=length, error_out=False)

    responses = []
    for item in paginated.items:
        responses.append({
            "project_name": item.project_name,
            "ergp_code": item.ergp_code,
            "mda_name": item.mda_name,
            "survey_type": item.survey_type,
            "percentage_completed": item.percentage_completed,
            "project_appropriation_2024": float(item.project_appropriation_2024) if item.project_appropriation_2024 else 0,
            "amount_released_2024": float(item.amount_released_2024) if item.amount_released_2024 else 0,
            "created": item.created.isoformat() if item.created else None,
        })

    # 4. Return format required by DataTables
    return jsonify({
        "draw": draw,
        "recordsTotal": records_total,
        "recordsFiltered": records_filtered,
        "data": responses
    })

# Compliance and Metrics
@api_bp.get("/compliance/mda")
def get_mda_compliance():
    """Returns MDA-level compliance data for visualization."""
    try:
        compliance_data = ComplianceMetrics.calculate_mda_compliance_data()
        
        return jsonify({
            "success": True,
            "data": compliance_data
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "message": f"Error calculating compliance: {str(e)}"
        }), 500

@api_bp.get("/compliance/ministry")
def get_ministry_compliance():
    """
    Groups individual MDA responses into high-level Ministry buckets 
    using the existing DataCleaner logic.
    """
    try:
        all_responses = SurveyResponse.query.all()
        ministry_groups = {}

        for resp in all_responses:
            # Leverage your existing cleaner logic
            # This ensures the API uses the same fuzzy matching as your ingestion
            _, parent_min = DataCleaner.map_mda_to_ministry(resp.mda_name)
            parent_min = parent_min or "OTHER INDEPENDENT AGENCIES"

            if parent_min not in ministry_groups:
                ministry_groups[parent_min] = {
                    "ministry_name": parent_min,
                    "mda_names": set(),
                    "response_count": 0,
                    "total_budget": 0.0,
                    "completion_scores": []
                }
            
            group = ministry_groups[parent_min]
            group["mda_names"].add(resp.mda_name)
            group["response_count"] += 1
            group["total_budget"] += float(resp.project_appropriation_2024 or 0)
            if resp.percentage_completed:
                group["completion_scores"].append(float(resp.percentage_completed))

        # Format for the Rich Table
        output = []
        for name, stats in ministry_groups.items():
            avg_comp = sum(stats["completion_scores"]) / len(stats["completion_scores"]) if stats["completion_scores"] else 0
            output.append({
                "ministry_name": name,
                "mda_count": len(stats["mda_names"]),
                "total_responses": stats["response_count"],
                "total_budget": stats["total_budget"],
                "avg_completion": round(avg_comp, 2)
            })

        return jsonify({"success": True, "data": output})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500