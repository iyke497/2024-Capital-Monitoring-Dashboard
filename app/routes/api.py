# app/routes/api.py
from flask import Blueprint, jsonify, request, send_file, current_app
from app import db
from ..models import SurveyResponse, SurveyMetadata, MinistryAgency, BudgetProject2024
from ..data_fetcher import DataFetcher
from ..data_cleaner import DataCleaner
from ..analytics import AnalyticsService, ImprovedMinistryAgency, AgencyConsolidationRules
from ..scheduler import get_last_fetch_time, is_fetch_in_progress, get_next_run_time
from app.export_service import ExportService
from datetime import datetime

api_bp = Blueprint("api", __name__)


# Data Fetching Endpoints
@api_bp.post("/fetch/survey1")
def fetch_survey1():
    """
    Manual API endpoint to fetch survey 1 data.
    NOTE: This is now disabled for public use - scheduler handles automatic updates.
    """
    return jsonify({
        "success": False,
        "message": "Manual fetching is disabled. Data is automatically updated every hour by the server.",
    }), 403


@api_bp.post("/fetch/survey2")
def fetch_survey2():
    """
    Manual API endpoint to fetch survey 2 data.
    NOTE: This is now disabled for public use - scheduler handles automatic updates.
    """
    return jsonify({
        "success": False,
        "message": "Manual fetching is disabled. Data is automatically updated every hour by the server.",
    }), 403


@api_bp.post("/fetch/all")
def fetch_all_surveys():
    """
    Manual API endpoint to fetch both surveys.
    NOTE: This is now disabled for public use - scheduler handles automatic updates.
    """
    return jsonify({
        "success": False,
        "message": "Manual fetching is disabled. Data is automatically updated every hour by the server.",
    }), 403


@api_bp.get("/fetch/status")
def get_fetch_status():
    """Get the status of the scheduled fetch operations"""
    last_fetch = get_last_fetch_time()
    next_run = get_next_run_time()
    is_fetching = is_fetch_in_progress()
    
    status = {
        "is_fetching": is_fetching,
        "last_fetch": last_fetch.isoformat() if last_fetch else None,
        "next_scheduled_run": next_run.isoformat() if next_run else None,
    }
    
    return jsonify(status)


@api_bp.route('/stats', methods=['GET'])
def get_stats():
    """Get dashboard statistics"""
    try:        
        total_responses = SurveyResponse.query.count()
        
        # Get unique survey types
        survey_types = db.session.query(
            SurveyResponse.survey_type
        ).distinct().filter(
            SurveyResponse.survey_type.isnot(None)
        ).all()
        survey_types = [st[0] for st in survey_types if st[0]]
        
        return jsonify({
            'total_responses': total_responses,
            'survey_types': survey_types
        })
    
    except Exception as e:
        current_app.logger.error(f"Stats error: {str(e)}")
        return jsonify({
            'error': str(e)
        }), 500


@api_bp.get("/responses")
def get_responses():
    # 1. Capture DataTables specific parameters
    draw = request.args.get("draw", type=int)
    start = request.args.get("start", 0, type=int)
    length = request.args.get("length", 10, type=int)
    search_value = request.args.get("search[value]", "")
    
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

    return jsonify({
        "draw": draw,
        "recordsTotal": records_total,
        "recordsFiltered": records_filtered,
        "data": responses
    })


# Compliance and Metrics Endpoints
@api_bp.get("/compliance/mda")
def get_mda_compliance():
    """Returns MDA-level compliance data"""
    try:
        analytics = AnalyticsService()
        compliance_data = analytics.mda_compliance()
        
        return jsonify({
            "success": True,
            "data": compliance_data,
            "summary": {
                'total_expected': sum(item['expected_projects'] for item in compliance_data),
                'total_reported': sum(item['reported_projects'] for item in compliance_data),
                'overall_compliance': round(
                    (sum(item['reported_projects'] for item in compliance_data) / 
                     sum(item['expected_projects'] for item in compliance_data) * 100)
                    if sum(item['expected_projects'] for item in compliance_data) > 0 else 0,
                    2
                )
            }
        })
    except Exception as e:
        current_app.logger.error(f"MDA compliance error: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500


@api_bp.get("/compliance/mda/<agency_code>/projects")
def get_mda_projects(agency_code):
    """Get project details for a specific MDA"""
    try:
        analytics = AnalyticsService()
        projects = analytics.mda_projects(agency_code)
        
        canonical_code = AgencyConsolidationRules.get_canonical_agency_code(agency_code)
        
        from app.models import MinistryAgency
        agency = MinistryAgency.query.filter_by(
            agency_code=canonical_code,
            is_active=True
        ).first()
        
        if not agency:
            return jsonify({"success": False, "error": f"Agency not found: {agency_code}"}), 404
        
        return jsonify({
            "success": True,
            "data": projects,
            "count": len(projects),
            "agency_info": {
                "agency_code": canonical_code,
                "agency_name": agency.agency_name,
                "ministry_name": agency.ministry_name,
                "is_consolidated": canonical_code != agency_code
            }
        })
    except Exception as e:
        current_app.logger.error(f"MDA projects error: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@api_bp.get("/compliance/ministry")
def get_ministry_compliance():
    """Get ministry-level compliance data"""
    try:
        analytics = AnalyticsService()
        ministry_data = analytics.ministry_compliance()
        
        return jsonify({
            "success": True,
            "data": ministry_data,
            "total_ministries": len(ministry_data)
        })
    except Exception as e:
        current_app.logger.error(f"Ministry compliance error: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500

#@api_bp.get("/compliance/ministry")
def get_ministry_compliance_():
    """Groups individual MDA responses into high-level Ministry buckets"""
    try:
        all_responses = SurveyResponse.query.all()
        ministry_groups = {}

        for resp in all_responses:
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

@api_bp.get("/analytics/dashboard")
def analytics_dashboard():
    svc = AnalyticsService()
    return jsonify(svc.dashboard_overview())


@api_bp.get("/analytics/budget-reporting")
def budget_reporting_overview():
    """Get overview of reported vs unreported 2024 budget projects"""
    try:
        svc = AnalyticsService()
        data = svc.performance.budget_reporting_overview()
        return jsonify({
            "success": True,
            "data": data
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500

# Weekly Activity
@api_bp.get("/analytics/weekly-activity")
def weekly_activity():
    """Get daily response counts for the past 7 days"""
    days = request.args.get('days', default=7, type=int)

    if days not in [7, 30]:
        days = 7

    try:
        svc = AnalyticsService()

        if days == 30:
            data = svc.activity.monthly_activity_summary()

            return jsonify({
                "success": True,
                "data": data
            })
        else:
            data = svc.activity.weekly_activity_summary()
            return jsonify({
                "success": True,
                "data": data
            })
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500

# Ministry Rankings

@api_bp.get("/analytics/ministry-rankings")
def ministry_rankings():
    """Get best and worst performing ministries grouped by parent ministry"""
    try:
        svc = AnalyticsService()
        performance_data = svc.performance.mda_performance_table()
        
        # Group by parent ministry and aggregate scores
        ministry_groups = {}
        
        for mda in performance_data:
            parent = mda.get('parent_ministry', 'OTHER INDEPENDENT AGENCIES')
            
            if parent not in ministry_groups:
                ministry_groups[parent] = {
                    'ministry_name': parent,
                    'total_mdas': 0,
                    'total_expected': 0,
                    'total_reported': 0,
                    'total_responses': 0,
                    'performance_scores': [],
                    'compliance_rates': [],
                }
            
            group = ministry_groups[parent]
            group['total_mdas'] += 1
            group['total_expected'] += mda.get('expected_projects', 0)
            group['total_reported'] += mda.get('reported_projects', 0)
            group['total_responses'] += mda.get('total_responses', 0)
            group['performance_scores'].append(mda.get('performance_index', 0))
            group['compliance_rates'].append(mda.get('compliance_rate_pct', 0))
        
        # Calculate averages and format output
        rankings = []
        for name, stats in ministry_groups.items():
            avg_performance = sum(stats['performance_scores']) / len(stats['performance_scores']) if stats['performance_scores'] else 0
            avg_compliance = sum(stats['compliance_rates']) / len(stats['compliance_rates']) if stats['compliance_rates'] else 0
            
            rankings.append({
                'ministry_name': name,
                'total_mdas': stats['total_mdas'],
                'expected_projects': stats['total_expected'],
                'reported_projects': stats['total_reported'],
                'total_responses': stats['total_responses'],
                'compliance_rate_pct': round(avg_compliance, 2),
                'performance_index': round(avg_performance, 2),
            })
        
        # Sort by performance index
        rankings.sort(key=lambda x: x['performance_index'], reverse=True)
        
        # Get top 10 and bottom 10
        best_10 = rankings[:10]
        worst_10 = list(reversed(rankings[-10:])) if len(rankings) >= 10 else []
        
        return jsonify({
            "success": True,
            "data": {
                "best": best_10,
                "worst": worst_10,
                "total_ministries": len(rankings)
            }
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500


# Export Routes

@api_bp.route('/export/responses', methods=['GET'])
def export_responses():
    """
    Export survey responses to Excel
    
    Query parameters:
        - survey_type: Filter by survey type
        - parent_ministry: Filter by parent ministry
        - state: Filter by state
        - mda_name: Filter by MDA name
        - project_status: Filter by project status
        - start_date: Filter responses created after this date (YYYY-MM-DD)
        - end_date: Filter responses created before this date (YYYY-MM-DD)
        - format: Export format (default: xlsx)
    
    Returns:
        Excel file download
    """
    try:
        # Parse query parameters for filtering
        filters = {}
        
        if request.args.get('survey_type'):
            filters['survey_type'] = request.args.get('survey_type')
        
        if request.args.get('parent_ministry'):
            filters['parent_ministry'] = request.args.get('parent_ministry')
        
        if request.args.get('mda_name'):
            filters['mda_name'] = request.args.get('mda_name')
        
        if request.args.get('state'):
            filters['state'] = request.args.get('state')
        
        if request.args.get('project_status'):
            filters['project_status'] = request.args.get('project_status')
        
        if request.args.get('start_date'):
            try:
                filters['start_date'] = datetime.strptime(
                    request.args.get('start_date'), 
                    '%Y-%m-%d'
                )
            except ValueError:
                return jsonify({
                    'error': 'Invalid start_date format. Use YYYY-MM-DD'
                }), 400
        
        if request.args.get('end_date'):
            try:
                filters['end_date'] = datetime.strptime(
                    request.args.get('end_date'), 
                    '%Y-%m-%d'
                )
            except ValueError:
                return jsonify({
                    'error': 'Invalid end_date format. Use YYYY-MM-DD'
                }), 400
        
        # Generate export
        output, filename = ExportService.export_filtered_responses(filters)
        
        # Return file
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
    
    except Exception as e:
        current_app.logger.error(f"Export error: {str(e)}")
        return jsonify({
            'error': 'Failed to generate export',
            'message': str(e)
        }), 500


@api_bp.route('/export/responses/preview', methods=['GET'])
def export_preview():
    """
    Get a preview of what will be exported (first 10 records)
    Returns JSON with column headers and sample data
    """
    try:
        from app.models import SurveyResponse
        
        # Get first 10 responses
        responses = SurveyResponse.query.order_by(
            SurveyResponse.parent_ministry,
            SurveyResponse.mda_name
        ).limit(10).all()
        
        # Get column headers
        headers = [header for header, _ in ExportService.EXPORT_COLUMNS]
        
        # Format preview data
        preview_data = []
        for response in responses:
            row = {}
            for header, field_name in ExportService.EXPORT_COLUMNS:
                value = getattr(response, field_name, None)
                row[header] = ExportService.format_cell_value(value, field_name)
            preview_data.append(row)
        
        return jsonify({
            'headers': headers,
            'sample_data': preview_data,
            'total_columns': len(headers),
            'sample_rows': len(preview_data)
        })
    
    except Exception as e:
        current_app.logger.error(f"Preview error: {str(e)}")
        return jsonify({
            'error': 'Failed to generate preview',
            'message': str(e)
        }), 500

# Add this endpoint to your routes/api.py for the export count feature (optional)

@api_bp.route('/export/count', methods=['GET'])
def export_count():
    """
    Get count of responses that would be exported with current filters
    This is used for the preview feature in the export modal
    """
    try:        
        query = SurveyResponse.query
        
        # Apply filters
        if request.args.get('parent_ministry'):
            query = query.filter_by(parent_ministry=request.args.get('parent_ministry'))

        if request.args.get('mda_name'):
            query = query.filter_by(mda_name=request.args.get('mda_name'))
        
        if request.args.get('state'):
            query = query.filter_by(state=request.args.get('state'))
        
        if request.args.get('project_status'):
            query = query.filter_by(project_status=request.args.get('project_status'))
        
        if request.args.get('survey_type'):
            query = query.filter_by(survey_type=request.args.get('survey_type'))
        
        if request.args.get('start_date'):
            try:
                start_date = datetime.strptime(
                    request.args.get('start_date'), 
                    '%Y-%m-%d'
                )
                query = query.filter(SurveyResponse.created >= start_date)
            except ValueError:
                pass
        
        if request.args.get('end_date'):
            try:
                end_date = datetime.strptime(
                    request.args.get('end_date'), 
                    '%Y-%m-%d'
                )
                query = query.filter(SurveyResponse.created <= end_date)
            except ValueError:
                pass
        
        count = query.count()
        
        return jsonify({
            'success': True,
            'count': count
        })
    
    except Exception as e:
        current_app.logger.error(f"Export count error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@api_bp.get("/export/filters")
def get_export_filters():
    """Get available filter options for export modal"""
    try:
        # Get unique parent ministries
        ministries = db.session.query(
            SurveyResponse.parent_ministry
        ).distinct().filter(
            SurveyResponse.parent_ministry.isnot(None)
        ).order_by(SurveyResponse.parent_ministry).all()
        ministries = [m[0] for m in ministries]
        
        # Get unique states
        states = db.session.query(
            SurveyResponse.state
        ).distinct().filter(
            SurveyResponse.state.isnot(None)
        ).order_by(SurveyResponse.state).all()
        states = [s[0] for s in states]
        
        # Get unique MDAs
        mdas = db.session.query(
            SurveyResponse.mda_name
        ).distinct().filter(
            SurveyResponse.mda_name.isnot(None)
        ).order_by(SurveyResponse.mda_name).all()
        mdas = [m[0] for m in mdas]
        
        return jsonify({
            'success': True,
            'parent_ministries': ministries,
            'states': states,
            'mdas': mdas
        })
    
    except Exception as e:
        current_app.logger.error(f"Export filters error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    

@api_bp.route('/api/admin/link-responses', methods=['POST'])
def link_survey_responses():
    """Admin endpoint to link survey responses"""
    try:
        force_relink = request.json.get('force_relink', False) if request.json else False
        results = ImprovedMinistryAgency.link_survey_responses(force_relink=force_relink)
        
        return jsonify({
            'success': True,
            'results': {
                'exact_matches': results['linked'],
                'fuzzy_matches': results['fuzzy'],
                'unmatched': results['unmatched'],
                'unmatched_details': results['unmatched_details'][:20]
            }
        })
    except Exception as e:
        current_app.logger.error(f"Link responses error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500