"""
Admin routes for managing database entities
Provides CRUD interfaces for MinistryAgency, BudgetProject2024, and SurveyResponse
"""
from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from app.database import db
from app.models import MinistryAgency, BudgetProject2024, SurveyResponse
from sqlalchemy import or_, func
from datetime import datetime

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

# Feature flag for SurveyResponse editing
ALLOW_SURVEY_EDIT = False  # Set to True to enable editing survey responses


# ==================== MINISTRY AGENCY ADMIN ====================

@admin_bp.route('/ministry-agencies')
def ministry_agencies_list():
    """List all ministry agencies with search and pagination"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    search = request.args.get('search', '')
    ministry_filter = request.args.get('ministry', '')
    
    query = MinistryAgency.query
    
    # Apply search filter
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            or_(
                MinistryAgency.agency_name.ilike(search_term),
                MinistryAgency.ministry_name.ilike(search_term),
                MinistryAgency.agency_code.ilike(search_term),
                MinistryAgency.ministry_code.ilike(search_term)
            )
        )
    
    # Apply ministry filter
    if ministry_filter:
        query = query.filter(MinistryAgency.ministry_code == ministry_filter)
    
    # Order by ministry and agency
    query = query.order_by(MinistryAgency.ministry_name, MinistryAgency.agency_name)
    
    # Paginate
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    # Get unique ministries for filter dropdown
    ministries = db.session.query(
        MinistryAgency.ministry_code,
        MinistryAgency.ministry_name
    ).distinct().order_by(MinistryAgency.ministry_name).all()
    
    return render_template('admin/ministry_agencies_list.html',
                         pagination=pagination,
                         search=search,
                         ministry_filter=ministry_filter,
                         ministries=ministries)


@admin_bp.route('/ministry-agencies/new', methods=['GET', 'POST'])
def ministry_agency_create():
    """Create a new ministry agency"""
    if request.method == 'POST':
        try:
            ministry_agency = MinistryAgency(
                ministry_code=request.form.get('ministry_code'),
                agency_code=request.form.get('agency_code'),
                agency_name=request.form.get('agency_name'),
                ministry_name=request.form.get('ministry_name'),
                is_self_accounting=request.form.get('is_self_accounting') == 'on',
                is_parastatal=request.form.get('is_parastatal') == 'on',
                is_active=request.form.get('is_active', 'on') == 'on',
                fiscal_year=request.form.get('fiscal_year', '2024')
            )
            
            db.session.add(ministry_agency)
            db.session.commit()
            
            flash('Ministry Agency created successfully!', 'success')
            return redirect(url_for('admin.ministry_agencies_list'))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Error creating ministry agency: {str(e)}', 'error')
    
    return render_template('admin/ministry_agency_form.html', ministry_agency=None)


@admin_bp.route('/ministry-agencies/<int:id>/edit', methods=['GET', 'POST'])
def ministry_agency_edit(id):
    """Edit an existing ministry agency"""
    ministry_agency = MinistryAgency.query.get_or_404(id)
    
    if request.method == 'POST':
        try:
            ministry_agency.ministry_code = request.form.get('ministry_code')
            ministry_agency.agency_code = request.form.get('agency_code')
            ministry_agency.agency_name = request.form.get('agency_name')
            ministry_agency.ministry_name = request.form.get('ministry_name')
            ministry_agency.is_self_accounting = request.form.get('is_self_accounting') == 'on'
            ministry_agency.is_parastatal = request.form.get('is_parastatal') == 'on'
            ministry_agency.is_active = request.form.get('is_active', 'on') == 'on'
            ministry_agency.fiscal_year = request.form.get('fiscal_year', '2024')
            
            # Normalized names will be auto-generated in __init__
            ministry_agency.agency_name_normalized = MinistryAgency.normalize_name(ministry_agency.agency_name)
            ministry_agency.ministry_name_normalized = MinistryAgency.normalize_name(ministry_agency.ministry_name)
            
            db.session.commit()
            
            flash('Ministry Agency updated successfully!', 'success')
            return redirect(url_for('admin.ministry_agencies_list'))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Error updating ministry agency: {str(e)}', 'error')
    
    return render_template('admin/ministry_agency_form.html', ministry_agency=ministry_agency)


@admin_bp.route('/ministry-agencies/<int:id>/delete', methods=['POST'])
def ministry_agency_delete(id):
    """Delete a ministry agency"""
    try:
        ministry_agency = MinistryAgency.query.get_or_404(id)
        
        # Check if there are related survey responses
        related_count = SurveyResponse.query.filter_by(ministry_agency_id=id).count()
        if related_count > 0:
            flash(f'Cannot delete: {related_count} survey responses are linked to this agency. Set to inactive instead.', 'error')
            return redirect(url_for('admin.ministry_agencies_list'))
        
        db.session.delete(ministry_agency)
        db.session.commit()
        
        flash('Ministry Agency deleted successfully!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting ministry agency: {str(e)}', 'error')
    
    return redirect(url_for('admin.ministry_agencies_list'))


# ==================== BUDGET PROJECT 2024 ADMIN ====================

@admin_bp.route('/budget-projects')
def budget_projects_list():
    """List all budget projects with search and pagination"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    search = request.args.get('search', '')
    ministry_filter = request.args.get('ministry', '')
    status_filter = request.args.get('status', '')
    
    query = BudgetProject2024.query
    
    # Apply search filter
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            or_(
                BudgetProject2024.project_name.ilike(search_term),
                BudgetProject2024.code.ilike(search_term),
                BudgetProject2024.ministry.ilike(search_term),
                BudgetProject2024.agency.ilike(search_term)
            )
        )
    
    # Apply ministry filter
    if ministry_filter:
        query = query.filter(BudgetProject2024.ministry == ministry_filter)
    
    # Apply status filter
    if status_filter:
        query = query.filter(BudgetProject2024.status_type == status_filter)
    
    # Order by code
    query = query.order_by(BudgetProject2024.code)
    
    # Paginate
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    # Get unique values for filters
    ministries = db.session.query(BudgetProject2024.ministry_name).distinct().order_by(BudgetProject2024.ministry_name).all()
    ministries = [m[0] for m in ministries if m[0]]
    
    statuses = db.session.query(BudgetProject2024.status_type).distinct().order_by(BudgetProject2024.status_type).all()
    statuses = [s[0] for s in statuses if s[0]]
    
    return render_template('admin/budget_projects_list.html',
                         pagination=pagination,
                         search=search,
                         ministry_filter=ministry_filter,
                         status_filter=status_filter,
                         ministries=ministries,
                         statuses=statuses)


@admin_bp.route('/budget-projects/new', methods=['GET', 'POST'])
def budget_project_create():
    """Create a new budget project"""
    if request.method == 'POST':
        try:
            budget_project = BudgetProject2024(
                code=request.form.get('code'),
                project_name=request.form.get('project_name'),
                status_type=request.form.get('status_type'),
                appropriation=float(request.form.get('appropriation', 0)) if request.form.get('appropriation') else None,
                ministry=request.form.get('ministry'),
                agency=request.form.get('agency'),
                ministry_code=request.form.get('ministry_code'),
                agency_code=request.form.get('agency_code')
            )
            
            db.session.add(budget_project)
            db.session.commit()
            
            flash('Budget Project created successfully!', 'success')
            return redirect(url_for('admin.budget_projects_list'))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Error creating budget project: {str(e)}', 'error')
    
    # Get ministry agencies for dropdown
    ministry_agencies = MinistryAgency.query.filter_by(is_active=True).order_by(
        MinistryAgency.ministry_name, MinistryAgency.agency_name
    ).all()
    
    return render_template('admin/budget_project_form.html', 
                         budget_project=None,
                         ministry_agencies=ministry_agencies)


@admin_bp.route('/budget-projects/<int:id>/edit', methods=['GET', 'POST'])
def budget_project_edit(id):
    """Edit an existing budget project"""
    budget_project = BudgetProject2024.query.get_or_404(id)
    
    if request.method == 'POST':
        try:
            budget_project.code = request.form.get('code')
            budget_project.project_name = request.form.get('project_name')
            budget_project.status_type = request.form.get('status_type')
            budget_project.appropriation = float(request.form.get('appropriation', 0)) if request.form.get('appropriation') else None
            budget_project.ministry = request.form.get('ministry')
            budget_project.agency = request.form.get('agency')
            budget_project.ministry_code = request.form.get('ministry_code')
            budget_project.agency_code = request.form.get('agency_code')
            
            db.session.commit()
            
            flash('Budget Project updated successfully!', 'success')
            return redirect(url_for('admin.budget_projects_list'))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Error updating budget project: {str(e)}', 'error')
    
    # Get ministry agencies for dropdown
    ministry_agencies = MinistryAgency.query.filter_by(is_active=True).order_by(
        MinistryAgency.ministry_name, MinistryAgency.agency_name
    ).all()
    
    return render_template('admin/budget_project_form.html', 
                         budget_project=budget_project,
                         ministry_agencies=ministry_agencies)


@admin_bp.route('/budget-projects/<int:id>/delete', methods=['POST'])
def budget_project_delete(id):
    """Delete a budget project"""
    try:
        budget_project = BudgetProject2024.query.get_or_404(id)
        db.session.delete(budget_project)
        db.session.commit()
        
        flash('Budget Project deleted successfully!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting budget project: {str(e)}', 'error')
    
    return redirect(url_for('admin.budget_projects_list'))


# ==================== SURVEY RESPONSE ADMIN (READ-ONLY/LIMITED EDIT) ====================

@admin_bp.route('/survey-responses')
def survey_responses_list():
    """List all survey responses (read-only with optional edit)"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    search = request.args.get('search', '')
    survey_type_filter = request.args.get('survey_type', '')
    status_filter = request.args.get('status', '')
    
    query = SurveyResponse.query
    
    # Apply search filter
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            or_(
                SurveyResponse.project_name.ilike(search_term),
                SurveyResponse.ergp_code.ilike(search_term),
                SurveyResponse.mda_name.ilike(search_term),
                SurveyResponse.parent_ministry.ilike(search_term)
            )
        )
    
    # Apply filters
    if survey_type_filter:
        query = query.filter(SurveyResponse.survey_type == survey_type_filter)
    
    if status_filter:
        query = query.filter(SurveyResponse.project_status == status_filter)
    
    # Order by created date descending
    query = query.order_by(SurveyResponse.created_at.desc())
    
    # Paginate
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    # Get unique values for filters
    survey_types = db.session.query(SurveyResponse.survey_type).distinct().all()
    survey_types = [st[0] for st in survey_types if st[0]]
    
    project_statuses = db.session.query(SurveyResponse.project_status).distinct().all()
    project_statuses = [ps[0] for ps in project_statuses if ps[0]]
    
    return render_template('admin/survey_responses_list.html',
                         pagination=pagination,
                         search=search,
                         survey_type_filter=survey_type_filter,
                         status_filter=status_filter,
                         survey_types=survey_types,
                         project_statuses=project_statuses,
                         allow_edit=ALLOW_SURVEY_EDIT)


@admin_bp.route('/survey-responses/<int:id>')
def survey_response_view(id):
    """View a survey response in detail"""
    survey_response = SurveyResponse.query.get_or_404(id)
    return render_template('admin/survey_response_view.html', 
                         survey_response=survey_response,
                         allow_edit=ALLOW_SURVEY_EDIT)


@admin_bp.route('/survey-responses/<int:id>/edit', methods=['GET', 'POST'])
def survey_response_edit(id):
    """Edit limited fields of a survey response (only if flag is enabled)"""
    if not ALLOW_SURVEY_EDIT:
        flash('Survey response editing is currently disabled.', 'error')
        return redirect(url_for('admin.survey_responses_list'))
    
    survey_response = SurveyResponse.query.get_or_404(id)
    
    if request.method == 'POST':
        try:
            # Only allow editing specific fields
            survey_response.project_status = request.form.get('project_status')
            survey_response.project_categorisation = request.form.get('project_categorisation')
            survey_response.execution_method = request.form.get('execution_method')
            survey_response.completion_cert_issued = request.form.get('completion_cert_issued') == 'on'
            survey_response.state = request.form.get('state')
            survey_response.lga = request.form.get('lga')
            survey_response.ward = request.form.get('ward')
            
            # Update ministry agency relationship if changed
            ministry_agency_id = request.form.get('ministry_agency_id')
            if ministry_agency_id:
                survey_response.ministry_agency_id = int(ministry_agency_id)
            
            db.session.commit()
            
            flash('Survey Response updated successfully!', 'success')
            return redirect(url_for('admin.survey_response_view', id=id))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Error updating survey response: {str(e)}', 'error')
    
    # Get ministry agencies for dropdown
    ministry_agencies = MinistryAgency.query.filter_by(is_active=True).order_by(
        MinistryAgency.ministry_name, MinistryAgency.agency_name
    ).all()
    
    return render_template('admin/survey_response_edit.html', 
                         survey_response=survey_response,
                         ministry_agencies=ministry_agencies)


# ==================== API ENDPOINTS FOR AJAX ====================

@admin_bp.route('/api/ministry-agencies/search')
def api_ministry_agencies_search():
    """API endpoint for searching ministry agencies (for autocomplete)"""
    query = request.args.get('q', '')
    limit = request.args.get('limit', 20, type=int)
    
    if not query:
        return jsonify([])
    
    search_term = f"%{query}%"
    results = MinistryAgency.query.filter(
        or_(
            MinistryAgency.agency_name.ilike(search_term),
            MinistryAgency.ministry_name.ilike(search_term)
        ),
        MinistryAgency.is_active == True
    ).limit(limit).all()
    
    return jsonify([{
        'id': ma.id,
        'agency_code': ma.agency_code,
        'ministry_code': ma.ministry_code,
        'agency_name': ma.agency_name,
        'ministry_name': ma.ministry_name
    } for ma in results])


@admin_bp.route('/api/ministry-agencies/<int:id>')
def api_ministry_agency_get(id):
    """API endpoint for getting a single ministry agency"""
    ministry_agency = MinistryAgency.query.get_or_404(id)
    return jsonify(ministry_agency.to_dict())


# ==================== ADMIN HOME ====================

@admin_bp.route('/')
def admin_home():
    """Admin dashboard home"""
    stats = {
        'ministry_agencies': MinistryAgency.query.filter_by(is_active=True).count(),
        'budget_projects': BudgetProject2024.query.count(),
        'survey_responses': SurveyResponse.query.count(),
        'total_appropriation': db.session.query(func.sum(BudgetProject2024.appropriation)).scalar() or 0
    }
    
    return render_template('admin/dashboard.html', stats=stats)
