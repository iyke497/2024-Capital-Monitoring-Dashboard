from datetime import datetime
from sqlalchemy.dialects.sqlite import JSON
from app.database import db

class SurveyResponse(db.Model):
    """Main table for survey responses - Organized by form sections"""
    __tablename__ = 'survey_responses'
    
    # ===== SECTION 1: RESPONSE METADATA =====
    # Survey metadata and tracking
    id = db.Column(db.Integer, primary_key=True)
    public_id = db.Column(db.String(100), unique=True, nullable=False)
    name = db.Column(db.String(200))
    survey_public_id = db.Column(db.String(100))
    survey_name = db.Column(db.String(200))
    survey_type = db.Column(db.String(50))  # To distinguish between survey 1 and 2
    owner_username = db.Column(db.String(100))
    owner_display_name = db.Column(db.String(200))
    organization_name = db.Column(db.String(200))
    created = db.Column(db.DateTime)
    updated = db.Column(db.DateTime)
    is_draft = db.Column(db.Boolean)
    is_report_generated = db.Column(db.Boolean)
    has_submitted_report = db.Column(db.Boolean)
    survey_response_status = db.Column(db.String(50))  # Survey completion status
    is_kobo_response = db.Column(db.Boolean)
    percentage_completed = db.Column(db.Integer)
    
    # ===== SECTION 2: PROJECT BASIC INFORMATION =====
    project_categorisation = db.Column(db.String(100))  # Capital/Constituency Project
    project_name = db.Column(db.Text)
    ergp_code = db.Column(db.String(20))
    parent_ministry = db.Column(db.String(250))
    mda_name = db.Column(db.String(200))
    sub_projects = db.Column(db.Text)  # SUB-PROJECT/ACTIVITY
    strategic_objective = db.Column(db.Text)
    key_performance_indicators = db.Column(db.Text)
    project_type = db.Column(db.String(100))
    project_deliverables = db.Column(db.Text)
    execution_method = db.Column(db.String(200))  # OUTSOURCED/IN HOUSE
    
    # ===== SECTION 3: CONTRACTOR INFORMATION =====
    contractor_rc_numbers = db.Column(db.Text)
    contractor_name = db.Column(db.Text)
    award_certificate = db.Column(JSON, nullable=True)  # CERTIFICATE OF AWARD
    
    # ===== SECTION 4: FINANCIAL INFORMATION =====
    project_appropriation_2024 = db.Column(db.Numeric(20, 2))
    amount_released_2024 = db.Column(db.Numeric(20, 2))
    amount_utilized_2024 = db.Column(db.Numeric(20, 2))
    total_cost_planned = db.Column(db.Numeric(20, 2))
    total_financial_commitment = db.Column(db.Numeric(20, 2))
    
    # ===== SECTION 5: PROJECT STATUS & TIMELINE =====
    project_status = db.Column(db.String(100))  # Actual project status
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    
    # ===== SECTION 6: IMPLEMENTATION PROGRESS =====
    project_achievements = db.Column(db.Text)
    completion_cert_issued = db.Column(db.Boolean)  # JOB COMPLETION CERTIFICATE ISSUED
    job_completion_certificate = db.Column(JSON, nullable=True)  # Certificate file status
    completion_cert_amount = db.Column(db.Numeric(20, 2))  # Amount in completion certificate
    
    # ===== SECTION 7: GEOGRAPHICAL INFORMATION =====
    state = db.Column(db.String(100))
    lga = db.Column(db.String(100))
    ward = db.Column(db.String(100))
    geolocations = db.Column(db.Text)
    
    # ===== SECTION 8: DOCUMENTS & ATTACHMENTS =====
    project_pictures = db.Column(JSON, nullable=True)  # PROJECT PICTURES
    other_documents = db.Column(JSON, nullable=True)  # OTHER RELEVANT DOCUMENTS
    
    # ===== SECTION 9: CHALLENGES & FEEDBACK =====
    challenges_recommendations = db.Column(db.Text)
    
    # ===== SECTION 10: AUDIT & RAW DATA =====
    raw_data = db.Column(db.Text)  # Store original JSON for reference
    
    # Audit timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # ===== NEW FIELD FOR RELATIONSHIP =====
    ministry_agency_id = db.Column(db.Integer, db.ForeignKey('ministry_agencies.id'), nullable=True)
    ministry_agency = db.relationship('MinistryAgency', backref='survey_responses')
    
    # ===== INDEXES FOR PERFORMANCE =====
    __table_args__ = (
        db.Index('idx_survey_type', 'survey_type'),
        db.Index('idx_project_status', 'project_status'),
        db.Index('idx_project_type', 'project_type'),
        db.Index('idx_state_lga', 'state', 'lga'),
        db.Index('idx_created_at', 'created_at'),
        db.Index('idx_ergp_code', 'ergp_code'),
        db.Index('idx_parent_ministry', 'parent_ministry'),
    )
    
    def to_dict(self, include_raw_data=False):
        """Convert model to dictionary for API responses"""
        data = {
            # Section 1: Response Metadata
            'id': self.id,
            'public_id': self.public_id,
            'survey_name': self.survey_name,
            'survey_type': self.survey_type,
            'owner_display_name': self.owner_display_name,
            'organization_name': self.organization_name,
            'created': self.created.isoformat() if self.created else None,
            'updated': self.updated.isoformat() if self.updated else None,
            'survey_response_status': self.survey_response_status,
            'percentage_completed': self.percentage_completed,
            
            # Section 2: Project Basic Information
            'project_categorisation': self.project_categorisation,
            'project_name': self.project_name,
            'ergp_code': self.ergp_code,
            'mda_name': self.mda_name,
            'parent_ministry': self.parent_ministry,
            'sub_projects': self.sub_projects,
            'strategic_objective': self.strategic_objective,
            'key_performance_indicators': self.key_performance_indicators,
            'project_type': self.project_type,
            'project_deliverables': self.project_deliverables,
            'execution_method': self.execution_method,
            
            # Section 3: Contractor Information
            'contractor_rc_numbers': self.contractor_rc_numbers,
            'contractor_name': self.contractor_name,
            'award_certificate': self.award_certificate,
            
            # Section 4: Financial Information
            'project_appropriation_2024': float(self.project_appropriation_2024) if self.project_appropriation_2024 else None,
            'amount_released_2024': float(self.amount_released_2024) if self.amount_released_2024 else None,
            'amount_utilized_2024': float(self.amount_utilized_2024) if self.amount_utilized_2024 else None,
            'total_cost_planned': float(self.total_cost_planned) if self.total_cost_planned else None,
            'total_financial_commitment': float(self.total_financial_commitment) if self.total_financial_commitment else None,
            
            # Section 5: Project Status & Timeline
            'project_status': self.project_status,
            'start_date': self.start_date.isoformat() if self.start_date else None,
            'end_date': self.end_date.isoformat() if self.end_date else None,
            
            # Section 6: Implementation Progress
            'project_achievements': self.project_achievements,
            'completion_cert_issued': self.completion_cert_issued,
            'job_completion_certificate': self.job_completion_certificate,
            'completion_cert_amount': float(self.completion_cert_amount) if self.completion_cert_amount else None,
            
            # Section 7: Geographical Information
            'state': self.state,
            'lga': self.lga,
            'ward': self.ward,
            'geolocations': self.geolocations,
            
            # Section 8: Documents & Attachments
            'project_pictures': self.project_pictures,
            'other_documents': self.other_documents,
            
            # Section 9: Challenges & Feedback
            'challenges_recommendations': self.challenges_recommendations,
            
            # Audit
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }
        
        if include_raw_data:
            data['raw_data'] = self.raw_data
            
        return data
    
    def calculate_financial_metrics(self):
        """Calculate financial metrics for analysis"""
        metrics = {}
        
        if self.project_appropriation_2024:
            # Release rate
            if self.amount_released_2024:
                metrics['release_rate'] = (float(self.amount_released_2024) / float(self.project_appropriation_2024)) * 100
            
            # Utilization rate
            if self.amount_utilized_2024:
                metrics['utilization_rate'] = (float(self.amount_utilized_2024) / float(self.project_appropriation_2024)) * 100
            
            # Utilization efficiency
            if self.amount_released_2024 and self.amount_utilized_2024:
                metrics['utilization_efficiency'] = (float(self.amount_utilized_2024) / float(self.amount_released_2024)) * 100
        
        return metrics

class SurveyMetadata(db.Model):
    """Store metadata about fetched surveys"""
    __tablename__ = 'survey_metadata'
    
    id = db.Column(db.Integer, primary_key=True)
    survey_public_id = db.Column(db.String(100), unique=True, nullable=False)
    survey_name = db.Column(db.String(200))
    survey_type = db.Column(db.String(50))
    total_responses = db.Column(db.Integer)
    last_fetched = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def to_dict(self):
        return {
            'id': self.id,
            'survey_public_id': self.survey_public_id,
            'survey_name': self.survey_name,
            'survey_type': self.survey_type,
            'total_responses': self.total_responses,
            'last_fetched': self.last_fetched.isoformat() if self.last_fetched else None,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }

class BudgetProject2024(db.Model):
    """Stores the approved 2024 budget projects with GIFMIS codes for exact matching."""
    __tablename__ = 'budget_projects_2024'
    
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(50), nullable=False)  # ERGP Code - no longer unique
    
    # Project details
    project_name = db.Column(db.Text)
    status_type = db.Column(db.String(100))
    appropriation = db.Column(db.Numeric(20, 2))
    
    # ===== ENHANCED: GIFMIS CODING SYSTEM =====
    ministry_code = db.Column(db.String(10), nullable=True)
    ministry_name = db.Column(db.String(250))
    
    agency_code = db.Column(db.String(12), nullable=True)    # May be null
    agency_name = db.Column(db.String(250))
    
    # Normalized fields for fuzzy matching (backup)
    agency_normalized = db.Column(db.String(250))
    
    # Audit
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # NEW: Composite unique constraint with agency_code NULL handling
    __table_args__ = (
        # Create a composite unique constraint on code + COALESCE(agency_code, 'NULL')
        db.Index('idx_unique_ergp_agency', 
                 code, 
                 db.func.coalesce(agency_code, 'NULL'),
                 unique=True),
        db.Index('idx_agency_code', 'agency_code'),
        db.Index('idx_ministry_code', 'ministry_code'),
        db.Index('idx_code_ministry', 'code', 'ministry_code'),
    )

class MinistryAgency(db.Model):
    """Normalized reference table for ministries and their agencies from GIFMIS coding system"""
    __tablename__ = 'ministry_agencies'
    
    id = db.Column(db.Integer, primary_key=True)
    
    # ===== GIFMIS CODING SYSTEM =====
    ministry_code = db.Column(db.String(10), nullable=False, index=True)
    agency_code = db.Column(db.String(12), unique=True, nullable=False, index=True)
    
    # ===== OFFICIAL NAMES =====
    agency_name = db.Column(db.String(300), nullable=False)
    ministry_name = db.Column(db.String(300), nullable=False)
    
    # ===== NORMALIZED NAMES FOR MATCHING =====
    agency_name_normalized = db.Column(db.String(300), index=True)
    ministry_name_normalized = db.Column(db.String(300), index=True)
    
    # ===== HIERARCHY INFORMATION =====
    is_self_accounting = db.Column(db.Boolean, default=False)  # Agencies that are also ministries
    is_parastatal = db.Column(db.Boolean, default=False)
    
    # ===== METADATA =====
    is_active = db.Column(db.Boolean, default=True)
    fiscal_year = db.Column(db.String(4), default='2024')  # For versioning by fiscal year
    
    # ===== AUDIT =====
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # ===== INDEXES =====
    __table_args__ = (
        db.Index('idx_ministry_agency', 'ministry_code', 'agency_code'),
        db.Index('idx_agency_name_search', 'agency_name_normalized'),
        db.Index('idx_ministry_search', 'ministry_name_normalized'),
        db.Index('idx_fiscal_year', 'fiscal_year', 'is_active'),
    )
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Auto-generate normalized names if not provided
        if not self.agency_name_normalized and self.agency_name:
            self.agency_name_normalized = self.normalize_name(self.agency_name)
        if not self.ministry_name_normalized and self.ministry_name:
            self.ministry_name_normalized = self.normalize_name(self.ministry_name)
        
        # Auto-detect if agency is self-accounting (ministry equals agency)
        if self.agency_name == self.ministry_name:
            self.is_self_accounting = True
    
    @staticmethod
    def normalize_name(name):
        """
        Even simpler normalization - just uppercase and clean whitespace.
        For cases where conservative normalization fails.
        """
        if not name or not isinstance(name, str):
            return ""
        
        normalized = name.upper().strip()
        
        # Just remove extra spaces and standardize AND
        normalized = normalized.replace('&', ' AND ')
        normalized = ' '.join(normalized.split())
        
        return normalized

    def to_dict(self):
        return {
            'id': self.id,
            'ministry_code': self.ministry_code,
            'agency_code': self.agency_code,
            'agency_name': self.agency_name,
            'ministry_name': self.ministry_name,
            'agency_name_normalized': self.agency_name_normalized,
            'ministry_name_normalized': self.ministry_name_normalized,
            'is_self_accounting': self.is_self_accounting,
            'is_parastatal': self.is_parastatal,
            'is_active': self.is_active,
            'fiscal_year': self.fiscal_year,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }
    
    @classmethod
    def find_agency_by_name(cls, name, threshold=0.85):
        """Fuzzy find agency by name with similarity threshold"""
        from difflib import SequenceMatcher
        
        normalized_search = cls.normalize_name(name)
        
        # First try exact match on normalized name
        exact_match = cls.query.filter(
            cls.agency_name_normalized == normalized_search,
            cls.is_active == True
        ).first()
        
        if exact_match:
            return exact_match
        
        # Fall back to fuzzy matching
        all_agencies = cls.query.filter(cls.is_active == True).all()
        best_match = None
        best_score = 0
        
        for agency in all_agencies:
            score = SequenceMatcher(
                None, 
                normalized_search, 
                agency.agency_name_normalized
            ).ratio()
            
            if score > best_score and score >= threshold:
                best_score = score
                best_match = agency
        
        return best_match
    
    @classmethod
    def get_agencies_by_ministry(cls, ministry_code=None, ministry_name=None):
        """Get all agencies under a ministry"""
        query = cls.query.filter(cls.is_active == True)
        
        if ministry_code:
            query = query.filter(cls.ministry_code == ministry_code)
        elif ministry_name:
            normalized = cls.normalize_name(ministry_name)
            query = query.filter(cls.ministry_name_normalized == normalized)
        
        return query.order_by(cls.agency_name).all()
    
    @classmethod
    def get_ministry_hierarchy(cls):
        """Get hierarchical structure of ministries and their agencies"""
        ministries = {}
        
        # Get all active records
        records = cls.query.filter(cls.is_active == True).order_by(
            cls.ministry_code, cls.agency_code
        ).all()
        
        for record in records:
            ministry_key = f"{record.ministry_code}|{record.ministry_name}"
            
            if ministry_key not in ministries:
                ministries[ministry_key] = {
                    'ministry_code': record.ministry_code,
                    'ministry_name': record.ministry_name,
                    'agencies': []
                }
            
            ministries[ministry_key]['agencies'].append({
                'agency_code': record.agency_code,
                'agency_name': record.agency_name,
                'is_self_accounting': record.is_self_accounting,
                'is_parastatal': record.is_parastatal
            })
        
        return list(ministries.values())
