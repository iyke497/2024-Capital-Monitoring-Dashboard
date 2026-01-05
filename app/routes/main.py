# app/routes/main.py
from flask import Blueprint, render_template

main_bp = Blueprint("main", __name__)


@main_bp.route("/")
def index():
    """Home page"""
    return render_template("index.html")

@main_bp.route("/compliance")
def compliance_metrics():
    """Renders the compliance table page."""
    return render_template("compliance.html")
