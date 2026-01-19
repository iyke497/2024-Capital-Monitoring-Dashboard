# app/__init__.py
from flask import Flask
from .config import Config
from .database import db
from .routes.main import main_bp
from .routes.api import api_bp
from .scheduler import init_scheduler
import sys
import os


def create_app(config_class=Config):
    """Application factory."""
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(config_class)

    # init extensions
    db.init_app(app)

    # create tables once at startup
    with app.app_context():
        db.create_all()

    # register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp, url_prefix="/api")

    if app.config.get('SCHEDULER_ENABLED', True):
        # Check if we're in Flask reloader parent process
        is_reloader_parent = app.debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true'
        
        if not is_reloader_parent:
            init_scheduler(app)
        else:
            print("⏸️  Skipping scheduler in Flask reloader parent process", file=sys.stderr)
    else:
        print("⏸️  Scheduler disabled by config", file=sys.stderr)

    return app
