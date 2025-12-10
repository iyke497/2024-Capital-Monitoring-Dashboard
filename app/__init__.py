# app/__init__.py
from flask import Flask
from .config import Config
from .database import db
from .routes.main import main_bp
from .routes.api import api_bp


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

    return app
