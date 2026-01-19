# app.py (root of project)
from app import create_app

app = create_app()

if __name__ == "__main__":
    # Respect Config.DEBUG / FLASK_DEBUG instead of hardcoding
    debug = bool(app.config.get("DEBUG", False))
    app.run(debug=debug, host="0.0.0.0", port=25000)
