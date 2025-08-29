# ============================================================================
# DEPRECATED: This file is being refactored for better modularity
# ============================================================================
# 
# This main file has been refactored into a modular Flask application using:
# - Flask application factory pattern (app.py)
# - Blueprints for route organization (blueprints/)
# - Centralized configuration management (config.py)  
# - Modular utility functions (modules/)
# - Proper separation of concerns
#
# To run the application, use:
#   python app.py
#
# Or for production:
#   gunicorn -w 4 -b 0.0.0.0:5000 app:app
#
# ============================================================================

# Backward compatibility: Import and run the new application
from app import app

if __name__ == "__main__":
    print("⚠️  DEPRECATION NOTICE: data_descriptor_main.py is deprecated")
    print("📦 The application has been refactored into modular components")
    print("🚀 Please use 'python app.py' to run the new modular application")
    print("=" * 60)
    
    # Run the new application for backward compatibility
    app.run(host='0.0.0.0', port=5000)