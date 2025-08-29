# Flyover Data Descriptor - Modular Flask Application

This directory contains the refactored Flyover data descriptor application, now organized using Flask best practices with a modular architecture.

## 🏗️ Architecture Overview

The application has been completely refactored from a monolithic structure to a modular Flask application using:

### **Application Factory Pattern** (`app.py`)
- Centralized application creation and configuration
- Environment-based configuration loading
- Proper initialization order
- Blueprint registration

### **Blueprints** (`blueprints/`)
- `main.py` - Core routes (landing page, static files)
- `ingest.py` - Data upload and processing routes
- `describe.py` - Data description and semantic mapping routes  
- `annotate.py` - Annotation workflow routes
- `api.py` - REST API endpoints

### **Configuration Management** (`config.py`)
- Environment-specific configurations (dev, prod, testing, docker)
- Centralized settings management
- Auto-detection of deployment environment
- Secure configuration handling

### **Modular Components** (`modules/`)
- `app_config.py` - Application setup and logging
- `session_management.py` - Session state management
- `file_operations.py` - File validation utilities
- `graphdb_operations.py` - GraphDB interactions and data upload
- `data_operations.py` - Data processing and semantic operations

## 🚀 Running the Application

### Development
```bash
# Using the new modular app (recommended)
python app.py

# Using the legacy main file (deprecated but supported)
python data_descriptor_main.py
```

### Production
```bash
# Using Gunicorn WSGI server
gunicorn -w 4 -b 0.0.0.0:5000 app:app

# With custom configuration
FLASK_ENV=production gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

### Docker
The application auto-detects Docker environment using environment variables:
- `FLYOVER_GRAPHDB_URL` - GraphDB URL
- `FLYOVER_REPOSITORY_NAME` - Repository name

## 📁 Directory Structure

```
data_descriptor/
├── app.py                    # Flask application factory
├── config.py                 # Configuration management
├── data_descriptor_main.py   # Legacy main file (deprecated)
├── blueprints/              # Route blueprints
│   ├── __init__.py
│   ├── main.py              # Core routes
│   ├── ingest.py            # Data ingestion
│   ├── describe.py          # Data description  
│   ├── annotate.py          # Annotation workflow
│   └── api.py               # REST API endpoints
├── modules/                 # Modular utilities
│   ├── __init__.py
│   ├── app_config.py        # App setup & logging
│   ├── session_management.py # Session management
│   ├── file_operations.py   # File validation
│   ├── graphdb_operations.py # GraphDB interactions
│   └── data_operations.py   # Data processing
├── utils/                   # Legacy utilities (integrated into modules)
├── templates/               # Jinja2 templates
├── assets/                  # Static assets
└── .env.example            # Environment configuration example
```

## 🔧 Configuration

Copy `.env.example` to `.env` and modify as needed:

```bash
cp .env.example .env
# Edit .env with your configuration
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLASK_ENV` | Flask environment | `development` |
| `FLASK_DEBUG` | Enable debug mode | `true` |
| `SECRET_KEY` | Flask secret key | Auto-generated |
| `FLYOVER_GRAPHDB_URL` | GraphDB URL | `http://localhost:7200` |
| `FLYOVER_REPOSITORY_NAME` | Repository name | `userRepo` |

## 📊 Key Improvements

### **Modularity**
- **Before**: 2000+ line monolithic file
- **After**: Organized into focused modules and blueprints

### **Maintainability**  
- Clear separation of concerns
- Single responsibility principle
- Proper dependency injection

### **Flask Best Practices**
- Application factory pattern
- Blueprint-based route organization
- Configuration management
- Proper error handling
- Type hints and documentation

### **Development Experience**
- Better IDE support with modular imports
- Easier testing with isolated components
- Clear code organization
- Comprehensive documentation

## 🔄 Migration from Legacy

The legacy `data_descriptor_main.py` file is deprecated but still functional for backward compatibility. It automatically imports and runs the new modular application.

To fully migrate:
1. Update any scripts to use `python app.py` instead
2. Update deployment configurations to use the new app factory
3. Test all functionality with the new modular structure

## 🧪 Testing

The modular structure enables better testing:

```python
# Test the application factory
from app import create_app

app = create_app('testing')
# Run tests...
```

## 📝 API Documentation

The application includes comprehensive API endpoints in the `api.py` blueprint:

- `GET /api/check-graph-exists` - Check if data exists
- `GET /api/existing-graph-structure` - Get data structure  
- `POST /api/execute-query` - Execute SPARQL queries
- `GET /api/session-status` - Get workflow status

See blueprint files for detailed API documentation.