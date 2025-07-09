# Annotation UI Integration - User Guide

The Flyover application now includes an integrated user interface for the annotation process, making it easier for users to annotate their data without needing command-line access.

## New Features

### 1. Annotation Review Page
- **Access**: Click "Review and Annotate Data" button on the download page
- **Features**:
  - Inspect all variables from your semantic map
  - See which variables are annotated vs unannotated
  - Review predicates, classes, and value mappings
  - Validate data before starting annotation process

### 2. UI-Triggered Annotation Process
- **Access**: Click "Start Annotation Process" on the annotation review page
- **Features**:
  - Triggers annotation helper functionality directly from the UI
  - Real-time status updates
  - Error handling and reporting
  - No command-line access required

### 3. Annotation Verification Page
- **Access**: Automatically redirected after annotation process completes
- **Features**:
  - View annotation status for each variable
  - Test individual variables with SPARQL queries
  - Verify semantic interoperability
  - View sample data (top 5 rows)

## Workflow

1. **Upload Data**: Upload your CSV files or connect to PostgreSQL database
2. **Describe Data**: Use the existing UI to describe your variables and data types
3. **Upload Semantic Map**: Upload your global semantic map JSON file
4. **Download Metadata**: Download your local ontology and semantic map
5. **[NEW] Review Annotations**: Click "Review and Annotate Data" to inspect your mappings
6. **[NEW] Start Annotation**: Click "Start Annotation Process" to run the annotation helper
7. **[NEW] Verify Results**: Test your annotations to ensure they work correctly

## Technical Details

### Backend Integration
- Annotation helper functionality moved into the Flask web application
- New routes: `/annotation-review`, `/start-annotation`, `/annotation-verify`, `/query-variable`
- Session-based storage of annotation status and results
- Error handling for GraphDB connection issues

### UI Components
- Follows existing Flyover aesthetic (Bootstrap-based styling)
- Responsive design matching units and categories pages
- Real-time status updates using JavaScript/AJAX
- Clear visual indicators for annotation status

### Validation Features
- Checks for required fields (predicate, class, local_definition)
- Identifies unannotated variables
- Validates semantic map structure
- Provides meaningful error messages

## Benefits

1. **No Command-Line Required**: Users can complete the entire workflow through the web interface
2. **Better Validation**: Visual inspection of mappings before annotation
3. **Real-Time Feedback**: Status updates and error reporting
4. **Verification Tools**: Built-in SPARQL queries to test annotation success
5. **User-Friendly**: Follows familiar UI patterns from the rest of the application

## Error Handling

The system gracefully handles various error conditions:
- Missing semantic map data
- Invalid variable definitions
- GraphDB connection issues
- SPARQL query failures
- Network timeouts

## Future Enhancements

Potential future improvements:
- Batch validation of all variables
- More sophisticated SPARQL query templates
- Export of verification results
- Integration with external ontology services