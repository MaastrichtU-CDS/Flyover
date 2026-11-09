"""
Which workflow each backend test module tests.

Most modules test a step of the RDF workflow, or the store and services it
runs on: ingest and the triplifier, describe, annotate and the annotation
helper, share, the RDF store, and the mapping suggestions. The rest test
what every workflow shares: the app, the session, the map format and
projects. ``conftest.py`` marks the first with ``rdf``:

    pytest -m rdf          # the RDF workflow's tests
    pytest -m "not rdf"    # the shared ones

Every test module is in exactly one of the sets below
(``tests/unit/test_workflows.py`` checks it), so a new module has to say
where it belongs. A new workflow gets its own marker and set. A name may
come before its module does (a branch still open adds it).
"""

from typing import Optional

RDF = frozenset(
    {
        # unit
        "test_annotation_helper.py",
        "test_data_preprocessing.py",
        "test_materialization.py",
        "test_prefix_handling.py",
        "test_query_builder.py",
        "test_rdf_store_repository.py",
        "test_rdf_store_service.py",
        "test_rdf_store_url.py",
        "test_services.py",
        "test_share_service.py",
        "test_store_datasets.py",
        "test_store_launcher.py",
        "test_store_lifecycle.py",
        "test_store_selection.py",
        "test_suggestions_contract.py",
        "test_suggestions_pasted_answer.py",
        "test_suggestions_prompt_export.py",
        "test_suggestions_rules.py",
        "test_suggestions_service.py",
        "test_value_mapping_annotations.py",
        # integration
        "test_integration_annotate.py",
        "test_integration_annotation_cli.py",
        "test_integration_describe.py",
        "test_integration_ingest.py",
        "test_integration_ingest_uploads.py",
        "test_integration_rdf_golden_path.py",
        "test_integration_share.py",
        "test_integration_store_datasets.py",
        "test_integration_store_edges.py",
        "test_integration_suggestions.py",
        # api
        "test_api_annotate.py",
        "test_api_describe.py",
        "test_api_ingest.py",
        "test_api_share.py",
        "test_api_store.py",
        "test_api_suggestions.py",
    }
)

SHARED = frozenset(
    {
        # unit
        "test_convert_legacy_mapping.py",
        "test_loaders.py",
        "test_mapping_schema.py",
        "test_mapping_validator.py",
        "test_project_map_fetch.py",
        "test_raw_file_address.py",
        "test_session_helpers.py",
        "test_workflows.py",
        # integration
        "test_integration_loader_validation.py",
        "test_integration_project.py",
        "test_integration_session.py",
        "test_loaders_real_files.py",
        "test_validation_real_files.py",
        # api
        "test_api_app.py",
        "test_api_compression.py",
        "test_api_factory.py",
        "test_api_fuzz.py",
        "test_api_project.py",
        "test_api_routes.py",
        "test_api_security.py",
        "test_api_session.py",
        # perf
        "test_perf_http.py",
    }
)


def marker_for(module_name: str) -> Optional[str]:
    """The workflow marker of the test module ``module_name`` (its file
    name), or None for a module every workflow shares."""
    return "rdf" if module_name in RDF else None
