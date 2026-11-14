"""
API tests of the store routes (``controllers/store_controller.py``): the
datasets the status bar's panel lists, exports and deletes. The datasets
service is a double; what crosses the HTTP boundary and what the session
forgets after a delete are checked.
"""

import pytest

from services.store_datasets import StoreDatasetError
from tests.api.support import TABLE

DATASETS = "/api/v1/store/datasets"
ONE = f"/api/v1/store/datasets/{TABLE}"
ANNOTATIONS = "/api/v1/store/annotations"

LISTED = [{"name": TABLE, "rows": 3, "described": True, "annotated": False}]


@pytest.fixture
def datasets(helpers):
    return helpers["store_datasets"].return_value


@pytest.fixture
def remembered(session_cache):
    """A session that knows things about two datasets."""
    session_cache.databases = [TABLE, "visits"]
    session_cache.existing_graph = True
    session_cache.descriptive_info = {TABLE: {"age": {}}, "visits": {"date": {}}}
    session_cache.DescriptiveInfoDetails = {TABLE: {}, "visits": {}}
    session_cache.annotation_status = {
        f"{TABLE}.age": {"success": True},
        "visits.date": {"success": True},
    }
    session_cache.suggestion_jobs = {"variables": object(), "values": None}
    session_cache.suggestion_ingested = {"variables": {}, "values": {}}
    return session_cache


class TestListing:
    @pytest.mark.route("GET", DATASETS, "success")
    def test_the_datasets_come_back_with_their_rows_and_progress(
        self, client, helpers, store, datasets
    ):
        response = client.get(DATASETS)

        assert response.status_code == 200
        assert response.get_json() == {"datasets": LISTED}
        # The service works on the store the request works on.
        helpers["store_datasets"].assert_called_once_with(store)

    @pytest.mark.route("GET", DATASETS, "failure")
    def test_a_store_that_cannot_be_reached_says_so(self, client, datasets):
        datasets.list.side_effect = StoreDatasetError(
            "Currently, the store cannot be reached by Flyover."
        )

        response = client.get(DATASETS)

        assert response.status_code == 502
        assert response.get_json() == {
            "error": "Currently, the store cannot be reached by Flyover."
        }


class TestDeletingOne:
    @pytest.mark.route("DELETE", "/api/v1/store/datasets/<name>", "success")
    def test_the_dataset_goes_and_the_session_forgets_it(
        self, client, datasets, remembered
    ):
        datasets.list.return_value = []

        response = client.delete(ONE)

        assert response.status_code == 200
        assert response.get_json() == {"deleted": [TABLE], "datasets": []}
        datasets.delete_dataset.assert_called_once_with(TABLE)
        assert remembered.databases is None
        assert remembered.existing_graph is False
        assert remembered.descriptive_info == {"visits": {"date": {}}}
        assert remembered.DescriptiveInfoDetails == {"visits": {}}
        assert remembered.annotation_status == {"visits.date": {"success": True}}
        assert remembered.suggestion_jobs is None
        assert remembered.suggestion_ingested is None

    @pytest.mark.route("DELETE", "/api/v1/store/datasets/<name>", "failure")
    def test_a_dataset_the_store_does_not_have_is_a_404(self, client, datasets):
        response = client.delete(f"{DATASETS}/elsewhere")

        assert response.status_code == 404
        assert response.get_json() == {
            "error": "The store has no dataset named 'elsewhere'."
        }
        datasets.delete_dataset.assert_not_called()

    @pytest.mark.parametrize("name", ["two%20words", "a%3Eb", "a%22b"])
    def test_a_name_that_cannot_be_a_dataset_is_a_400(self, client, datasets, name):
        response = client.delete(f"{DATASETS}/{name}")

        assert response.status_code == 400
        datasets.has.assert_not_called()

    def test_a_refused_delete_says_so_and_the_session_keeps_what_it_knew(
        self, client, datasets, remembered
    ):
        datasets.delete_dataset.side_effect = StoreDatasetError(
            "The store refused to delete (HTTP 500)."
        )

        response = client.delete(ONE)

        assert response.status_code == 502
        assert response.get_json() == {
            "error": "The store refused to delete (HTTP 500)."
        }
        assert remembered.databases == [TABLE, "visits"]

    def test_another_site_cannot_delete_a_dataset(self, client, datasets):
        response = client.delete(ONE, headers={"Origin": "https://evil.example"})

        assert response.status_code == 403
        datasets.delete_dataset.assert_not_called()


class TestExportingOne:
    @pytest.mark.route("GET", "/api/v1/store/datasets/<name>/export", "success")
    def test_the_dataset_comes_as_a_trig_file_named_after_it(self, client, datasets):
        datasets.export_dataset.return_value = "<http://g> { <a> <b> <c> . }\n"

        response = client.get(f"{ONE}/export")

        assert response.status_code == 200
        assert response.mimetype == "application/trig"
        assert response.headers["Content-Disposition"] == (
            f'attachment; filename="{TABLE}.trig"'
        )
        assert response.get_data(as_text=True) == "<http://g> { <a> <b> <c> . }\n"
        datasets.export_dataset.assert_called_once_with(TABLE)

    @pytest.mark.route("GET", "/api/v1/store/datasets/<name>/export", "failure")
    def test_a_dataset_the_store_does_not_have_is_a_404(self, client, datasets):
        assert client.get(f"{DATASETS}/elsewhere/export").status_code == 404
        datasets.export_dataset.assert_not_called()

    def test_an_export_the_store_refuses_says_so(self, client, datasets):
        datasets.export_dataset.side_effect = StoreDatasetError(
            "The store refused to export http://data.local/patients/ (HTTP 500)."
        )

        response = client.get(f"{ONE}/export")

        assert response.status_code == 502


class TestEveryDataset:
    @pytest.mark.route("DELETE", DATASETS, "success")
    def test_every_dataset_goes_and_the_session_forgets_them_all(
        self, client, datasets, remembered
    ):
        datasets.list.side_effect = [LISTED, []]

        response = client.delete(DATASETS)

        assert response.status_code == 200
        assert response.get_json() == {"deleted": [TABLE], "datasets": []}
        datasets.delete_all.assert_called_once_with()
        assert remembered.descriptive_info is None
        assert remembered.DescriptiveInfoDetails is None
        assert remembered.annotation_status is None
        assert remembered.databases is None

    @pytest.mark.route("DELETE", DATASETS, "failure")
    def test_a_refused_delete_of_every_dataset_says_so(self, client, datasets):
        datasets.delete_all.side_effect = StoreDatasetError(
            "The store refused to delete (HTTP 500)."
        )

        assert client.delete(DATASETS).status_code == 502

    @pytest.mark.route("GET", "/api/v1/store/export", "success")
    def test_every_dataset_comes_as_one_trig_file(self, client, datasets):
        response = client.get("/api/v1/store/export")

        assert response.status_code == 200
        assert response.headers["Content-Disposition"] == (
            'attachment; filename="flyover-datasets.trig"'
        )
        datasets.export_all.assert_called_once_with()

    @pytest.mark.route("GET", "/api/v1/store/export", "failure")
    def test_a_refused_export_of_every_dataset_says_so(self, client, datasets):
        datasets.export_all.side_effect = StoreDatasetError("Currently, …")

        assert client.get("/api/v1/store/export").status_code == 502


class TestTheAnnotations:
    @pytest.mark.route("DELETE", ANNOTATIONS, "success")
    def test_every_annotation_goes_and_the_data_and_descriptions_stay(
        self, client, datasets, remembered
    ):
        datasets.delete_annotations.return_value = [
            "http://annotation.local/patients/",
            "http://materialized.local/patients/",
        ]

        response = client.delete(ANNOTATIONS)

        assert response.status_code == 200
        assert response.get_json() == {
            "deleted": [
                "http://annotation.local/patients/",
                "http://materialized.local/patients/",
            ],
            "datasets": LISTED,
        }
        assert remembered.annotation_status is None
        assert remembered.descriptive_info == {
            TABLE: {"age": {}},
            "visits": {"date": {}},
        }
        assert remembered.databases == [TABLE, "visits"]

    @pytest.mark.route("DELETE", ANNOTATIONS, "failure")
    def test_a_refused_delete_of_the_annotations_says_so(self, client, datasets):
        datasets.delete_annotations.side_effect = StoreDatasetError(
            "The store refused to delete (HTTP 500)."
        )

        assert client.delete(ANNOTATIONS).status_code == 502

    @pytest.mark.route("GET", "/api/v1/store/annotations/export", "success")
    def test_the_annotations_come_as_one_trig_file(self, client, datasets):
        response = client.get(f"{ANNOTATIONS}/export")

        assert response.status_code == 200
        assert response.headers["Content-Disposition"] == (
            'attachment; filename="flyover-annotations.trig"'
        )

    @pytest.mark.route("GET", "/api/v1/store/annotations/export", "failure")
    def test_a_refused_export_of_the_annotations_says_so(self, client, datasets):
        datasets.export_annotations.side_effect = StoreDatasetError("Currently, …")

        assert client.get(f"{ANNOTATIONS}/export").status_code == 502


def test_a_dataset_called_export_can_be_deleted_like_any_other(client, datasets):
    datasets.has.side_effect = lambda name: name == "export"

    response = client.delete("/api/v1/store/datasets/export")

    assert response.status_code == 200
    datasets.delete_dataset.assert_called_once_with("export")
