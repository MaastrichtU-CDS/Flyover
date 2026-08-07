"""CSV to OMOP CDM 5.4 conversion.

The module intentionally separates local batch construction from PostgreSQL IO.
That keeps validation deterministic and makes it possible to prove that a failed
conversion cannot leave a partly populated CDM.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

import psycopg2
from dateutil.parser import isoparse

from ..contracts import first_table, validate_omop_mapping, variable_key
from ..constraints import validate_value
from ..errors import V2Error
from ..profiling import read_source
from ..terminology import connection_parameters, safe_schema
from .base import Converter, ConverterManifest, ProjectContext

CLINICAL_TABLES = ("person", "observation", "measurement", "condition_occurrence")

REQUIRED_COLUMNS = {
    "person": {
        "person_id", "gender_concept_id", "year_of_birth", "race_concept_id",
        "ethnicity_concept_id", "person_source_value",
    },
    "observation": {
        "observation_id", "person_id", "observation_concept_id", "observation_date",
        "observation_type_concept_id",
    },
    "measurement": {
        "measurement_id", "person_id", "measurement_concept_id", "measurement_date",
        "measurement_type_concept_id",
    },
    "condition_occurrence": {
        "condition_occurrence_id", "person_id", "condition_concept_id",
        "condition_start_date", "condition_type_concept_id",
    },
}


def _present(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _date(value: Any, field: str) -> date:
    try:
        return isoparse(str(value)).date()
    except (TypeError, ValueError, OverflowError) as exc:
        raise V2Error(
            "invalid_date", "A mapped event or birth date is invalid", 422,
            field_errors={field: f"Expected an ISO date, got {value!r}"},
        ) from exc


def _number(value: Any, field: str) -> float:
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            raise InvalidOperation
        return float(number)
    except (InvalidOperation, ValueError) as exc:
        raise V2Error(
            "invalid_number", "A mapped numeric value is invalid", 422,
            field_errors={field: f"Expected a finite number, got {value!r}"},
        ) from exc


def _local_to_canonical(column: dict[str, Any], value: Any) -> Any:
    """Translate legacy canonical->local mappings without losing ignored values."""
    if not _present(value):
        return None
    text = str(value)
    if text in {str(item) for item in column.get("ignoredValues", [])}:
        return None
    for canonical, local_values in column.get("localMappings", {}).items():
        if local_values is None:
            continue
        candidates = local_values if isinstance(local_values, list) else [local_values]
        if text in {str(candidate) for candidate in candidates}:
            return canonical
    return text


def _columns_by_variable(table: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for column in table.get("columns", {}).values():
        result[variable_key(str(column["mapsTo"]))].append(column)
    return result


def _matching_column(columns: list[dict[str, Any]], row: dict[str, Any]) -> dict[str, Any] | None:
    for column in columns:
        when = column.get("when")
        if not when or str(row.get(when["column"], "")) == str(when["equals"]):
            return column
    return None


def _mapped_value(
    columns: dict[str, list[dict[str, Any]]], variable: str | None, row: dict[str, Any]
) -> Any:
    if not variable:
        return None
    column = _matching_column(columns.get(variable, []), row)
    if not column:
        return None
    return _local_to_canonical(column, row.get(column["localColumn"]))


def _concept_id(value: Any, field: str, *, allow_zero: bool = False) -> int:
    try:
        concept_id = int(value)
    except (TypeError, ValueError) as exc:
        raise V2Error(
            "terminology_unresolved", "All terminology mappings must be resolved", 422,
            field_errors={field: "Select an OMOP concept"},
        ) from exc
    if concept_id < 0 or (concept_id == 0 and not allow_zero):
        raise V2Error(
            "terminology_unresolved", "All terminology mappings must be resolved", 422,
            field_errors={field: "Select a non-zero OMOP concept"},
        )
    return concept_id


class OmopBatchBuilder:
    """Normalize wide/long CSV rows and create ready-to-insert CDM records."""

    def __init__(self, context: ProjectContext):
        self.context = context
        _, self.table = first_table(context.mapping)
        self.columns = _columns_by_variable(self.table)
        self.omop = context.requirement["targets"]["omop"]
        self.person_binding = self.omop["person"]
        self.event_bindings = self.omop.get("variables", {})

    def build(self) -> dict[str, list[dict[str, Any]]]:
        try:
            import json
            with open(self.context.source["profile_path"], encoding="utf-8") as stream:
                validate_omop_mapping(self.context.requirement, self.context.mapping, json.load(stream))
        except KeyError as exc:
            raise V2Error("profile_missing", "A source profile is required before conversion", 422) from exc
        frame = read_source(self.context.source["path"])
        rows = frame.to_dicts()
        roles = self.table.get("roles", {})
        subject_column = roles.get("subject")
        if not subject_column or subject_column not in frame.columns:
            raise V2Error("invalid_mapping", "The subject column is missing", 422)

        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row_number, row in enumerate(rows, start=2):
            subject = row.get(subject_column)
            if not _present(subject):
                raise V2Error(
                    "missing_subject", "Every source row needs a subject identifier", 422,
                    field_errors={f"rows.{row_number}.{subject_column}": "Required"},
                )
            grouped[str(subject)].append(row)
            for variable, definitions in self.columns.items():
                column = _matching_column(definitions, row)
                if column:
                    validate_value(
                        variable, _local_to_canonical(column, row.get(column["localColumn"])),
                        self.context.requirement["schema"]["variables"][variable].get("constraints", []),
                        row_number,
                    )

        batches: dict[str, list[dict[str, Any]]] = {table: [] for table in CLINICAL_TABLES}
        person_ids: dict[str, int] = {}
        for person_id, (subject, subject_rows) in enumerate(grouped.items(), start=1):
            person_ids[subject] = person_id
            batches["person"].append(self._person(person_id, subject, subject_rows))

        counters = {"Observation": 0, "Measurement": 0, "ConditionOccurrence": 0}
        for subject, subject_rows in grouped.items():
            for row in subject_rows:
                for variable, binding in self.event_bindings.items():
                    column = _matching_column(self.columns.get(variable, []), row)
                    if not column:
                        continue
                    value = _local_to_canonical(column, row.get(column["localColumn"]))
                    if not _present(value):
                        continue
                    domain = binding["domain"]
                    counters[domain] += 1
                    record = self._event(
                        counters[domain], person_ids[subject], variable, value, binding, row
                    )
                    batches[self._table_for_domain(domain)].append(record)
        return batches

    def _consistent_value(self, variable: str | None, rows: list[dict[str, Any]]) -> Any:
        values = [_mapped_value(self.columns, variable, row) for row in rows]
        distinct = {str(value) for value in values if _present(value)}
        if len(distinct) > 1:
            raise V2Error(
                "inconsistent_person", "Repeated person attributes disagree in long-form data", 422,
                field_errors={f"targets.omop.person.{variable}": "Values differ for one subject"},
            )
        return next((value for value in values if _present(value)), None)

    def _person(self, person_id: int, subject: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        birth_date_value = self._consistent_value(self.person_binding.get("birthDate"), rows)
        birth_year_value = self._consistent_value(self.person_binding.get("birthYear"), rows)
        if _present(birth_date_value):
            born = _date(birth_date_value, "targets.omop.person.birthDate")
            year, month, day = born.year, born.month, born.day
        else:
            try:
                year = int(str(birth_year_value))
            except (TypeError, ValueError) as exc:
                raise V2Error(
                    "missing_birth", "Every person needs a valid birth date or birth year", 422
                ) from exc
            born, month, day = None, None, None

        sex_value = self._consistent_value(self.person_binding.get("sexAtBirth"), rows)
        gender_ids = self.person_binding.get("genderConceptIds", {})
        gender_concept = int(gender_ids.get(str(sex_value), 0)) if _present(sex_value) else 0
        return {
            "person_id": person_id,
            "gender_concept_id": gender_concept,
            "year_of_birth": year,
            "month_of_birth": month,
            "day_of_birth": day,
            "birth_datetime": datetime.combine(born, datetime.min.time()) if born else None,
            "race_concept_id": int(self.person_binding.get("raceConceptId", 0)),
            "ethnicity_concept_id": int(self.person_binding.get("ethnicityConceptId", 0)),
            "location_id": None,
            "provider_id": None,
            "care_site_id": None,
            "person_source_value": subject,
            "gender_source_value": str(sex_value) if _present(sex_value) else None,
            "gender_source_concept_id": 0,
            "race_source_value": None,
            "race_source_concept_id": 0,
            "ethnicity_source_value": None,
            "ethnicity_source_concept_id": 0,
        }

    def _event(
        self,
        event_id: int,
        person_id: int,
        variable: str,
        value: Any,
        binding: dict[str, Any],
        row: dict[str, Any],
    ) -> dict[str, Any]:
        domain = binding["domain"]
        event_date_value = _mapped_value(self.columns, binding.get("dateVariable"), row)
        if not _present(event_date_value) and self.table.get("layout") == "long":
            event_date_value = row.get(self.table.get("roles", {}).get("eventDate", ""))
        event_date = _date(event_date_value, f"targets.omop.variables.{variable}.dateVariable")
        concept_id = _concept_id(binding.get("conceptId"), f"targets.omop.variables.{variable}.conceptId")
        type_id = _concept_id(binding.get("typeConceptId"), f"targets.omop.variables.{variable}.typeConceptId")
        mode = binding["valueMode"]
        value_number = _number(value, variable) if mode == "number" else None
        value_concept = None
        if mode in {"concept", "presence"}:
            lookup_value = str(value) if mode == "concept" else "present"
            value_concept = _concept_id(
                binding.get("termConceptIds", {}).get(lookup_value),
                f"targets.omop.variables.{variable}.termConceptIds.{lookup_value}",
            )

        common = {"person_id": person_id}
        if domain == "Measurement":
            return {
                "measurement_id": event_id, **common,
                "measurement_concept_id": concept_id,
                "measurement_date": event_date,
                "measurement_datetime": datetime.combine(event_date, datetime.min.time()),
                "measurement_time": None,
                "measurement_type_concept_id": type_id,
                "operator_concept_id": 0,
                "value_as_number": value_number,
                "value_as_concept_id": value_concept,
                "unit_concept_id": int(binding.get("unitConceptId", 0)),
                "range_low": None, "range_high": None,
                "provider_id": None, "visit_occurrence_id": None,
                "visit_detail_id": None,
                "measurement_source_value": variable,
                "measurement_source_concept_id": 0,
                "unit_source_value": binding.get("unitUri"),
                "unit_source_concept_id": 0,
                "value_source_value": str(value),
                "measurement_event_id": None,
                "meas_event_field_concept_id": 0,
            }
        if domain == "Observation":
            return {
                "observation_id": event_id, **common,
                "observation_concept_id": concept_id,
                "observation_date": event_date,
                "observation_datetime": datetime.combine(event_date, datetime.min.time()),
                "observation_type_concept_id": type_id,
                "value_as_number": value_number,
                "value_as_string": str(value) if mode == "string" else None,
                "value_as_concept_id": value_concept,
                "qualifier_concept_id": 0,
                "unit_concept_id": int(binding.get("unitConceptId", 0)),
                "provider_id": None, "visit_occurrence_id": None,
                "visit_detail_id": None,
                "observation_source_value": variable,
                "observation_source_concept_id": 0,
                "unit_source_value": binding.get("unitUri"),
                "qualifier_source_value": None,
                "value_source_value": str(value),
                "observation_event_id": None,
                "obs_event_field_concept_id": 0,
            }
        return {
            "condition_occurrence_id": event_id, **common,
            "condition_concept_id": value_concept or concept_id,
            "condition_start_date": event_date,
            "condition_start_datetime": datetime.combine(event_date, datetime.min.time()),
            "condition_end_date": None, "condition_end_datetime": None,
            "condition_type_concept_id": type_id,
            "condition_status_concept_id": 0,
            "stop_reason": None, "provider_id": None,
            "visit_occurrence_id": None, "visit_detail_id": None,
            "condition_source_value": str(value),
            "condition_source_concept_id": 0,
            "condition_status_source_value": None,
        }

    @staticmethod
    def _table_for_domain(domain: str) -> str:
        return {
            "Observation": "observation",
            "Measurement": "measurement",
            "ConditionOccurrence": "condition_occurrence",
        }[domain]


class OmopConverter(Converter):
    def manifest(self) -> ConverterManifest:
        return ConverterManifest(
            id="omop-5.4",
            version="2.0.0",
            label="OMOP CDM 5.4",
            input_kinds=("csv",),
            output_kind="postgresql",
            capabilities=("wide", "long", "preflight", "atomic-write"),
            configuration_schema={
                "type": "object",
                "required": ["host", "database", "user", "password"],
                "properties": {
                    "host": {"type": "string"}, "port": {"type": "integer", "default": 5432},
                    "database": {"type": "string"}, "user": {"type": "string"},
                    "password": {"type": "string", "writeOnly": True},
                    "schema": {"type": "string", "default": "public"},
                    "sslmode": {"type": "string", "default": "require"},
                },
            },
        )

    def validate(self, context: ProjectContext, target: dict[str, Any]) -> dict[str, Any]:
        batches = OmopBatchBuilder(context).build()
        schema = safe_schema(target)
        try:
            with psycopg2.connect(**connection_parameters(target)) as connection:
                checks = self._database_checks(connection, schema)
        except V2Error:
            raise
        except psycopg2.Error as exc:
            raise V2Error(
                "postgres_connection_failed", "Could not validate the OMOP database connection", 422,
                details={"databaseError": exc.diag.message_primary or exc.__class__.__name__},
            ) from exc
        return {
            "ready": True,
            "cdmVersion": "5.4",
            "checks": checks,
            "rowCounts": {table: len(records) for table, records in batches.items()},
        }

    def convert(self, context: ProjectContext, target: dict[str, Any]) -> dict[str, Any]:
        batches = OmopBatchBuilder(context).build()
        schema = safe_schema(target)
        try:
            with psycopg2.connect(**connection_parameters(target)) as connection:
                connection.autocommit = False
                self._database_checks(connection, schema)
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"flyover:{schema}",))
                    self._assert_empty(cursor, schema)
                    for table in CLINICAL_TABLES:
                        self._insert(cursor, schema, table, batches[table])
                connection.commit()
        except V2Error:
            raise
        except psycopg2.Error as exc:
            raise V2Error(
                "omop_write_failed", "The OMOP transaction failed and was rolled back", 422,
                details={"databaseError": exc.diag.message_primary or exc.__class__.__name__},
            ) from exc
        return {
            "status": "completed",
            "target": {"kind": "omop", "cdmVersion": "5.4", "schema": schema},
            "inserted": {table: len(records) for table, records in batches.items()},
        }

    def _database_checks(self, connection: Any, schema: str) -> dict[str, Any]:
        server_version = int(connection.server_version)
        if server_version < 140000:
            raise V2Error("postgres_version", "PostgreSQL 14 or newer is required", 422)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_schema = %s", (schema,),
            )
            found: dict[str, set[str]] = defaultdict(set)
            for table, column in cursor.fetchall():
                found[table].add(column)
            missing_tables = sorted((set(REQUIRED_COLUMNS) | {"concept", "concept_relationship", "cdm_source"}) - set(found))
            missing_columns = {
                table: sorted(columns - found.get(table, set()))
                for table, columns in REQUIRED_COLUMNS.items() if columns - found.get(table, set())
            }
            if missing_tables or missing_columns:
                raise V2Error(
                    "invalid_omop_schema", "Required OMOP CDM 5.4 tables or columns are missing", 422,
                    details={"missingTables": missing_tables, "missingColumns": missing_columns},
                )
            cursor.execute(f"SELECT count(*) FROM {schema}.concept")
            if cursor.fetchone()[0] == 0:
                raise V2Error("omop_vocabulary_empty", "OMOP vocabulary tables must be loaded", 422)
            if "cdm_version" not in found["cdm_source"]:
                raise V2Error("invalid_omop_schema", "cdm_source.cdm_version is required", 422)
            cursor.execute(f"SELECT cdm_version FROM {schema}.cdm_source LIMIT 1")
            version_row = cursor.fetchone()
            if not version_row or str(version_row[0]).strip() != "5.4":
                raise V2Error("omop_version", "The target must declare OMOP CDM version 5.4", 422)
            denied = []
            for table in CLINICAL_TABLES:
                cursor.execute("SELECT has_table_privilege(current_user, %s, 'INSERT')", (f"{schema}.{table}",))
                if not cursor.fetchone()[0]:
                    denied.append(table)
            if denied:
                raise V2Error("omop_privileges", "INSERT privilege is required for all target tables", 422, details={"tables": denied})
            self._assert_empty(cursor, schema)
        return {
            "postgres14OrNewer": True,
            "requiredTables": True,
            "vocabularyLoaded": True,
            "clinicalTablesEmpty": True,
            "transactionPrivileges": True,
        }

    @staticmethod
    def _assert_empty(cursor: Any, schema: str) -> None:
        populated: dict[str, int] = {}
        for table in CLINICAL_TABLES:
            cursor.execute(f"SELECT count(*) FROM {schema}.{table}")
            count = int(cursor.fetchone()[0])
            if count:
                populated[table] = count
        if populated:
            raise V2Error(
                "omop_target_not_empty", "The first Flyover v2 release requires empty clinical tables", 409,
                details={"rowCounts": populated},
            )

    @staticmethod
    def _insert(cursor: Any, schema: str, table: str, records: Iterable[dict[str, Any]]) -> None:
        rows = list(records)
        if not rows:
            return
        columns = list(rows[0])
        placeholders = ", ".join(["%s"] * len(columns))
        statement = f"INSERT INTO {schema}.{table} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(statement, [[record[column] for column in columns] for record in rows])
