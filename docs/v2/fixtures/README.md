# Flyover v2 local test run

This fixture set exercises both supported CSV layouts and writes into a deliberately small OMOP CDM 5.4 PostgreSQL schema. It includes body weight, administrative gender, and AJCC v8 clinical TNM staging for a lung-cancer cohort. The clinical T, N, and M observations use the LOINC staging-type codes and SNOMED CT AJCC category values used by HL7 mCODE. The SQL fixture is for development only: it contains just the columns and vocabulary concepts needed by these examples, not the complete OMOP DDL or vocabulary. Its `910xxx` concept IDs are fixture-local stand-ins; Flyover resolves the real concept IDs from the vocabulary loaded in the target OMOP database.

## Files

- `omop-wide-requirement.jsonld` — requirement used by both layouts.
- `omop-wide-source.csv` — one person and measurement per row.
- `omop-long-source.csv` — repeated people with one event per row.
- `../../../triplifier/backend/data_descriptor/tests/fixtures/omop54_minimal.sql` — minimal disposable OMOP 5.4 target.

## 1. Start Flyover and the disposable OMOP target

The `omop-demo` Compose profile starts PostgreSQL 16 and initializes the minimal schema automatically. It does not configure TLS, so explicitly enable insecure PostgreSQL only for this local test:

```bash
FLYOVER_ALLOW_INSECURE_POSTGRES=true \
  docker compose --profile omop-demo up --build -d
```

Flyover remains bound to `127.0.0.1:5000`; PostgreSQL is also available from the host at `127.0.0.1:5433`. The target uses temporary storage and is recreated empty when its container is recreated.

## 2. Run the wide workflow

Open `http://localhost:5000/v2/`, create a project, and use:

1. Requirement: paste or upload `omop-wide-requirement.jsonld`.
2. Source: upload `omop-wide-source.csv`.
3. Layout: `wide`.
4. Subject column: `person_id`.
5. Map:
   - `identifier` → `person_id`
   - `birth_date` → `birth_date`
   - `biological_sex` → `gender`
   - `measurement_date` → `measurement_date`
   - `weight` → `weight_kg`
   - `tnm_date` → `tnm_date`
   - `clinical_t` → `clinical_t`
   - `clinical_n` → `clinical_n`
   - `clinical_m` → `clinical_m`
6. Under category values for `biological_sex`, map:
   - local `F` → canonical `female`
   - local `M` → canonical `male`
7. Map the local clinical TNM categories:
   - `T1`, `T2`, and `T3` → `cT1`, `cT2`, and `cT3`
   - `N0` and `N1` → `cN0` and `cN1`
   - `M0` and `M1` → `cM0` and `cM1`
8. PostgreSQL settings:
   - Host: `omop-minimal`
   - Port: `5432`
   - Database: `flyover`
   - User/password: `flyover` / `flyover`
   - CDM schema: `flyover_test`
   - TLS mode: `disable (local test only)`
9. Resolve terminology, save the reviewed concepts, run preflight, then run the OMOP transaction.

Expected result: five `person` rows, five `measurement` rows, and fifteen `observation` rows (one clinical T, N, and M observation for each person). The gender mapping uses concept `8532` for female and `8507` for male.

## 3. Run the long workflow

The first-release target must be empty, so reset it before creating a second project:

```bash
docker compose --profile omop-demo up --detach --force-recreate omop-minimal
```

Wait until `docker compose --profile omop-demo ps` reports the service as healthy.

Upload `omop-long-source.csv` with:

- Layout: `long`
- Subject: `person_id`
- Event type: `event_type`
- Event value: `event_value`
- Event date: `event_date`

Map `identifier`, `birth_date`, and `biological_sex` to `person_id`, `birth_date`, and `gender`. Map local gender values `F` → `female` and `M` → `male`. Under event discriminator values, map:

- `body_weight` → `weight`
- `clinical_t` → `clinical_t`, with `T1`/`T2`/`T3` mapped to `cT1`/`cT2`/`cT3`
- `clinical_n` → `clinical_n`, with `N0`/`N1` mapped to `cN0`/`cN1`
- `clinical_m` → `clinical_m`, with `M0`/`M1` mapped to `cM0`/`cM1`

The event-date role supplies the date, so `measurement_date` and `tnm_date` do not need source-column mappings in this layout.

Expected result: five `person` rows, six `measurement` rows, and fifteen `observation` rows.

## 4. Inspect and clean up

```bash
docker compose --profile omop-demo exec omop-minimal \
  psql --username flyover --dbname flyover \
  --command 'SELECT person_id, person_source_value, gender_concept_id, gender_source_value FROM flyover_test.person;'

docker compose --profile omop-demo exec omop-minimal \
  psql --username flyover --dbname flyover \
  --command 'SELECT person_id, measurement_date, value_as_number FROM flyover_test.measurement;'

docker compose --profile omop-demo exec omop-minimal \
  psql --username flyover --dbname flyover \
  --command 'SELECT person_id, observation_source_value, value_source_value, observation_concept_id, value_as_concept_id FROM flyover_test.observation ORDER BY person_id, observation_source_value;'

docker compose --profile omop-demo down
```

The OMOP target uses a `tmpfs`, so `down` removes its database. Flyover project state remains in the `flyover-v2-data` Docker volume unless you add `--volumes`.
