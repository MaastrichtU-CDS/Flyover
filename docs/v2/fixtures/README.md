# Flyover v2 local test run

This fixture set exercises both supported CSV layouts and writes into a deliberately small OMOP CDM 5.4 PostgreSQL schema. The SQL fixture is for development only: it contains just the columns and vocabulary concepts needed by these examples, not the complete OMOP DDL or vocabulary.

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
6. Under category values for `biological_sex`, map:
   - local `F` → canonical `female`
   - local `M` → canonical `male`
7. PostgreSQL settings:
   - Host: `omop-minimal`
   - Port: `5432`
   - Database: `flyover`
   - User/password: `flyover` / `flyover`
   - CDM schema: `flyover_test`
   - TLS mode: `disable (local test only)`
8. Resolve terminology, save the reviewed concepts, run preflight, then run the OMOP transaction.

Expected result: three `person` rows and three `measurement` rows. The person gender concepts are `8532` (female), `8507` (male), and `8532` (female).

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

Map `identifier`, `birth_date`, and `biological_sex` to `person_id`, `birth_date`, and `gender`. Map local gender values `F` → `female` and `M` → `male`. Under event discriminator values, map `body_weight` to `weight`. The event-date role supplies the date, so `measurement_date` does not need a source-column mapping in this layout.

Expected result: three `person` rows and four `measurement` rows.

## 4. Inspect and clean up

```bash
docker compose --profile omop-demo exec omop-minimal \
  psql --username flyover --dbname flyover \
  --command 'SELECT person_id, person_source_value, gender_concept_id, gender_source_value FROM flyover_test.person;'

docker compose --profile omop-demo exec omop-minimal \
  psql --username flyover --dbname flyover \
  --command 'SELECT person_id, measurement_date, value_as_number FROM flyover_test.measurement;'

docker compose --profile omop-demo down
```

The OMOP target uses a `tmpfs`, so `down` removes its database. Flyover project state remains in the `flyover-v2-data` Docker volume unless you add `--volumes`.
