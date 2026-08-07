DROP SCHEMA IF EXISTS flyover_test CASCADE;
CREATE SCHEMA flyover_test;

CREATE TABLE flyover_test.cdm_source (cdm_version text NOT NULL);
INSERT INTO flyover_test.cdm_source VALUES ('5.4');

CREATE TABLE flyover_test.concept (
  concept_id integer PRIMARY KEY, concept_name text NOT NULL, domain_id text NOT NULL,
  vocabulary_id text NOT NULL, concept_code text NOT NULL, standard_concept text,
  invalid_reason text
);
CREATE TABLE flyover_test.concept_relationship (
  concept_id_1 integer NOT NULL, concept_id_2 integer NOT NULL,
  relationship_id text NOT NULL, invalid_reason text
);
INSERT INTO flyover_test.concept VALUES
  (3025315, 'Body weight', 'Measurement', 'LOINC', '29463-7', 'S', NULL),
  (9529, 'kilogram', 'Unit', 'UCUM', 'kg', 'S', NULL),
  (32817, 'EHR', 'Type Concept', 'Type Concept', 'EHR', 'S', NULL),
  (8507, 'MALE', 'Gender', 'Gender', 'M', 'S', NULL),
  (8532, 'FEMALE', 'Gender', 'Gender', 'F', 'S', NULL),
  (910001, 'Primary tumor.clinical [Class] Cancer', 'Observation', 'LOINC', '21905-5', 'S', NULL),
  (910002, 'Regional lymph nodes.clinical [Class] Cancer', 'Observation', 'LOINC', '21906-3', 'S', NULL),
  (910003, 'Distant metastases.clinical [Class] Cancer', 'Observation', 'LOINC', '21907-1', 'S', NULL),
  (910011, 'American Joint Committee on Cancer cT1', 'Observation', 'SNOMED', '1228889001', 'S', NULL),
  (910012, 'American Joint Committee on Cancer cT2', 'Observation', 'SNOMED', '1228929004', 'S', NULL),
  (910013, 'American Joint Committee on Cancer cT3', 'Observation', 'SNOMED', '1228938002', 'S', NULL),
  (910021, 'American Joint Committee on Cancer cN0', 'Observation', 'SNOMED', '1229967007', 'S', NULL),
  (910022, 'American Joint Committee on Cancer cN1', 'Observation', 'SNOMED', '1229973008', 'S', NULL),
  (910031, 'American Joint Committee on Cancer cM0', 'Observation', 'SNOMED', '1229901006', 'S', NULL),
  (910032, 'American Joint Committee on Cancer cM1', 'Observation', 'SNOMED', '1229903009', 'S', NULL),
  (1001, 'Source weight', 'Measurement', 'Example', 'source-weight', NULL, NULL),
  (1002, 'Ambiguous weight', 'Measurement', 'Example', 'ambiguous', NULL, NULL),
  (1003, 'Second weight', 'Measurement', 'LOINC', 'second', 'S', NULL),
  (1004, 'Wrong domain', 'Observation', 'Example', 'wrong-domain', 'S', NULL),
  (1005, 'Invalid concept', 'Measurement', 'Example', 'invalid', 'S', 'D');
INSERT INTO flyover_test.concept_relationship VALUES
  (1001, 3025315, 'Maps to', NULL),
  (1002, 3025315, 'Maps to', NULL),
  (1002, 1003, 'Maps to', NULL);

CREATE TABLE flyover_test.person (
  person_id bigint PRIMARY KEY, gender_concept_id integer NOT NULL, year_of_birth integer NOT NULL,
  month_of_birth integer, day_of_birth integer, birth_datetime timestamp,
  race_concept_id integer NOT NULL, ethnicity_concept_id integer NOT NULL,
  location_id bigint, provider_id bigint, care_site_id bigint, person_source_value text,
  gender_source_value text, gender_source_concept_id integer,
  race_source_value text, race_source_concept_id integer,
  ethnicity_source_value text, ethnicity_source_concept_id integer
);

CREATE TABLE flyover_test.measurement (
  measurement_id bigint PRIMARY KEY, person_id bigint NOT NULL, measurement_concept_id integer NOT NULL,
  measurement_date date NOT NULL, measurement_datetime timestamp, measurement_time text,
  measurement_type_concept_id integer NOT NULL, operator_concept_id integer,
  value_as_number double precision, value_as_concept_id integer, unit_concept_id integer,
  range_low double precision, range_high double precision, provider_id bigint,
  visit_occurrence_id bigint, visit_detail_id bigint, measurement_source_value text,
  measurement_source_concept_id integer, unit_source_value text, unit_source_concept_id integer,
  value_source_value text, measurement_event_id bigint, meas_event_field_concept_id integer
);

CREATE TABLE flyover_test.observation (
  observation_id bigint PRIMARY KEY, person_id bigint NOT NULL, observation_concept_id integer NOT NULL,
  observation_date date NOT NULL, observation_datetime timestamp,
  observation_type_concept_id integer NOT NULL, value_as_number double precision,
  value_as_string text, value_as_concept_id integer, qualifier_concept_id integer,
  unit_concept_id integer, provider_id bigint, visit_occurrence_id bigint, visit_detail_id bigint,
  observation_source_value text, observation_source_concept_id integer,
  unit_source_value text, qualifier_source_value text, value_source_value text,
  observation_event_id bigint, obs_event_field_concept_id integer
);

CREATE TABLE flyover_test.condition_occurrence (
  condition_occurrence_id bigint PRIMARY KEY, person_id bigint NOT NULL,
  condition_concept_id integer NOT NULL, condition_start_date date NOT NULL,
  condition_start_datetime timestamp, condition_end_date date, condition_end_datetime timestamp,
  condition_type_concept_id integer NOT NULL, condition_status_concept_id integer,
  stop_reason text, provider_id bigint, visit_occurrence_id bigint, visit_detail_id bigint,
  condition_source_value text, condition_source_concept_id integer,
  condition_status_source_value text
);
