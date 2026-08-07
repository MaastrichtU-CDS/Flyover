// Generated-contract boundary. `npm run generate-api` will replace this file
// once the OpenAPI generator is enabled in CI; names mirror the public schema.
export type Readiness = {
  requirement: boolean;
  source: boolean;
  mapping: boolean;
  omopPreflight: boolean;
};

export type Project = {
  id: string;
  name: string;
  created_at: string;
  updated_at: string;
  requirementRevision: number;
  mappingRevision: number;
  hasSource: boolean;
  readiness: Readiness;
};

export type ApiError = {
  code: string;
  message: string;
  fieldErrors: Record<string, string>;
  details: Record<string, unknown>;
};

export type ColumnProfile = {
  rowCount: number;
  nullCount: number;
  uniqueCount: number;
  inferredType: "numeric" | "string";
  frequenciesSuppressed?: boolean;
  valueFrequencies?: Array<{ value: string; count: number }>;
  numeric?: { minimum: number; maximum: number; mean: number; median: number };
};

export type Profile = {
  layout: "wide" | "long";
  roles: Record<string, string>;
  rowCount: number;
  columnCount: number;
  columns: Record<string, ColumnProfile>;
  conditionalValueProfiles?: Record<string, ColumnProfile>;
};

export type Job = {
  id: string;
  status: "queued" | "running" | "completed" | "failed" | "retryable" | "cancelled";
  progress: number;
  report?: Record<string, unknown>;
};
