import type { ApiError } from "./api-types";

export class RequestError extends Error {
  constructor(public readonly error: ApiError, public readonly status: number) {
    super(error.message);
  }
}

export async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const headers = new Headers(options.headers);
  if (options.body && !(options.body instanceof FormData)) headers.set("Content-Type", "application/json");
  const response = await fetch(`/api/v2${path}`, { ...options, headers });
  if (!response.ok) {
    const fallback: ApiError = { code: "request_failed", message: response.statusText, fieldErrors: {}, details: {} };
    throw new RequestError(await response.json().catch(() => fallback), response.status);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export function jsonBody(value: unknown): Pick<RequestInit, "body"> {
  return { body: JSON.stringify(value) };
}
