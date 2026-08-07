import { FormEvent, useEffect, useMemo, useState } from "react";
import { api, jsonBody, RequestError } from "./api";
import type { Job, Profile, Project } from "./api-types";

type Json = Record<string, any>;
type Notice = { kind: "error" | "success"; text: string } | null;

const emptyRequirement = JSON.stringify({
  "@context": { "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#" },
  "formatVersion": "2.0",
  "schema": { "variables": {} },
  "targets": { "omop": { "cdmVersion": "5.4", "person": {}, "variables": {} } },
}, null, 2);

function message(error: unknown): string {
  return error instanceof RequestError ? error.error.message : error instanceof Error ? error.message : "Unexpected error";
}

export function App() {
  const [projectId, setProjectId] = useState<string | null>(() => new URLSearchParams(location.search).get("project"));
  const open = (id: string | null) => {
    const url = id ? `/v2/?project=${encodeURIComponent(id)}` : "/v2/";
    history.pushState({}, "", url);
    setProjectId(id);
  };
  useEffect(() => {
    const listener = () => setProjectId(new URLSearchParams(location.search).get("project"));
    addEventListener("popstate", listener);
    return () => removeEventListener("popstate", listener);
  }, []);
  return (
    <>
      <header className="masthead">
        <button className="brand" onClick={() => open(null)}>Flyover <span>v2</span></button>
        <p>Local semantic mapping and conversion</p>
        <a href="/">Legacy interface</a>
      </header>
      <main>{projectId ? <Workspace id={projectId} close={() => open(null)} /> : <Dashboard open={open} />}</main>
    </>
  );
}

function Dashboard({ open }: { open: (id: string) => void }) {
  const [projects, setProjects] = useState<Project[]>([]);
  const [name, setName] = useState("");
  const [notice, setNotice] = useState<Notice>(null);
  const load = () => api<Project[]>("/projects").then(setProjects).catch((error) => setNotice({ kind: "error", text: message(error) }));
  useEffect(() => { void load(); }, []);
  async function create(event: FormEvent) {
    event.preventDefault();
    try {
      const project = await api<Project>("/projects", { method: "POST", ...jsonBody({ name }) });
      open(project.id);
    } catch (error) { setNotice({ kind: "error", text: message(error) }); }
  }
  return <section className="page">
    <div className="intro"><p className="eyebrow">Private by design</p><h1>Resume a project or begin a new mapping</h1><p>Files and category profiles remain in this installation. Database passwords exist only for the current request.</p></div>
    <NoticeView notice={notice} />
    <form className="new-project" onSubmit={create}><label>New project name<input value={name} onChange={(event) => setName(event.target.value)} required maxLength={200} /></label><button>Create project</button></form>
    <div className="projects">
      {projects.map((project) => <button className="project" key={project.id} onClick={() => open(project.id)}>
        <strong>{project.name}</strong><span>{new Date(project.updated_at).toLocaleString()}</span>
        <Progress readiness={project.readiness} />
      </button>)}
      {!projects.length && <p className="empty">No projects yet.</p>}
    </div>
  </section>;
}

function Progress({ readiness }: { readiness: Project["readiness"] }) {
  const labels: Record<string, string> = { requirement: "Requirement", source: "Source", mapping: "Mapping", omopPreflight: "Preflight" };
  return <div className="progress">{Object.entries(readiness).map(([key, ready]) => <span className={ready ? "done" : ""} key={key}>{labels[key]}</span>)}</div>;
}

function Workspace({ id, close }: { id: string; close: () => void }) {
  const [project, setProject] = useState<Project | null>(null);
  const [requirement, setRequirement] = useState<Json | null>(null);
  const [requirementText, setRequirementText] = useState(emptyRequirement);
  const [profile, setProfile] = useState<Profile | null>(null);
  const [mapping, setMapping] = useState<Json | null>(null);
  const [notice, setNotice] = useState<Notice>(null);
  const refresh = async () => {
    const next = await api<Project>(`/projects/${id}`); setProject(next);
    if (next.readiness.requirement) {
      const value = await api<{ requirement: Json }>(`/projects/${id}/requirement`);
      setRequirement(value.requirement); setRequirementText(JSON.stringify(value.requirement, null, 2));
    }
    if (next.readiness.source) setProfile(await api<Profile>(`/projects/${id}/profile`));
    if (next.readiness.mapping) setMapping((await api<{ mapping: Json }>(`/projects/${id}/mapping`)).mapping);
  };
  useEffect(() => { refresh().catch((error) => setNotice({ kind: "error", text: message(error) })); }, [id]);
  if (!project) return <section className="page"><p>Loading project…</p><NoticeView notice={notice} /></section>;
  return <section className="page workspace">
    <button className="back" onClick={close}>← All projects</button>
    <div className="workspace-title"><div><p className="eyebrow">Named project</p><h1>{project.name}</h1></div><Progress readiness={project.readiness} /></div>
    <NoticeView notice={notice} />
    <details open={!requirement}><summary><Step n="1" title="Requirement" done={!!requirement} /></summary>
      <RequirementEditor id={id} revision={project.requirementRevision} text={requirementText} setText={setRequirementText} onSaved={async () => { await refresh(); setNotice({ kind: "success", text: "Requirement saved as a new immutable revision." }); }} onError={(text) => setNotice({ kind: "error", text })} />
    </details>
    <details open={!!requirement && !profile}><summary><Step n="2" title="CSV source and private profile" done={!!profile} /></summary>
      <SourceUpload id={id} onSaved={async () => { await refresh(); setNotice({ kind: "success", text: "Source profiled locally." }); }} onError={(text) => setNotice({ kind: "error", text })} />
      {profile && <ProfileView profile={profile} />}
    </details>
    <details open={!!profile && !mapping}><summary><Step n="3" title="Column and event mapping" done={!!mapping} /></summary>
      {requirement && profile ? <MappingEditor id={id} revision={project.mappingRevision} requirement={requirement} profile={profile} initial={mapping} onSaved={async () => { await refresh(); setNotice({ kind: "success", text: "Mapping revision saved." }); }} onError={(text) => setNotice({ kind: "error", text })} /> : <p>Complete the preceding steps first.</p>}
    </details>
    <details open={!!mapping}><summary><Step n="4" title="Terminology and OMOP 5.4" done={false} /></summary>
      {requirement && mapping ? <OmopPanel id={id} onError={(text) => setNotice({ kind: "error", text })} /> : <p>Save a mapping first.</p>}
    </details>
    <details><summary><Step n="5" title="Mapped CSV and RDF exports" done={false} /></summary>{mapping ? <OtherExports id={id} onError={(text) => setNotice({ kind: "error", text })} /> : <p>Save a mapping first.</p>}</details>
    <details><summary><Step n="6" title="HealthDCAT-AP Release 7" done={false} /></summary><PublicationPanel id={id} recordCount={profile?.rowCount} onError={(text) => setNotice({ kind: "error", text })} /></details>
  </section>;
}

function Step({ n, title, done }: { n: string; title: string; done: boolean }) {
  return <span className="step"><b>{done ? "✓" : n}</b>{title}</span>;
}

function RequirementEditor(props: { id: string; revision: number; text: string; setText: (text: string) => void; onSaved: () => void; onError: (text: string) => void }) {
  async function save() {
    try {
      await api(`/projects/${props.id}/requirement`, { method: "PUT", headers: props.revision ? { "If-Match": String(props.revision) } : {}, ...jsonBody(JSON.parse(props.text)) });
      props.onSaved();
    } catch (error) { props.onError(message(error)); }
  }
  async function loadFile(file?: File) { if (!file) return; try { props.setText(await file.text()); } catch (error) { props.onError(message(error)); } }
  return <div className="panel"><p>Upload, paste, or edit a Flyover 2.0 JSON-LD requirement. The server validates OMOP bindings before saving.</p><label>Requirement file<input type="file" accept=".json,.jsonld,application/json,application/ld+json" onChange={(event) => void loadFile(event.target.files?.[0])} /></label><textarea className="code" value={props.text} onChange={(event) => props.setText(event.target.value)} spellCheck={false} /><button onClick={save}>Validate and save</button></div>;
}

function SourceUpload({ id, onSaved, onError }: { id: string; onSaved: () => void; onError: (text: string) => void }) {
  const [file, setFile] = useState<File | null>(null); const [layout, setLayout] = useState("wide");
  const [roles, setRoles] = useState({ subject: "", eventType: "", eventValue: "", eventDate: "" });
  async function upload(event: FormEvent) {
    event.preventDefault(); if (!file) return;
    const form = new FormData(); form.set("file", file); form.set("layout", layout); form.set("roles", JSON.stringify(roles));
    try { await api(`/projects/${id}/source`, { method: "POST", body: form }); onSaved(); } catch (error) { onError(message(error)); }
  }
  return <form className="panel form-grid" onSubmit={upload}>
    <label>CSV file<input type="file" accept=".csv,text/csv" required onChange={(event) => setFile(event.target.files?.[0] ?? null)} /></label>
    <label>Layout<select value={layout} onChange={(event) => setLayout(event.target.value)}><option value="wide">Wide — one person per row</option><option value="long">Long — one event per row</option></select></label>
    <label>Subject column<input value={roles.subject} required onChange={(event) => setRoles({ ...roles, subject: event.target.value })} /></label>
    {layout === "long" && <><label>Event type column<input required value={roles.eventType} onChange={(event) => setRoles({ ...roles, eventType: event.target.value })} /></label><label>Event value column<input required value={roles.eventValue} onChange={(event) => setRoles({ ...roles, eventValue: event.target.value })} /></label><label>Event date column<input required value={roles.eventDate} onChange={(event) => setRoles({ ...roles, eventDate: event.target.value })} /></label></>}
    <button>Upload and profile</button>
  </form>;
}

function ProfileView({ profile }: { profile: Profile }) {
  const [query, setQuery] = useState("");
  const columns = Object.entries(profile.columns).filter(([name]) => name.toLowerCase().includes(query.toLowerCase()));
  return <div className="panel"><div className="profile-heading"><h3>{profile.rowCount.toLocaleString()} rows · {profile.columnCount} columns</h3><input placeholder="Search columns" value={query} onChange={(event) => setQuery(event.target.value)} /></div>
    <div className="table-wrap"><table><thead><tr><th>Column</th><th>Type</th><th>Null</th><th>Unique</th><th>Private frequencies</th></tr></thead><tbody>{columns.map(([name, item]) => <tr key={name}><td>{name}</td><td>{item.inferredType}</td><td>{item.nullCount}</td><td>{item.uniqueCount}</td><td>{item.frequenciesSuppressed ? "Suppressed (high cardinality)" : item.valueFrequencies?.slice(0, 4).map((v) => `${v.value} (${v.count})`).join(", ")}</td></tr>)}</tbody></table></div>
  </div>;
}

function MappingEditor(props: { id: string; revision: number; requirement: Json; profile: Profile; initial: Json | null; onSaved: () => void; onError: (text: string) => void }) {
  const variables = Object.keys(props.requirement.schema.variables); const sourceColumns = Object.keys(props.profile.columns);
  const initialState = useMemo(() => {
    const selected: Record<string, string> = {}; const events: Record<string, string> = {}; const categories: Record<string, Record<string, string>> = {};
    const table = props.initial && Object.values((Object.values(props.initial.databases)[0] as Json).tables)[0] as Json;
    Object.values(table?.columns ?? {}).forEach((column: any) => {
      const variable = column.mapsTo.split("/").pop();
      if (column.when) events[String(column.when.equals)] = variable; else selected[variable] = column.localColumn;
      Object.entries(column.localMappings ?? {}).forEach(([canonical, local]) => {
        const values = Array.isArray(local) ? local : [local];
        values.filter((value) => value != null).forEach((value) => { (categories[variable] ??= {})[String(value)] = canonical; });
      });
      (column.ignoredValues ?? []).forEach((value: string) => { (categories[variable] ??= {})[String(value)] = "__ignored__"; });
    });
    return { selected, events, categories };
  }, [props.initial]);
  const [selected, setSelected] = useState<Record<string, string>>(initialState.selected);
  const [eventMap, setEventMap] = useState<Record<string, string>>(initialState.events);
  const [categoryChoice, setCategoryChoice] = useState<Record<string, Record<string, string>>>(initialState.categories);
  const eventValues = props.profile.layout === "long" ? props.profile.columns[props.profile.roles.eventType]?.valueFrequencies?.map((item) => item.value) ?? [] : [];
  const categoricalVariables = variables.filter((variable) => Object.keys(props.requirement.schema.variables[variable]?.valueMapping?.terms ?? {}).length > 0);
  function localValues(variable: string) {
    const column = selected[variable];
    if (column) return props.profile.columns[column]?.valueFrequencies ?? [];
    const discriminator = Object.entries(eventMap).find(([, mapped]) => mapped === variable)?.[0];
    return discriminator ? props.profile.conditionalValueProfiles?.[discriminator]?.valueFrequencies ?? [] : [];
  }
  function categoryFields(variable: string) {
    const localMappings: Record<string, string[]> = {}; const ignoredValues: string[] = [];
    Object.entries(categoryChoice[variable] ?? {}).forEach(([local, choice]) => {
      if (choice === "__ignored__") ignoredValues.push(local);
      else if (choice) (localMappings[choice] ??= []).push(local);
    });
    return { ...(Object.keys(localMappings).length ? { localMappings } : {}), ...(ignoredValues.length ? { ignoredValues } : {}) };
  }
  async function save() {
    const columns: Json = {};
    Object.entries(selected).filter(([, column]) => column).forEach(([variable, column]) => { columns[`column-${variable}`] = { mapsTo: `schema:variable/${variable}`, localColumn: column, ...categoryFields(variable) }; });
    Object.entries(eventMap).filter(([, variable]) => variable).forEach(([value, variable], index) => { columns[`event-${index}`] = { mapsTo: `schema:variable/${variable}`, localColumn: props.profile.roles.eventValue, when: { column: props.profile.roles.eventType, equals: value }, ...categoryFields(variable) }; });
    const mapping = { formatVersion: "2.0", databases: { source: { tables: { source: { layout: props.profile.layout, roles: props.profile.roles, columns } } } } };
    try { await api(`/projects/${props.id}/mapping`, { method: "PUT", headers: props.revision ? { "If-Match": String(props.revision) } : {}, ...jsonBody(mapping) }); props.onSaved(); } catch (error) { props.onError(message(error)); }
  }
  return <div className="panel"><p>Map shared/person fields to columns. In long mode, map each event-type value to one semantic variable.</p>
    <div className="mapping-grid">{variables.map((variable) => <label key={variable}><span>{variable}</span><select value={selected[variable] ?? ""} onChange={(event) => setSelected({ ...selected, [variable]: event.target.value })}><option value="">Unmapped</option>{sourceColumns.map((column) => <option key={column}>{column}</option>)}</select></label>)}</div>
    {props.profile.layout === "long" && <><h3>Event discriminator values</h3><div className="mapping-grid">{eventValues.map((value) => <label key={value}><span>{value}</span><select value={eventMap[value] ?? ""} onChange={(event) => setEventMap({ ...eventMap, [value]: event.target.value })}><option value="">Ignored</option>{variables.map((variable) => <option key={variable}>{variable}</option>)}</select></label>)}</div></>}
    <h3>Category values</h3><p>Give every local value a canonical term, or explicitly ignore it. Unmapped values block OMOP preflight.</p>{categoricalVariables.map((variable) => {
      const values = localValues(variable); if (!values.length) return null;
      const terms = Object.keys(props.requirement.schema.variables[variable].valueMapping.terms);
      return <fieldset className="categories" key={variable}><legend>{variable}</legend>{values.map((item) => <label key={item.value}><span>{item.value} ({item.count})</span><select value={categoryChoice[variable]?.[item.value] ?? ""} onChange={(event) => setCategoryChoice({ ...categoryChoice, [variable]: { ...categoryChoice[variable], [item.value]: event.target.value } })}><option value="">Unmapped</option><option value="__ignored__">Ignored</option>{terms.map((term) => <option key={term} value={term}>{term}</option>)}</select></label>)}</fieldset>;
    })}
    <button onClick={save}>Validate and save mapping</button>
  </div>;
}

function OmopPanel({ id, onError }: { id: string; onError: (text: string) => void }) {
  const [target, setTarget] = useState<Json>({ host: "localhost", port: 5432, database: "", user: "", password: "", schema: "public", sslmode: "require" });
  const [result, setResult] = useState<Json | null>(null); const [job, setJob] = useState<Job | null>(null); const [selections, setSelections] = useState<Record<string, number>>({});
  const set = (key: string, value: string | number) => setTarget({ ...target, [key]: value });
  async function resolve() { try { const value = await api<Json>(`/projects/${id}/terminology/resolve`, { method: "POST", ...jsonBody(target) }); setResult(value); const selected: Record<string, number> = {}; value.resolutions.filter((item: Json) => item.status === "resolved").forEach((item: Json) => { selected[item.key] = item.conceptId; }); setSelections(selected); } catch (error) { onError(message(error)); } }
  async function saveTerminology() { if (!result) return; const overrides: Json = {}; result.resolutions.forEach((item: Json) => { if (selections[item.key]) overrides[item.key] = { conceptId: selections[item.key], uri: item.uri }; }); try { await api(`/projects/${id}/terminology/overrides`, { method: "PUT", ...jsonBody({ overrides, target }) }); setResult({ ...result, saved: true }); } catch (error) { onError(message(error)); } }
  async function preflight() { try { setResult(await api<Json>(`/projects/${id}/omop/preflight`, { method: "POST", ...jsonBody(target) })); } catch (error) { onError(message(error)); } }
  async function convert() { try { const next = await api<Job>(`/projects/${id}/conversions`, { method: "POST", ...jsonBody({ converterId: "omop-5.4", target }) }); setJob(next); poll(next.id); } catch (error) { onError(message(error)); } }
  function poll(jobId: string) { const timer = setInterval(async () => { const next = await api<Job>(`/projects/${id}/jobs/${jobId}`); setJob(next); if (!["queued", "running"].includes(next.status)) clearInterval(timer); }, 1000); }
  return <div className="panel"><p>Credentials are sent only to terminology, preflight, or the supervised converter process. The password is never saved.</p><div className="form-grid compact">
    {[["host", "Host"], ["database", "Database"], ["user", "User"], ["schema", "CDM schema"]].map(([key, label]) => <label key={key}>{label}<input value={target[key]} onChange={(event) => set(key, event.target.value)} /></label>)}
    <label>Password<input type="password" value={target.password} autoComplete="new-password" onChange={(event) => set("password", event.target.value)} /></label><label>TLS mode<select value={target.sslmode} onChange={(event) => set("sslmode", event.target.value)}><option value="require">require</option><option value="verify-ca">verify-ca</option><option value="verify-full">verify-full</option><option value="disable">disable (local test only)</option></select></label>
  </div><div className="actions"><button onClick={resolve}>1. Resolve terminology</button><button className="secondary" onClick={preflight}>2. Run preflight</button><button className="danger" onClick={convert}>3. Write OMOP transaction</button></div>
    {result?.resolutions && <div className="terminology"><h3>Terminology review</h3>{result.resolutions.map((item: Json) => <label key={item.key}><span><strong>{item.key}</strong><small>{item.uri} · {item.status}</small></span><select value={selections[item.key] ?? ""} onChange={(event) => setSelections({ ...selections, [item.key]: Number(event.target.value) })}><option value="">Unresolved</option>{item.candidates.map((candidate: Json) => <option value={candidate.concept_id} key={candidate.concept_id}>{candidate.concept_name} ({candidate.concept_id})</option>)}</select></label>)}<button onClick={saveTerminology}>Save reviewed concepts</button>{result.saved && <span className="saved">Saved</span>}</div>}
    {result && !result.resolutions && <pre className="result">{JSON.stringify(result, null, 2)}</pre>}{job && <p className="job"><strong>Conversion:</strong> {job.status} ({job.progress}%)</p>}
  </div>;
}

function OtherExports({ id, onError }: { id: string; onError: (text: string) => void }) {
  const [job, setJob] = useState<Job | null>(null); const [baseUri, setBaseUri] = useState("https://example.org/flyover/");
  async function submit(converterId: "mapped-csv" | "rdf") { try { const next = await api<Job>(`/projects/${id}/conversions`, { method: "POST", ...jsonBody({ converterId, target: converterId === "rdf" ? { baseUri } : {} }) }); setJob(next); const timer = setInterval(async () => { const current = await api<Job>(`/projects/${id}/jobs/${next.id}`); setJob(current); if (!["queued", "running"].includes(current.status)) clearInterval(timer); }, 700); } catch (error) { onError(message(error)); } }
  const artifacts = (job?.report?.artifacts ?? []) as Array<Json>;
  return <div className="panel"><p>Both exports contain mapped attributes only. RDF ontology/profile creation remains separate from data materialization.</p><label>RDF base URI<input value={baseUri} onChange={(event) => setBaseUri(event.target.value)} /></label><div className="actions"><button onClick={() => submit("mapped-csv")}>Export mapped CSV</button><button className="secondary" onClick={() => submit("rdf")}>Export RDF</button></div>{job && <p className="job">Job: {job.status}</p>}<div className="downloads">{artifacts.map((artifact) => <a key={artifact.id} href={artifact.downloadUrl}>{artifact.kind}</a>)}</div></div>;
}

function PublicationPanel({ id, recordCount, onError }: { id: string; recordCount?: number; onError: (text: string) => void }) {
  const [revision, setRevision] = useState(0); const [artifacts, setArtifacts] = useState<Array<Json>>([]);
  const [metadata, setMetadata] = useState<Json>({ datasetUri: "", title: "", description: "", publisherUri: "", contactName: "", contactEmail: "", accessLevel: "restricted", accessUrl: "", licenseUri: "", accessRightsDescription: "", recordCount, confirmRecordCount: false });
  useEffect(() => { api<{ revision: number; metadata: Json }>(`/projects/${id}/publication`).then((value) => { setRevision(value.revision); setMetadata(value.metadata); }).catch((error) => { if (!(error instanceof RequestError) || error.status !== 404) onError(message(error)); }); }, [id]);
  const set = (key: string, value: unknown) => setMetadata({ ...metadata, [key]: value });
  async function save() { try { const value = await api<{ revision: number }>(`/projects/${id}/publication`, { method: "PUT", headers: revision ? { "If-Match": String(revision) } : {}, ...jsonBody(metadata) }); setRevision(value.revision); } catch (error) { onError(message(error)); } }
  async function exportMetadata() { try { const value = await api<{ artifacts: Array<Json> }>(`/projects/${id}/publication/export`, { method: "POST" }); setArtifacts(value.artifacts); } catch (error) { onError(message(error)); } }
  return <div className="panel"><p>Fields adapt to the access level. Aggregate counts are included only after explicit confirmation.</p><div className="form-grid compact">
    {[['datasetUri', 'Dataset URI'], ['title', 'Title'], ['publisherUri', 'Publisher URI'], ['contactName', 'Contact name'], ['contactEmail', 'Contact email']].map(([key, label]) => <label key={key}>{label}<input value={metadata[key] ?? ""} onChange={(event) => set(key, event.target.value)} /></label>)}
    <label>Access level<select value={metadata.accessLevel} onChange={(event) => set("accessLevel", event.target.value)}><option value="public">Public</option><option value="restricted">Restricted</option><option value="non-public">Non-public</option></select></label>
  </div><label>Description<textarea value={metadata.description} onChange={(event) => set("description", event.target.value)} /></label>
    {metadata.accessLevel === "public" && <div className="form-grid"><label>Access URL<input value={metadata.accessUrl} onChange={(event) => set("accessUrl", event.target.value)} /></label><label>License URI<input value={metadata.licenseUri} onChange={(event) => set("licenseUri", event.target.value)} /></label></div>}
    {metadata.accessLevel === "restricted" && <label>Access procedure<textarea value={metadata.accessRightsDescription} onChange={(event) => set("accessRightsDescription", event.target.value)} /></label>}
    <label className="check"><input type="checkbox" checked={!!metadata.confirmRecordCount} onChange={(event) => set("confirmRecordCount", event.target.checked)} /> Include aggregate record count ({metadata.recordCount ?? "unknown"})</label>
    <div className="actions"><button onClick={save}>Save metadata revision</button><button className="secondary" onClick={exportMetadata} disabled={!revision}>Export Turtle and report</button></div><div className="downloads">{artifacts.map((artifact) => <a key={artifact.id} href={artifact.downloadUrl}>{artifact.kind}</a>)}</div>
  </div>;
}

function NoticeView({ notice }: { notice: Notice }) { return notice ? <p role="alert" className={`notice ${notice.kind}`}>{notice.text}</p> : null; }
