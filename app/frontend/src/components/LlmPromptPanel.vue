<script>
// Named helpers live in a plain script block: <script setup> cannot export.
export const CHUNK_OPTIONS = [20, 40, 80, 160, 400]

// Clipboard write that works outside secure contexts too: the async API
// needs https or localhost, so fall back to selecting a hidden textarea
// and the legacy copy command. Returns whether anything was copied.
export async function copyText(text) {
  try {
    if (navigator?.clipboard?.writeText) {
      await navigator.clipboard.writeText(text)
      return true
    }
  } catch {
    // Fall through to the legacy path.
  }
  try {
    const ta = document.createElement('textarea')
    ta.value = text
    ta.setAttribute('readonly', '')
    ta.style.position = 'fixed'
    ta.style.opacity = '0'
    document.body.appendChild(ta)
    ta.select()
    const ok = document.execCommand && document.execCommand('copy')
    document.body.removeChild(ta)
    return !!ok
  } catch {
    return false
  }
}
</script>

<script setup>
/**
 * LlmPromptPanel — the per-database "use an external LLM" panel on the
 * describe pages: the prompt export + paste-back round trip of
 * docs/mapping-suggestions/02-llm-prompt-export-roundtrip.md; the rules
 * the prompt obeys are in docs/mapping-suggestions/prompt-export-rules.md.
 *
 * Collapsed by default. Open, it generates a prompt for one database and
 * phase on the server (the browser's semantic map goes along so the
 * "already mapped" context matches what the user sees) and opens it in a
 * modal that shows, first of all, the part of the prompt that carries
 * the user's own column names or values. Copy, download and the
 * paste-back field all sit in that modal — copy behind one
 * acknowledgement checkbox, the answer import ungated — so a
 * multi-part round trip never has to leave the modal. The LLM's answer
 * comes back through the store's ingest(): the server validates it and
 * the imported records render as ordinary pasted_llm suggestion pills
 * that need the same explicit review.
 *
 * Copying tries the async clipboard API (secure contexts only) and falls
 * back to a selected textarea + execCommand; every part can also be
 * downloaded as a .txt. The part size is the user's choice so small
 * context windows work.
 *
 * Props:
 *   phase — 'variables' or 'values'.
 *   database — the store database name the panel is for.
 *
 * Emits:
 *   ingested — after a successful import; payload is the ingest summary.
 */

import { computed, onBeforeUnmount, ref, watch } from 'vue'
import * as jsonld from '@/lib/jsonld'
import { useStatusStore } from '@/stores/status'
import { useSuggestionsStore } from '@/stores/suggestions'

const props = defineProps({
  phase: { type: String, required: true },
  database: { type: String, required: true },
})

const emit = defineEmits(['ingested'])

const suggestions = useSuggestionsStore()
const status = useStatusStore()

const open = ref(false)
// The site's default (FLYOVER_SUGGESTION_PROMPT_CHUNK via /status), not a
// hardcoded 40, so an operator's env choice reaches the UI.
const chunk = ref(suggestions.promptExportChunk || 40)
const generating = ref(false)
const generateError = ref('')
const prompt = ref(null)
// The modal: which part is shown, whether the user acknowledged the risk
// (reset every time the modal opens), whether the full text is unfolded.
const modalOpen = ref(false)
const part = ref(1)
const acknowledged = ref(false)
const fullOpen = ref(false)
const answer = ref('')
const importing = ref(false)
const importError = ref('')
const importResult = ref(null)

const itemLabel = computed(() => (props.phase === 'values' ? 'values' : 'columns'))

// Offer the site default even when it is not one of the stock sizes.
const chunkOptions = computed(() => {
  const d = suggestions.promptExportChunk || 40
  return CHUNK_OPTIONS.includes(d) ? CHUNK_OPTIONS : [...CHUNK_OPTIONS, d].sort((a, b) => a - b)
})

const chunks = computed(() => prompt.value?.chunks || [])

const currentChunk = computed(
  () => chunks.value.find((c) => c.index === part.value) || chunks.value[0] || null,
)

// Section 3 of a part: the local column names or values, the only part
// of the prompt that carries the user's own data. Shown first in the
// modal; the rest of the prompt is schema and instructions.
const dataSection = computed(() => {
  const text = currentChunk.value?.prompt || ''
  const start = text.indexOf('\n## 3.')
  if (start < 0) return text
  const end = text.indexOf('\n## 4.', start)
  return text.slice(start + 1, end > start ? end : text.length).trim()
})

// What the guards kept out of this prompt, one short line.
const leftOut = computed(() => {
  const p = prompt.value
  if (!p) return []
  const out = (p.held_back || []).map((h) => `${h.column} held back (${h.reason})`)
  if (p.suppressed) {
    out.push(`${p.suppressed} rare value${p.suppressed === 1 ? '' : 's'} left out (seen fewer than ${p.min_value_count} times)`)
  }
  return out
})

const importSummary = computed(() => {
  const r = importResult.value
  if (!r) return ''
  const parts = [`${r.accepted} imported`]
  if (r.nulled) parts.push(`${r.nulled} left for you to decide (invalid key or already mapped)`)
  if (r.rejected) parts.push(`${r.rejected} ignored`)
  if (r.skipped) parts.push(`${r.skipped} already mapped`)
  return `${parts.join(', ')}. Review the imported suggestions in the table.`
})

const summary = computed(() => {
  const p = prompt.value
  if (!p) return ''
  if (!p.item_count) {
    const rare = p.suppressed
      ? ` ${p.suppressed} rare value${p.suppressed === 1 ? '' : 's'} (seen fewer than ${p.min_value_count} times) stay with you.`
      : ''
    return props.phase === 'values'
      ? `Every value of the mapped categorical columns is already mapped; nothing to ask an LLM.${rare}`
      : 'Every column is already mapped; nothing to ask an LLM.'
  }
  const parts = [`${p.item_count} ${itemLabel.value} still to map`]
  if (p.chunks?.length > 1) parts.push(`${p.chunks.length} parts of at most ${p.chunk_hint}`)
  if (p.held_back?.length) parts.push(`${p.held_back.length} column${p.held_back.length === 1 ? '' : 's'} held back`)
  if (p.suppressed) parts.push(`${p.suppressed} rare value${p.suppressed === 1 ? '' : 's'} left out`)
  return parts.join(' · ')
})

async function generate() {
  generating.value = true
  generateError.value = ''
  prompt.value = null
  try {
    prompt.value = await suggestions.fetchPrompt(props.phase, props.database, {
      mapping: jsonld.getMapping(),
      chunk: chunk.value,
    })
    if (chunks.value.length) openModal()
  } catch (err) {
    generateError.value = err?.message || 'Could not generate the prompt.'
  } finally {
    generating.value = false
  }
}

function openModal() {
  part.value = chunks.value[0]?.index || 1
  acknowledged.value = false
  fullOpen.value = false
  modalOpen.value = true
}

function closeModal() {
  modalOpen.value = false
}

function onKeydown(e) {
  if (e.key === 'Escape' && modalOpen.value) closeModal()
}

watch(modalOpen, (isOpen) => {
  if (typeof document === 'undefined') return
  if (isOpen) document.addEventListener('keydown', onKeydown)
  else document.removeEventListener('keydown', onKeydown)
})

onBeforeUnmount(() => {
  if (typeof document !== 'undefined') document.removeEventListener('keydown', onKeydown)
})

async function copyCurrent() {
  const item = currentChunk.value
  if (!item || !acknowledged.value) return
  const ok = await copyText(item.prompt)
  if (ok) {
    status.success(
      chunks.value.length > 1
        ? `Part ${item.index} of ${chunks.value.length} copied — paste it into your LLM client`
        : 'Prompt copied — paste it into your LLM client',
    )
  } else {
    fullOpen.value = true
    status.warning('Copying is blocked in this browser; select the text in the full prompt or download it.')
  }
}

function downloadCurrent() {
  const item = currentChunk.value
  if (!item || !acknowledged.value) return
  const name = `flyover-prompt-${props.phase}-${props.database}${
    chunks.value.length > 1 ? `-part${item.index}` : ''
  }.txt`
  const blob = new Blob([item.prompt], { type: 'text/plain;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = name
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

async function importAnswer() {
  const text = answer.value.trim()
  if (!text) return
  importing.value = true
  importError.value = ''
  importResult.value = null
  try {
    importResult.value = await suggestions.ingest(props.phase, props.database, {
      answer: text,
      mapping: jsonld.getMapping(),
    })
    answer.value = ''
    emit('ingested', importResult.value)
  } catch (err) {
    importError.value = err?.message || 'Could not import the answer.'
  } finally {
    importing.value = false
  }
}
</script>

<template>
  <div
    class="llm-help-panel"
    :class="{ open }"
    :data-phase="phase"
    :data-database="database"
  >
    <button
      type="button"
      class="btn btn-sm btn-outline-secondary llm-help-toggle"
      :aria-expanded="open ? 'true' : 'false'"
      title="Generate a prompt for an LLM and paste its answer back as suggestions"
      @click="open = !open"
    >
      <i class="fas fa-robot" /> Use an LLM
      <i
        class="fas"
        :class="open ? 'fa-chevron-up' : 'fa-chevron-down'"
      />
    </button>

    <div
      v-if="open"
      class="llm-help-body"
    >
      <p class="llm-help-intro">
        Flyover runs no language model. Generate a prompt, run it in an LLM your institution
        allows, and paste the answer back; nothing is saved until you accept a suggestion.
      </p>

      <div class="llm-help-options">
        <label class="llm-help-option">
          Items per prompt
          <select
            v-model.number="chunk"
            class="form-select form-select-sm llm-help-chunk-size"
            title="Smaller parts suit an LLM with a small context window"
          >
            <option
              v-for="n in chunkOptions"
              :key="n"
              :value="n"
            >
              {{ n }}
            </option>
          </select>
        </label>
        <button
          type="button"
          class="btn btn-sm btn-primary llm-help-generate"
          :disabled="generating"
          @click="generate"
        >
          <i
            class="fas"
            :class="generating ? 'fa-spinner fa-spin' : 'fa-wand-magic-sparkles'"
          />
          {{ prompt ? 'Regenerate prompt' : 'Generate prompt' }}
        </button>
        <button
          v-if="chunks.length"
          type="button"
          class="btn btn-sm btn-outline-primary llm-help-show"
          @click="openModal"
        >
          <i class="fas fa-eye" /> Show prompt
        </button>
        <span
          v-if="generateError"
          class="llm-help-error"
          role="alert"
        >{{ generateError }}</span>
      </div>
      <p
        v-if="prompt"
        class="llm-help-summary"
      >
        {{ summary }}
      </p>
    </div>

    <div
      v-if="modalOpen && currentChunk"
      class="llm-prompt-modal"
      role="dialog"
      aria-modal="true"
      :aria-label="`Prompt for ${database}`"
    >
      <div
        class="llm-prompt-modal-backdrop"
        @click="closeModal"
      />
      <div class="llm-prompt-modal-dialog">
        <div class="llm-prompt-modal-header">
          <h5 class="llm-prompt-modal-title">
            <i class="fas fa-robot" /> Prompt for {{ database }} · {{ itemLabel }}
          </h5>
          <button
            type="button"
            class="llm-prompt-modal-close"
            aria-label="Close"
            @click="closeModal"
          >
            &times;
          </button>
        </div>
        <div class="llm-prompt-modal-body">
          <div
            v-if="chunks.length > 1"
            class="llm-prompt-parts"
          >
            <span>{{ chunks.length }} parts, send each on its own:</span>
            <button
              v-for="c in chunks"
              :key="c.index"
              type="button"
              class="btn btn-sm llm-prompt-part"
              :class="c.index === currentChunk.index ? 'btn-primary' : 'btn-outline-primary'"
              @click="part = c.index"
            >
              Part {{ c.index }}
            </button>
          </div>
          <h6 class="llm-prompt-modal-heading">
            <i class="fas fa-shield-halved" />
            {{ phase === 'values' ? 'Values' : 'Column names' }} that leave the browser
            <template v-if="chunks.length > 1">
              in part {{ currentChunk.index }}
            </template>
          </h6>
          <pre class="llm-prompt-modal-data">{{ dataSection }}</pre>
          <p
            v-if="leftOut.length"
            class="llm-prompt-modal-left-out"
          >
            Left out: {{ leftOut.join(' · ') }}
          </p>
          <p class="llm-prompt-modal-privacy">
            {{ prompt.privacy }}
          </p>
          <details
            class="llm-prompt-modal-full"
            :open="fullOpen || null"
            @toggle="fullOpen = $event.target.open"
          >
            <summary>Full prompt ({{ currentChunk.item_count }} {{ itemLabel }})</summary>
            <textarea
              class="form-control llm-help-preview"
              readonly
              rows="12"
              :value="currentChunk.prompt"
            />
          </details>
          <label class="llm-prompt-ack">
            <input
              v-model="acknowledged"
              type="checkbox"
              class="llm-prompt-ack-input"
            >
            I have checked the {{ phase === 'values' ? 'values' : 'column names' }} above for
            personal data and accept the risk of sending them to the LLM I use.
          </label>
          <!-- The paste-back ends the modal body: copy a part, run it in the
               LLM, paste the answer here, import it, move to the next part —
               a multi-part round trip never has to leave the modal. -->
          <div class="llm-help-paste llm-prompt-modal-paste">
            <label
              class="llm-help-paste-label"
              :for="`llm-answer-${phase}-${database}`"
            >
              Paste the LLM's answer
            </label>
            <textarea
              :id="`llm-answer-${phase}-${database}`"
              v-model="answer"
              class="form-control llm-help-answer"
              rows="5"
              placeholder="Paste the JSON answer here; code fences and surrounding text are fine"
            />
            <button
              type="button"
              class="btn btn-sm btn-primary llm-help-import"
              :disabled="importing || !answer.trim()"
              @click="importAnswer"
            >
              <i
                class="fas"
                :class="importing ? 'fa-spinner fa-spin' : 'fa-file-import'"
              />
              Import answer
            </button>
            <span
              v-if="importError"
              class="llm-help-error"
              role="alert"
            >{{ importError }}</span>
            <div
              v-if="importResult"
              class="llm-help-import-result"
            >
              <span>{{ importSummary }}</span>
              <ul v-if="importResult.messages?.length">
                <li
                  v-for="message in importResult.messages"
                  :key="message"
                >
                  {{ message }}
                </li>
              </ul>
            </div>
          </div>
        </div>
        <div class="llm-prompt-modal-footer">
          <button
            type="button"
            class="btn btn-sm btn-primary llm-help-copy"
            :disabled="!acknowledged"
            :title="acknowledged ? null : 'Tick the acknowledgement first'"
            @click="copyCurrent"
          >
            <i class="fas fa-copy" /> Copy prompt
          </button>
          <button
            type="button"
            class="btn btn-sm btn-outline-secondary llm-help-download"
            :disabled="!acknowledged"
            :title="acknowledged ? null : 'Tick the acknowledgement first'"
            @click="downloadCurrent"
          >
            <i class="fas fa-download" /> Download .txt
          </button>
          <button
            type="button"
            class="btn btn-sm btn-link llm-prompt-modal-close-footer"
            @click="closeModal"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
/* display:contents unwraps the root: the toggle button joins the
   per-database button row (next to "Dismiss all suggestions"), and the
   open body drops in below that row. */
.llm-help-panel {
  display: contents;
}

.llm-help-toggle {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
  margin-left: 0.75rem;
  font-size: 0.8em;
}

.llm-help-body {
  margin-top: 0.5rem;
  padding: 0.75rem 0.9rem;
  border-left: 4px solid rgba(118, 75, 162, 0.75);
  background: rgba(118, 75, 162, 0.06);
  border-radius: 4px;
  font-size: 0.9rem;
}

.llm-help-intro {
  margin-bottom: 0.6rem;
}

.llm-help-options {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.5rem 0.75rem;
  margin-bottom: 0.4rem;
}

.llm-help-option {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  margin: 0;
}

.llm-help-chunk-size {
  width: auto;
  display: inline-block;
}

.llm-help-error {
  color: #b02a37;
}

.llm-help-summary {
  margin-bottom: 0.3rem;
  font-weight: 500;
}

.llm-help-paste {
  margin-top: 0.9rem;
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
  align-items: flex-start;
}

.llm-help-paste-label {
  font-weight: 500;
  margin: 0;
}

.llm-help-answer {
  width: 100%;
  font-family: monospace;
  font-size: 0.8rem;
}

.llm-help-import-result {
  font-size: 0.85rem;
}

.llm-help-import-result ul {
  margin: 0.25rem 0 0;
  color: #6c757d;
}

/* The prompt modal: a fixed overlay of our own, since the page loads
   Bootstrap's CSS but not its JavaScript. */
.llm-prompt-modal {
  position: fixed;
  inset: 0;
  z-index: 1080;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1rem;
}

.llm-prompt-modal-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
}

.llm-prompt-modal-dialog {
  position: relative;
  display: flex;
  flex-direction: column;
  width: min(60rem, 100%);
  max-height: calc(100vh - 2rem);
  background: #fff;
  border-radius: 6px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
  font-size: 0.9rem;
}

.llm-prompt-modal-header,
.llm-prompt-modal-footer {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.6rem 1rem;
}

.llm-prompt-modal-header {
  border-bottom: 1px solid #dee2e6;
}

.llm-prompt-modal-footer {
  border-top: 1px solid #dee2e6;
}

.llm-prompt-modal-title {
  flex: 1;
  margin: 0;
  font-size: 1rem;
}

.llm-prompt-modal-close {
  border: none;
  background: none;
  font-size: 1.4rem;
  line-height: 1;
  cursor: pointer;
}

.llm-prompt-modal-body {
  overflow: auto;
  padding: 0.75rem 1rem;
}

.llm-prompt-parts {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.4rem;
  margin-bottom: 0.6rem;
}

.llm-prompt-modal-heading {
  margin: 0 0 0.3rem;
  font-size: 0.95rem;
  color: #5a3c82;
}

/* The user's own names or values: the part to read before copying. */
.llm-prompt-modal-data {
  max-height: 40vh;
  overflow: auto;
  margin: 0 0 0.5rem;
  padding: 0.5rem 0.75rem;
  border-left: 4px solid #764ba2;
  background: rgba(118, 75, 162, 0.08);
  font-family: monospace;
  font-size: 0.8rem;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.llm-prompt-modal-left-out {
  margin-bottom: 0.4rem;
  color: #842029;
  font-size: 0.85rem;
}

.llm-prompt-modal-privacy {
  margin-bottom: 0.5rem;
  color: #5a3c82;
  font-size: 0.85rem;
}

.llm-prompt-modal-full {
  margin-bottom: 0.6rem;
  font-size: 0.85rem;
}

.llm-help-preview {
  width: 100%;
  margin-top: 0.3rem;
  font-family: monospace;
  font-size: 0.8rem;
}

.llm-prompt-ack {
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  margin: 0;
  font-weight: 500;
}

.llm-prompt-ack-input {
  flex: none;
  margin: 0.2rem 0 0;
}

/* The paste-back sits at the bottom of the modal body, set apart from the
   acknowledgement, so a multi-part round trip never leaves the modal. */
.llm-prompt-modal-paste {
  width: 100%;
  margin-top: 0.9rem;
  padding-top: 0.7rem;
  border-top: 1px dashed #ced4da;
}
</style>
