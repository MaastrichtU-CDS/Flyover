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
 * describe pages (issue 2, prompt export + paste-back round trip).
 *
 * Collapsed by default. Open, it generates a prompt for one database and
 * phase on the server (the browser's semantic map goes along so the
 * "already mapped" context matches what the user sees), shows what the
 * prompt contains and the privacy notice, offers copy / download per
 * chunk, and takes the LLM's answer back through the store's ingest():
 * the server validates it and the imported records render as ordinary
 * pasted_llm suggestion pills that need the same explicit review.
 *
 * Works everywhere: copying tries the async clipboard API (secure
 * contexts only), falls back to a selected textarea + execCommand, and
 * every chunk can also be downloaded as a .txt or read from a preview.
 * The chunk size is the user's choice so small context windows work.
 *
 * Props:
 *   phase — 'variables' or 'values'.
 *   database — the store database name the panel is for.
 *
 * Emits:
 *   ingested — after a successful import; payload is the ingest summary.
 */

import { computed, reactive, ref } from 'vue'
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
const previewOpen = reactive({})
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

// The pre-generation notice describes the phase; once a prompt has been
// generated the server's notice wins (the variables prompt shares no
// values; the values one shares the distinct values being mapped).
const privacyNotice = computed(() => {
  if (prompt.value?.privacy) return prompt.value.privacy
  return props.phase === 'values'
    ? 'The prompt contains variable keys and labels, their term keys, your column names and the distinct values being mapped. It contains no data rows.'
    : 'The prompt contains variable keys and labels and your column names. It contains no data rows.'
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
    return props.phase === 'values'
      ? 'Every value of the mapped categorical columns in this database is already mapped; there is nothing to ask an LLM.'
      : 'Every column of this database is already mapped; there is nothing to ask an LLM.'
  }
  const parts = [`${p.item_count} ${itemLabel.value} still to map`]
  if (p.already_mapped) parts.push(`${p.already_mapped} column${p.already_mapped === 1 ? '' : 's'} already mapped and shown as context`)
  if (p.chunks?.length > 1) parts.push(`split into ${p.chunks.length} parts of at most ${p.chunk_hint} ${itemLabel.value}; send each part on its own`)
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
  } catch (err) {
    generateError.value = err?.message || 'Could not generate the prompt.'
  } finally {
    generating.value = false
  }
}

async function copyChunk(item) {
  const ok = await copyText(item.prompt)
  if (ok) {
    status.success(
      prompt.value.chunks.length > 1
        ? `Part ${item.index} of ${prompt.value.chunks.length} copied — paste it into your LLM client`
        : 'Prompt copied — paste it into your LLM client',
    )
  } else {
    previewOpen[item.index] = true
    status.warning('Copying is blocked in this browser; select the text in the preview below or download it.')
  }
}

function downloadChunk(item) {
  const name = `flyover-prompt-${props.phase}-${props.database}${
    prompt.value.chunks.length > 1 ? `-part${item.index}` : ''
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

function togglePreview(index) {
  previewOpen[index] = !previewOpen[index]
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
        No language model is currently running inside Flyover.<br>
        You can generate a prompt below and copy this into an LLM that you are allowed to use within your institution.<br>
        The LLM's answer should help with your mapping.<br>
        Carefully review its suggestions; nothing is saved until you accept.
      </p>
      <p class="llm-help-privacy">
        <i class="fas fa-shield-halved" />
        {{ privacyNotice }} Review it before sending.
      </p>


      <div class="llm-help-options">
        <label class="llm-help-option">
          Adjust the items per prompt to the limits of the LLM available to you; items per prompt
          <select
            v-model.number="chunk"
            class="form-select form-select-sm llm-help-chunk-size"
            title="Smaller parts fit models with a small context window"
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
      </div>

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
      <span
        v-if="generateError"
        class="llm-help-error"
        role="alert"
      >{{ generateError }}</span>

      <div
        v-if="prompt"
        class="llm-help-result"
      >
        <p class="llm-help-summary">
          {{ summary }}
        </p>
        <details
          v-if="prompt.item_count"
          class="llm-help-contains"
        >
          <summary>What leaves the browser</summary>
          <ul>
            <li
              v-for="entry in prompt.contains"
              :key="entry"
            >
              {{ entry }}
            </li>
          </ul>
        </details>
        <div
          v-for="item in prompt.chunks"
          :key="item.index"
          class="llm-help-chunk"
        >
          <span class="llm-help-chunk-label">
            <template v-if="prompt.chunks.length > 1">Part {{ item.index }} of {{ prompt.chunks.length }} · </template>
            {{ item.item_count }} {{ itemLabel }}
          </span>
          <button
            type="button"
            class="btn btn-sm btn-outline-primary llm-help-copy"
            @click="copyChunk(item)"
          >
            <i class="fas fa-copy" /> Copy prompt
          </button>
          <button
            type="button"
            class="btn btn-sm btn-outline-secondary llm-help-download"
            @click="downloadChunk(item)"
          >
            <i class="fas fa-download" /> Download .txt
          </button>
          <button
            type="button"
            class="btn btn-sm btn-link llm-help-preview-toggle"
            @click="togglePreview(item.index)"
          >
            {{ previewOpen[item.index] ? 'Hide' : 'Show' }} prompt
          </button>
          <textarea
            v-if="previewOpen[item.index]"
            class="form-control llm-help-preview"
            readonly
            rows="12"
            :value="item.prompt"
          />
        </div>
      </div>

      <div class="llm-help-paste">
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
  margin-bottom: 0.4rem;
}

.llm-help-privacy {
  margin-bottom: 0.6rem;
  color: #5a3c82;
  font-size: 0.85rem;
}

.llm-help-options {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem 1.5rem;
  margin-bottom: 0.6rem;
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
  margin-left: 0.5rem;
  color: #b02a37;
}

.llm-help-result {
  margin-top: 0.75rem;
}

.llm-help-summary {
  margin-bottom: 0.3rem;
  font-weight: 500;
}

.llm-help-contains {
  margin-bottom: 0.5rem;
  font-size: 0.85rem;
}

.llm-help-contains ul {
  margin: 0.25rem 0 0;
}

.llm-help-chunk {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.5rem;
  margin: 0.35rem 0;
}

.llm-help-chunk-label {
  min-width: 9rem;
}

.llm-help-preview {
  width: 100%;
  font-family: monospace;
  font-size: 0.8rem;
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
</style>
