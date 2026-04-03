/**
 * Describe Variables App - Vue.js 3 Component
 *
 * Replaces the DescribeVariablesPage module with a reactive Vue 3 application
 * using the Options API. Handles variable description page functionality
 * including pagination, form state caching, semantic mapping, and IndexedDB
 * persistence.
 *
 * Uses [[ ]] delimiters to avoid conflicts with Jinja2 {{ }} syntax.
 * Mounts to the #describe-variables-app element in the DOM.
 *
 * @requires Vue 3 CDN (loaded in HTML template)
 * @requires jQuery (for Bootstrap selectpicker interactions)
 * @requires FlyoverDB (from db-utils.js)
 * @requires JSONLDMapper (from jsonld-mapper.js)
 */

/** @const {number} Duration in ms to show auto-fill feedback */
const AUTO_FILL_FEEDBACK_MS = 3000;

/** @const {number} Duration in ms to show manual override feedback */
const OVERRIDE_FEEDBACK_MS = 2000;

const DescribeVariablesApp = Vue.createApp({
    delimiters: ['[[', ']]'],

    /**
     * Component template containing database sections, variable rows with
     * pagination, and submit button. Uses Vue reactive bindings.
     */
    template: `
    <form class="form-horizontal" method="POST" action="units" @submit="onFormSubmit">
        <hr />

        <div id="databases-container">
            <div v-for="dbName in databaseNames" :key="dbName">
                <h2 style="display: inline-block;">
                    <i class="fas fa-database"></i> [[ dbName ]]
                </h2>
                <button class="toggle-button" type="button"
                        :class="{ open: expandedDatabases[dbName] }"
                        @click="toggleDatabase(dbName)">
                    <span class="toggle-text">
                        [[ expandedDatabases[dbName] ? 'Show less' : 'Show more' ]]
                    </span>
                    <svg class="angle-icon" xmlns="http://www.w3.org/2000/svg"
                         viewBox="0 0 448 512" width="20" height="20">
                        <path :d="expandedDatabases[dbName] ? anglePaths.down : anglePaths.up" />
                    </svg>
                </button>

                <div class="content"
                     :class="{ active: expandedDatabases[dbName], hidden: !expandedDatabases[dbName] }"
                     :data-database="dbName">
                    <div class="variables-container"
                         :id="'variables-' + dbName"
                         :data-columns="JSON.stringify(columnInfoData[dbName])"
                         :data-total="columnInfoData[dbName].length">

                        <div v-for="item in currentPageItems(dbName)" :key="dbName + '_' + item"
                             class="variable-row">
                            <div class="variable-label">[[ item ]]</div>
                            <div class="variable-controls">
                                <select :id="'ncit_comment_' + dbName + '_' + item"
                                        :name="'ncit_comment_' + dbName + '_' + item"
                                        class="selectpicker description-select"
                                        data-live-search="true"
                                        :data-database="dbName"
                                        :data-item="item">
                                    <option value="">Description</option>
                                    <option value="Other">Other</option>
                                    <option v-for="name in globalVariableNames" :key="name"
                                            :value="name">[[ name ]]</option>
                                </select>

                                <input class="form-control" type="text"
                                       :id="'comment_' + dbName + '_' + item"
                                       :name="'comment_' + dbName + '_' + item"
                                       placeholder="If other, please specify"
                                       :disabled="getDescriptionValue(dbName, item) !== 'Other'"
                                       :value="getCommentValue(dbName, item)" />

                                <div class="datatype-container">
                                    <select :id="dbName + '_' + item"
                                            :name="dbName + '_' + item"
                                            class="selectpicker datatype-select"
                                            data-live-search="true"
                                            :data-database="dbName"
                                            :data-item="item">
                                        <option value="">Data type</option>
                                        <option value="categorical">Categorical</option>
                                        <option value="continuous">Continuous</option>
                                        <option value="identifier">Identifier</option>
                                        <option value="standardised">Standardised</option>
                                    </select>
                                    <small class="datatype-feedback"
                                           :id="'feedback_' + dbName + '_' + item"
                                           style="display: none; color: #28a745;">
                                        <i class="fas fa-magic"></i> Auto-filled based on description
                                    </small>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div v-if="totalPages(dbName) > 1" class="pagination-controls">
                        <button type="button" class="prev-btn"
                                :disabled="(databasePages[dbName] || 1) <= 1"
                                @click="changePage(dbName, -1)">
                            &#x2190;
                        </button>
                        <span class="page-indicator">
                            Page <span :id="'current-page-' + dbName">
                                [[ databasePages[dbName] || 1 ]]
                            </span> of [[ totalPages(dbName) ]]
                        </span>
                        <button type="button" class="next-btn"
                                :disabled="(databasePages[dbName] || 1) >= totalPages(dbName)"
                                @click="changePage(dbName, 1)">
                            &#x2192;
                        </button>
                    </div>
                </div>
                <hr>
            </div>
        </div>

        <!-- Hidden inputs for cached state not on current page -->
        <template v-for="(cached, key) in hiddenFieldEntries" :key="'hidden-' + key">
            <input v-if="cached.description" type="hidden"
                   :name="'ncit_comment_' + key"
                   :value="cached.description" />
            <input v-if="cached.datatype" type="hidden"
                   :name="key"
                   :value="cached.datatype" />
            <input v-if="cached.comment" type="hidden"
                   :name="'comment_' + key"
                   :value="cached.comment" />
        </template>

        <!-- Hidden inputs for preselected values not yet cached or rendered -->
        <template v-for="(desc, key) in preselectedHiddenEntries" :key="'pre-' + key">
            <input type="hidden"
                   :name="'ncit_comment_' + key"
                   :value="desc" />
            <input v-if="preselectedDatatypes[key]" type="hidden"
                   :name="key"
                   :value="preselectedDatatypes[key]" />
        </template>

        <p>
            <button type="submit" id="submitBtn" class="btn btn-primary"
                    :disabled="!hasAnyDescription"
                    :class="{ processing: isSubmitting }">
                <template v-if="!isSubmitting">
                    <i class="fas fa-play"></i> Submit
                </template>
                <template v-else>
                    <i class="fas loading-icon"
                       :class="loadingIconClass"></i> Processing descriptions...
                </template>
            </button>
        </p>
    </form>
    `,

    /**
     * Reactive state for the describe variables page.
     * @returns {Object} Component data
     */
    data() {
        return {
            /** @type {number} Number of variables per page */
            PAGE_SIZE: 10,

            /** @type {Object<string, number>} Current page number per database */
            databasePages: {},

            /** @type {Object<string, Object>} Cached form state keyed by "database_item" */
            formStateCache: {},

            /** @type {Object|null} Column info organized by database name */
            columnInfoData: null,

            /** @type {Object<string, string>} Map from description name to suggested datatype */
            descriptionToDatatype: {},

            /** @type {Object<string, string>} Preselected datatypes from JSONLDMapper */
            preselectedDatatypes: {},

            /** @type {Set<string>} Fields whose datatypes were auto-filled */
            autoFilledFields: new Set(),

            /** @type {Set<string>} Fields whose datatypes were manually overridden */
            manualOverrides: new Set(),

            /** @type {Object<string, Object>} Selected descriptions per database */
            selectedDescriptions: {},

            /** @type {number|null} Interval ID for loading animation */
            loadingInterval: null,

            /** @type {Object<string, boolean>} Expanded/collapsed state per database */
            expandedDatabases: {},

            /** @type {Array<string>} Global variable names from JSONLDMapper */
            globalVariableNames: [],

            /** @type {Object<string, string>} Preselected descriptions from JSONLDMapper */
            preselectedDescriptions: {},

            /** @type {boolean} Whether form is being submitted */
            isSubmitting: false,

            /** @type {boolean} Toggle for loading icon animation */
            loadingIconIsPen: false,

            /** SVG path data for chevron angles */
            anglePaths: {
                up: 'M201.4 137.4c12.5-12.5 32.8-12.5 45.3 0l160 160c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L224 205.3 86.6 342.6c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3l160-160z',
                down: 'M201.4 374.6c12.5 12.5 32.8 12.5 45.3 0l160-160c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0L224 306.7 86.6 169.4c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3l160 160z'
            }
        };
    },

    computed: {
        /**
         * Sorted database names from column info.
         * @returns {Array<string>}
         */
        databaseNames() {
            if (!this.columnInfoData) return [];
            return Object.keys(this.columnInfoData);
        },

        /**
         * Whether any description has been selected (enables submit button).
         * @returns {boolean}
         */
        hasAnyDescription() {
            // Check visible selects on current pages
            for (const dbName of this.databaseNames) {
                const items = this.currentPageItems(dbName);
                for (const item of items) {
                    const key = `${dbName}_${item}`;
                    const cached = this.formStateCache[key];
                    if (cached && cached.description) return true;
                }
            }

            // Check cached state from other pages
            for (const key in this.formStateCache) {
                if (this.formStateCache[key].description) return true;
            }

            // Check preselected descriptions
            for (const key in this.preselectedDescriptions) {
                if (this.preselectedDescriptions[key]) return true;
            }

            return false;
        },

        /**
         * Cached form entries not rendered on any current page (for hidden inputs).
         * @returns {Object<string, Object>}
         */
        hiddenFieldEntries() {
            const visibleKeys = new Set();
            for (const dbName of this.databaseNames) {
                const items = this.currentPageItems(dbName);
                for (const item of items) {
                    visibleKeys.add(`${dbName}_${item}`);
                }
            }

            const hidden = {};
            for (const [key, cached] of Object.entries(this.formStateCache)) {
                if (!visibleKeys.has(key)) {
                    hidden[key] = cached;
                }
            }
            return hidden;
        },

        /**
         * Preselected entries not cached or rendered (for hidden inputs on submit).
         * @returns {Object<string, string>}
         */
        preselectedHiddenEntries() {
            const visibleKeys = new Set();
            for (const dbName of this.databaseNames) {
                const items = this.currentPageItems(dbName);
                for (const item of items) {
                    visibleKeys.add(`${dbName}_${item}`);
                }
            }

            const hidden = {};
            for (const [key, desc] of Object.entries(this.preselectedDescriptions)) {
                if (!visibleKeys.has(key) && !this.formStateCache[key]) {
                    hidden[key] = desc;
                }
            }
            return hidden;
        },

        /**
         * CSS class for the loading animation icon.
         * @returns {string}
         */
        loadingIconClass() {
            return this.loadingIconIsPen ? 'fa-pen' : 'fa-edit';
        }
    },

    methods: {
        /**
         * Initialize the app: load IndexedDB, column info, semantic mapping.
         * @param {Object} backendColumnInfo - Column info from the server
         */
        async init(backendColumnInfo) {
            try {
                await this.initializeIndexedDB();
            } catch (e) {
                console.error('Failed to initialize IndexedDB:', e);
            }

            if (backendColumnInfo && Object.keys(backendColumnInfo).length > 0) {
                this.columnInfoData = backendColumnInfo;
                try {
                    await this.saveColumnInfo(backendColumnInfo);
                } catch (e) {
                    console.error('Failed to save column info:', e);
                }
            } else {
                this.columnInfoData = await this.loadColumnInfo();
            }

            if (this.columnInfoData) {
                for (const dbName of Object.keys(this.columnInfoData)) {
                    this.databasePages[dbName] = 1;
                    this.expandedDatabases[dbName] = false;
                }
            }

            await this.loadAndApplySemanticMapping();

            // Apply preselections and initialize selectpickers after DOM render
            this.$nextTick(() => {
                this.initializeAllSelectpickers();
                this.applyAllPreselections();
            });
        },

        /**
         * Initialize IndexedDB via FlyoverDB.
         */
        async initializeIndexedDB() {
            if (FlyoverDB.isSupported()) {
                await FlyoverDB.initDB();
            }
        },

        /**
         * Save column info to IndexedDB.
         * @param {Object} data - Column info data
         */
        async saveColumnInfo(data) {
            await FlyoverDB.saveData('metadata', {
                key: 'column_info',
                data: data,
                timestamp: new Date().toISOString()
            });
        },

        /**
         * Load column info from IndexedDB.
         * @returns {Object|null} Column info data
         */
        async loadColumnInfo() {
            const result = await FlyoverDB.getData('metadata', 'column_info');
            if (result && result.data) {
                return result.data;
            }
            return null;
        },

        /**
         * Load and apply semantic mapping from IndexedDB via JSONLDMapper.
         */
        async loadAndApplySemanticMapping() {
            try {
                await JSONLDMapper.loadFromIndexedDB();
                this.globalVariableNames = JSONLDMapper.getGlobalVariableNames()
                    .filter(name => name !== 'Other');

                const preselections = JSONLDMapper.computePreselections();
                this.preselectedDescriptions = preselections.preselectedDescriptions || {};
                this.preselectedDatatypes = preselections.preselectedDatatypes || {};
                this.descriptionToDatatype = preselections.descriptionToDatatype || {};
            } catch (error) {
                console.error('Failed to load semantic mapping:', error);
            }
        },

        /**
         * Get variables for the current page of a database.
         * @param {string} dbName - Database name
         * @returns {Array<string>} Column names for the current page
         */
        currentPageItems(dbName) {
            if (!this.columnInfoData || !this.columnInfoData[dbName]) return [];
            const columns = this.columnInfoData[dbName];
            const page = this.databasePages[dbName] || 1;
            const start = (page - 1) * this.PAGE_SIZE;
            const end = Math.min(start + this.PAGE_SIZE, columns.length);
            return columns.slice(start, end);
        },

        /**
         * Calculate total pages for a database.
         * @param {string} dbName - Database name
         * @returns {number} Total number of pages
         */
        totalPages(dbName) {
            if (!this.columnInfoData || !this.columnInfoData[dbName]) return 1;
            return Math.ceil(this.columnInfoData[dbName].length / this.PAGE_SIZE);
        },

        /**
         * Get cached description value for a variable.
         * @param {string} dbName - Database name
         * @param {string} item - Column name
         * @returns {string} Cached description or empty string
         */
        getDescriptionValue(dbName, item) {
            const key = `${dbName}_${item}`;
            const cached = this.formStateCache[key];
            if (cached && cached.description) return cached.description;
            if (this.preselectedDescriptions[key]) return this.preselectedDescriptions[key];
            return '';
        },

        /**
         * Get cached comment value for a variable.
         * @param {string} dbName - Database name
         * @param {string} item - Column name
         * @returns {string} Cached comment or empty string
         */
        getCommentValue(dbName, item) {
            const key = `${dbName}_${item}`;
            const cached = this.formStateCache[key];
            return (cached && cached.comment) ? cached.comment : '';
        },

        /**
         * Toggle database section visibility.
         * @param {string} dbName - Database name
         */
        toggleDatabase(dbName) {
            if (this.expandedDatabases[dbName]) {
                // Collapsing: cache current page state
                this.cacheCurrentPageState(dbName);
            }
            this.expandedDatabases[dbName] = !this.expandedDatabases[dbName];

            if (this.expandedDatabases[dbName]) {
                this.$nextTick(() => {
                    this.initializeSelectpickersForDatabase(dbName);
                    this.applyPreselectionsToCurrentPage(dbName);
                    this.updateDescriptionOptions();
                });
            }
        },

        /**
         * Change the current page for a database.
         * @param {string} dbName - Database name
         * @param {number} direction - Direction (+1 or -1)
         */
        changePage(dbName, direction) {
            const currentPage = this.databasePages[dbName] || 1;
            const newPage = currentPage + direction;
            const total = this.totalPages(dbName);

            if (newPage < 1 || newPage > total) return;

            this.cacheCurrentPageState(dbName);
            this.databasePages[dbName] = newPage;

            this.$nextTick(() => {
                this.initializeSelectpickersForDatabase(dbName);
                this.applyPreselectionsToCurrentPage(dbName);
                this.updateDescriptionOptions();
            });
        },

        /**
         * Cache the form state of the currently visible variables for a database.
         * Reads DOM values via jQuery since selectpicker manages its own state.
         * @param {string} dbName - Database name
         */
        cacheCurrentPageState(dbName) {
            const self = this;
            $(`#variables-${dbName} .variable-row`).each(function () {
                const row = $(this);
                const descSelect = row.find('.description-select');
                const datatypeSelect = row.find('.datatype-select');
                const item = descSelect.data('item');
                if (!item) return;

                const key = `${dbName}_${item}`;
                const domDescription = descSelect.val();
                const domDatatype = datatypeSelect.val();
                const domComment = $(`#comment_${dbName}_${item}`).val();

                const existing = self.formStateCache[key] || {};
                self.formStateCache[key] = {
                    database: dbName,
                    description: domDescription || existing.description || '',
                    datatype: domDatatype || existing.datatype || '',
                    comment: domComment || existing.comment || ''
                };
            });

            this.syncToIndexedDB();
        },

        /**
         * Sync form state cache to IndexedDB via JSONLDMapper.
         */
        async syncToIndexedDB() {
            try {
                await JSONLDMapper.updateMappingFromForm(this.formStateCache);
            } catch (error) {
                console.error('Failed to sync to IndexedDB:', error);
            }
        },

        /**
         * Initialize selectpickers for all databases.
         */
        initializeAllSelectpickers() {
            try {
                const $pickers = $('.selectpicker');
                if ($pickers.length > 0) {
                    $pickers.selectpicker();
                }
            } catch (e) {
                console.error('Failed to initialize selectpickers:', e);
            }
            this.attachAllEventHandlers();
        },

        /**
         * Initialize selectpickers for a specific database section.
         * @param {string} dbName - Database name
         */
        initializeSelectpickersForDatabase(dbName) {
            const container = $(`#variables-${dbName}`);
            try {
                const $pickers = container.find('.selectpicker');
                if ($pickers.length > 0) {
                    $pickers.selectpicker();
                }
            } catch (e) {
                console.error('Failed to initialize selectpicker:', e);
            }
            this.restoreCachedValues(dbName);
            this.attachEventHandlersForDatabase(dbName);
        },

        /**
         * Restore cached selectpicker values for a database page.
         * @param {string} dbName - Database name
         */
        restoreCachedValues(dbName) {
            const items = this.currentPageItems(dbName);
            for (const item of items) {
                const key = `${dbName}_${item}`;
                const cached = this.formStateCache[key];
                if (cached) {
                    if (cached.description) {
                        try {
                            $(`#ncit_comment_${dbName}_${item}`).selectpicker('val', cached.description);
                        } catch (e) { /* selectpicker not ready */ }
                    }
                    if (cached.datatype) {
                        try {
                            $(`#${dbName}_${item}`).selectpicker('val', cached.datatype);
                        } catch (e) { /* selectpicker not ready */ }
                    }
                    if (cached.comment) {
                        $(`#comment_${dbName}_${item}`).val(cached.comment);
                        $(`#comment_${dbName}_${item}`).prop('disabled', false);
                    }
                }
            }
        },

        /**
         * Attach event handlers for all database sections.
         */
        attachAllEventHandlers() {
            for (const dbName of this.databaseNames) {
                this.attachEventHandlersForDatabase(dbName);
            }
        },

        /**
         * Attach selectpicker change event handlers for a database section.
         * @param {string} dbName - Database name
         */
        attachEventHandlersForDatabase(dbName) {
            const self = this;
            const container = $(`#variables-${dbName}`);

            // Unbind first to avoid duplicate handlers
            container.find('.description-select').off('changed.bs.select');
            container.find('.datatype-select').off('changed.bs.select');
            container.find('input[id^="comment_"]').off('change');

            container.find('.description-select').on('changed.bs.select', function () {
                const db = $(this).data('database');
                const item = $(this).data('item');
                const commentBox = $(`#comment_${db}_${item}`);
                const key = `${db}_${item}`;

                if ($(this).val() === 'Other') {
                    commentBox.removeAttr('disabled');
                } else {
                    commentBox.attr('disabled', 'disabled');
                }

                if (!self.formStateCache[key]) {
                    self.formStateCache[key] = {};
                }
                self.formStateCache[key].database = db;
                self.formStateCache[key].description = $(this).val();

                self.autoPopulateDatatype(db, item);
                self.updateDescriptionOptions();
                self.syncToIndexedDB();
            });

            container.find('.datatype-select').on('changed.bs.select', function () {
                const db = $(this).data('database');
                const item = $(this).data('item');
                const fieldKey = `${db}_${item}`;

                if (!self.formStateCache[fieldKey]) {
                    self.formStateCache[fieldKey] = {};
                }
                self.formStateCache[fieldKey].database = db;
                self.formStateCache[fieldKey].datatype = $(this).val();

                if (self.autoFilledFields.has(fieldKey)) {
                    self.manualOverrides.add(fieldKey);
                    self.showOverrideConfirmation(db, item);
                }

                self.syncToIndexedDB();
            });

            container.find('input[id^="comment_"]').on('change', function () {
                const id = $(this).attr('id');
                const parts = id.replace('comment_', '').split('_');
                const item = parts.pop();
                const db = parts.join('_');
                const key = `${db}_${item}`;

                if (!self.formStateCache[key]) {
                    self.formStateCache[key] = {};
                }
                self.formStateCache[key].database = db;
                self.formStateCache[key].comment = $(this).val();

                self.syncToIndexedDB();
            });
        },

        /**
         * Auto-populate datatype based on selected description.
         * @param {string} database - Database name
         * @param {string} item - Column name
         */
        autoPopulateDatatype(database, item) {
            const descriptionSelect = $(`#ncit_comment_${database}_${item}`);
            const datatypeSelect = $(`#${database}_${item}`);
            const feedback = $(`#feedback_${database}_${item}`);
            const fieldKey = `${database}_${item}`;
            const selectedDescription = descriptionSelect.val();

            if (selectedDescription && selectedDescription !== '' && selectedDescription !== 'Other') {
                const suggestedDatatype = this.preselectedDatatypes[fieldKey]
                    || this.descriptionToDatatype[selectedDescription];

                if (suggestedDatatype) {
                    if (datatypeSelect.val() === '' || !this.manualOverrides.has(fieldKey)) {
                        try {
                            if (datatypeSelect.length > 0) {
                                datatypeSelect.selectpicker('val', suggestedDatatype);
                            }
                        } catch (e) {
                            console.error('Failed to set datatype value:', e);
                        }

                        // Update cache
                        if (!this.formStateCache[fieldKey]) {
                            this.formStateCache[fieldKey] = {};
                        }
                        this.formStateCache[fieldKey].datatype = suggestedDatatype;

                        this.autoFilledFields.add(fieldKey);
                        feedback.show();
                        setTimeout(function () {
                            feedback.fadeOut();
                        }, AUTO_FILL_FEEDBACK_MS);
                    }
                }
            } else {
                if (this.autoFilledFields.has(fieldKey) && !this.manualOverrides.has(fieldKey)) {
                    try {
                        if (datatypeSelect.length > 0) {
                            datatypeSelect.selectpicker('val', '');
                        }
                    } catch (e) {
                        console.error('Failed to clear datatype value:', e);
                    }
                    this.autoFilledFields.delete(fieldKey);
                    feedback.hide();
                }
            }
        },

        /**
         * Show manual override confirmation feedback.
         * @param {string} database - Database name
         * @param {string} item - Column name
         */
        showOverrideConfirmation(database, item) {
            const feedback = $(`#feedback_${database}_${item}`);
            feedback.html('<i class="fas fa-user-edit"></i> Manually overridden')
                .css('color', '#ffc107').show();
            setTimeout(function () {
                feedback.fadeOut();
            }, OVERRIDE_FEEDBACK_MS);
        },

        /**
         * Build the selected descriptions map for duplicate detection.
         */
        buildSelectedDescriptions() {
            this.selectedDescriptions = {};

            for (const key in this.formStateCache) {
                const cached = this.formStateCache[key];
                const desc = cached.description;
                const db = cached.database;
                if (desc && desc !== '' && desc !== 'Other' && db) {
                    if (!this.selectedDescriptions[db]) this.selectedDescriptions[db] = {};
                    this.selectedDescriptions[db][desc] = key;
                }
            }

            const self = this;
            $('select.description-select').each(function () {
                const $select = $(this);
                const db = $select.data('database');
                const item = $select.data('item');
                const key = `${db}_${item}`;
                const val = $select.val();

                if (val && val !== '' && val !== 'Other') {
                    if (!self.selectedDescriptions[db]) self.selectedDescriptions[db] = {};
                    if (!self.selectedDescriptions[db][val]) {
                        self.selectedDescriptions[db][val] = key;
                    }
                }
            });
        },

        /**
         * Update description options to disable already-selected descriptions.
         */
        updateDescriptionOptions() {
            this.buildSelectedDescriptions();
            const self = this;

            $('select.description-select').each(function () {
                const $select = $(this);
                const db = $select.data('database');
                const item = $select.data('item');
                const currentKey = `${db}_${item}`;
                const dbSelections = self.selectedDescriptions[db] || {};

                $select.find('option').each(function () {
                    const optVal = $(this).val();
                    if (optVal === '' || optVal === 'Other') return;

                    if (dbSelections[optVal] && dbSelections[optVal] !== currentKey) {
                        $(this).prop('disabled', true);
                    } else {
                        $(this).prop('disabled', false);
                    }
                });

                $select.selectpicker('refresh');
            });
        },

        /**
         * Apply preselections from JSONLDMapper to a database's current page.
         * @param {string} database - Database name
         */
        applyPreselectionsToCurrentPage(database) {
            const preselections = JSONLDMapper.computePreselections();
            this.descriptionToDatatype = preselections.descriptionToDatatype;
            this.preselectedDatatypes = preselections.preselectedDatatypes;
            this.preselectedDescriptions = preselections.preselectedDescriptions;

            $(`#variables-${database} .description-select`).each((_, el) => {
                const $select = $(el);
                const db = $select.data('database');
                const item = $select.data('item');
                const key = `${db}_${item}`;

                if (key in this.preselectedDescriptions && !(key in this.formStateCache)) {
                    const value = this.preselectedDescriptions[key];
                    try {
                        if ($select.length > 0) {
                            $select.selectpicker('val', value);
                        }
                    } catch (e) {
                        console.error('Failed to set select value:', e);
                    }
                }
            });

            $(`#variables-${database} .datatype-select`).each((_, el) => {
                const $select = $(el);
                const db = $select.data('database');
                const item = $select.data('item');
                const key = `${db}_${item}`;

                if (key in this.preselectedDatatypes && !(key in this.formStateCache)) {
                    const value = this.preselectedDatatypes[key];
                    try {
                        if ($select.length > 0) {
                            $select.selectpicker('val', value);
                        }
                    } catch (e) {
                        console.error('Failed to set select value:', e);
                    }
                }
            });
        },

        /**
         * Apply preselections to all databases.
         */
        applyAllPreselections() {
            for (const dbName of this.databaseNames) {
                this.applyPreselectionsToCurrentPage(dbName);
            }
            this.updateDescriptionOptions();
        },

        /**
         * Handle form submission. Caches all visible state, then allows
         * the native form POST to proceed. Hidden inputs for cached and
         * preselected values are rendered reactively in the template.
         * @param {Event} e - Submit event
         */
        onFormSubmit(e) {
            // Cache all currently visible page states
            for (const dbName of this.databaseNames) {
                this.cacheCurrentPageState(dbName);
            }

            // Refresh preselections for hidden inputs
            const preselections = JSONLDMapper.computePreselections();
            this.preselectedDescriptions = preselections.preselectedDescriptions || {};
            this.preselectedDatatypes = preselections.preselectedDatatypes || {};

            this.startLoadingAnimation();
        },

        /**
         * Start the loading animation on the submit button.
         */
        startLoadingAnimation() {
            this.isSubmitting = true;
            this.loadingIconIsPen = false;

            const self = this;
            this.loadingInterval = setInterval(function () {
                self.loadingIconIsPen = !self.loadingIconIsPen;
            }, 1000);
        },

        /**
         * Stop the loading animation on the submit button.
         */
        stopLoadingAnimation() {
            if (this.loadingInterval) {
                clearInterval(this.loadingInterval);
                this.loadingInterval = null;
            }
            this.isSubmitting = false;
        }
    },

    /**
     * Lifecycle: after component is mounted, call populateDescriptionOptions
     * to fill selectpickers rendered by the legacy JSONLDMapper.
     */
    mounted() {
        // JSONLDMapper.populateDescriptionOptions may query DOM for selects;
        // after mounting, those selects exist in the Vue-rendered DOM.
        this.$nextTick(() => {
            try {
                JSONLDMapper.populateDescriptionOptions();
            } catch (e) {
                // Silently ignore if mapping not loaded yet; init() handles this.
            }
        });
    },

    /**
     * Lifecycle: clean up intervals on unmount.
     */
    beforeUnmount() {
        this.stopLoadingAnimation();
    }
});

// Export for global access
window.DescribeVariablesApp = DescribeVariablesApp;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DescribeVariablesApp;
}
