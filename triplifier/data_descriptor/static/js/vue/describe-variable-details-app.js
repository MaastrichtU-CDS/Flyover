/**
 * Describe Variable Details App - Vue.js 3 Component
 *
 * Replaces the DescribeVariableDetailsPage module with a reactive Vue 3 application
 * using the Options API. Handles the variable details page functionality including
 * categorical/continuous variable rendering, category selection with selectpicker
 * integration, and form submission with IndexedDB persistence.
 *
 * Uses [[ ]] delimiters to avoid conflicts with Jinja2 {{ }} syntax.
 * Mounts to the #describe-details-app element in the DOM.
 *
 * @requires Vue 3 CDN (loaded in HTML template)
 * @requires jQuery (for Bootstrap selectpicker interactions)
 * @requires FlyoverDB (from db-utils.js)
 * @requires JSONLDMapper (from jsonld-mapper.js)
 */

/** @const {Array<Object>} Default category options when no semantic options are available */
const DEFAULT_CATEGORY_OPTIONS = [
    { value: 'Yes', label: 'Yes' },
    { value: 'No', label: 'No' },
    { value: 'Male', label: 'Male sex' },
    { value: 'Female', label: 'Female sex' },
    { value: 'Primary Education', label: 'Primary education' },
    { value: 'Secondary Education', label: 'Secondary education' },
    { value: 'Tertiary Education', label: 'Tertiary education' },
    { value: 'Missing', label: 'Missing value' },
    { value: 'Other', label: 'Other' }
];

const DescribeVariableDetailsApp = Vue.createApp({
    delimiters: ['[[', ']]'],

    /**
     * Component template containing database sections with toggle, variable rows
     * (continuous/categorical), category selects with selectpicker, and submit/back buttons.
     * The Reference Guide section remains outside Vue in the HTML template.
     */
    template: `
    <form class="form-horizontal" method="POST" action="end" @submit="onFormSubmit">
        <hr />

        <div id="databases-container">
            <div v-for="(db, dbIndex) in parsedDatabases" :key="db.name" class="database-section">
                <h2 style="display: inline-block;">
                    <i class="fas fa-database"></i> [[ db.name ]]
                </h2>
                <button class="toggle-button" type="button"
                        :class="{ open: expandedDatabases[db.name] }"
                        @click="toggleDatabase(db.name)">
                    <span class="toggle-text">
                        [[ expandedDatabases[db.name] ? 'Show less' : 'Show more' ]]
                    </span>
                    <svg class="angle-icon" xmlns="http://www.w3.org/2000/svg"
                         viewBox="0 0 448 512" width="20" height="20">
                        <path :d="expandedDatabases[db.name] ? anglePaths.down : anglePaths.up" />
                    </svg>
                </button>

                <div class="content variables-container"
                     :class="{ active: expandedDatabases[db.name], hidden: !expandedDatabases[db.name] }">

                    <template v-for="(variable, varIdx) in db.variables" :key="db.name + '_' + varIdx">

                        <!-- Continuous variable row -->
                        <div v-if="variable.type === 'continuous'" class="variable-row">
                            <div class="variable-label"
                                 v-html="variable.isMissing
                                     ? '<span class=\\'missing-description\\'>' + variable.displayLabel + '</span>'
                                     : variable.displayLabel">
                            </div>
                            <div class="variable-controls">
                                <input :id="db.name + '_' + variable.localVariable"
                                       type="text"
                                       :name="db.name + '_' + variable.localVariable"
                                       placeholder="Type unit here"
                                       class="form-control" />
                                <input :id="db.name + '_' + variable.localVariable + '_notation'"
                                       type="text"
                                       :name="db.name + '_' + variable.localVariable + '_notation_missing_or_unspecified'"
                                       placeholder="Type missing value notation here"
                                       class="form-control" />
                            </div>
                        </div>

                        <!-- Categorical variable row -->
                        <template v-if="variable.type === 'categorical'">
                            <div class="variable-row">
                                <div class="variable-label"
                                     v-html="variable.isMissing
                                         ? '<span class=\\'missing-description\\'>' + variable.displayLabel + '</span>'
                                         : variable.displayLabel">
                                </div>
                                <div class="variable-controls">
                                    <button class="item-toggle-button" type="button"
                                            :class="{ open: isVariableExpanded(db.name, varIdx) }"
                                            @click="toggleVariable(db.name, varIdx)">
                                    </button>
                                </div>
                            </div>

                            <!-- Categories section -->
                            <div class="toggle-content categorical-section"
                                 :class="{
                                     active: isVariableExpanded(db.name, varIdx),
                                     hidden: !isVariableExpanded(db.name, varIdx)
                                 }">
                                <template v-for="(cat, catIdx) in variable.categories" :key="cat.selectId">
                                    <div class="category-item">
                                        <div class="category-label">
                                            [[ cat.displayValue ]] (counted: [[ cat.count ]])
                                        </div>
                                        <div class="category-controls">
                                            <select :id="cat.selectId"
                                                    :name="cat.selectName"
                                                    class="selectpicker form-control category-select"
                                                    :data-comment-id="cat.commentId"
                                                    :data-database="db.name"
                                                    :data-variable="variable.localVariable"
                                                    :data-global-variable="variable.globalVarName"
                                                    :data-category-value="cat.safeValue"
                                                    :data-previous-selection="cat.preselectedValue"
                                                    data-live-search="true">
                                                <option value="">Description</option>
                                                <template v-if="variable.categoryOptions.length > 0">
                                                    <option v-for="opt in variable.categoryOptions"
                                                            :key="opt" :value="opt">[[ opt ]]</option>
                                                    <option value="Other">Other</option>
                                                </template>
                                                <template v-else>
                                                    <option v-for="opt in defaultCategoryOptions"
                                                            :key="opt.value"
                                                            :value="opt.value">[[ opt.label ]]</option>
                                                </template>
                                            </select>
                                            <input class="form-control" type="text"
                                                   :id="cat.commentId"
                                                   :name="cat.commentName"
                                                   :data-database="db.name"
                                                   :data-variable="variable.localVariable"
                                                   :data-category-value="cat.safeValue"
                                                   placeholder="If other, please specify"
                                                   disabled="disabled"
                                                   style="width: 200px;" />
                                        </div>
                                    </div>
                                    <input type="hidden"
                                           :id="cat.countId"
                                           :name="cat.countName"
                                           :data-database="db.name"
                                           :data-variable="variable.localVariable"
                                           :data-category-value="cat.safeValue"
                                           :value="cat.count" />
                                </template>
                            </div>
                        </template>

                    </template>
                </div>
                <hr>
            </div>
        </div>

        <p>
            <button type="submit" id="submitBtn" class="btn btn-primary"
                    :disabled="isProcessing"
                    :class="{ processing: isProcessing }">
                <template v-if="!isProcessing">
                    <i class="fas fa-play"></i> Submit
                </template>
                <template v-else>
                    <i class="fas loading-icon"
                       :class="loadingIconClass"></i> Processing descriptions...
                </template>
            </button>
            <a href="/describe_variables" class="btn btn-light">
                <i class="fas fa-backward"></i> Back to Describe Variables
            </a>
        </p>
    </form>
    `,

    /**
     * Reactive state for the describe variable details page.
     * @returns {Object} Component data
     */
    data() {
        return {
            /** @type {Object|null} Descriptive info from backend */
            descriptiveInfo: null,

            /** @type {Object|null} Detailed descriptive info from backend */
            descriptiveInfoDetails: null,

            /** @type {Object|null} Preselected values from backend */
            preselectedValues: null,

            /** @type {Object<string, boolean>} Expanded/collapsed state per database */
            expandedDatabases: {},

            /** @type {Object<string, Object<number, boolean>>} Expanded/collapsed state per variable */
            expandedVariables: {},

            /** @type {number|null} Interval ID for loading animation */
            loadingInterval: null,

            /** @type {boolean} Whether form is being submitted */
            isProcessing: false,

            /** @type {boolean} Whether JSONLDMapper has been loaded from IndexedDB */
            mapperLoaded: false,

            /** @type {boolean} Toggle for loading icon animation */
            loadingIconIsPen: false,

            /** @type {Array<Object>} Processed database structures for rendering */
            databases: [],

            /** @type {Array<Object>} Default category options */
            defaultCategoryOptions: DEFAULT_CATEGORY_OPTIONS,

            /** SVG path data for chevron angles */
            anglePaths: {
                up: 'M201.4 137.4c12.5-12.5 32.8-12.5 45.3 0l160 160c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L224 205.3 86.6 342.6c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3l160-160z',
                down: 'M201.4 374.6c12.5 12.5 32.8 12.5 45.3 0l160-160c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0L224 306.7 86.6 169.4c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3l160 160z'
            }
        };
    },

    computed: {
        /**
         * Processed database structures from descriptiveInfoDetails.
         * Transforms the raw backend data into a Vue-friendly array of database objects,
         * each containing processed variable entries with category metadata.
         * @returns {Array<Object>}
         */
        parsedDatabases() {
            if (!this.descriptiveInfoDetails) return [];

            // Depend on mapperLoaded so this re-computes after JSONLDMapper loads
            const _loaded = this.mapperLoaded;

            const result = [];
            let dbIdx = 0;

            for (const [database, variables] of Object.entries(this.descriptiveInfoDetails)) {
                dbIdx++;
                if (!variables || variables.length === 0) continue;

                const dbEntry = {
                    name: database,
                    dbIdx: dbIdx,
                    variables: []
                };

                let itemIdx = 0;

                for (const variable of variables) {
                    if (typeof variable === 'string') {
                        itemIdx++;
                        dbEntry.variables.push(this._buildContinuousVariable(database, variable, dbIdx, itemIdx));
                    } else if (typeof variable === 'object') {
                        for (const [varName, categories] of Object.entries(variable)) {
                            itemIdx++;
                            dbEntry.variables.push(
                                this._buildCategoricalVariable(database, varName, categories, dbIdx, itemIdx)
                            );
                        }
                    }
                }

                result.push(dbEntry);
            }

            return result;
        },

        /**
         * CSS class for the animated loading icon during form submission.
         * @returns {string}
         */
        loadingIconClass() {
            return this.loadingIconIsPen ? 'fa-pen' : 'fa-edit';
        }
    },

    methods: {
        /**
         * Initialize the component with backend data.
         * Called from the HTML template after page load.
         * @param {Object} descriptiveInfo - Descriptive info from backend
         * @param {Object} descriptiveInfoDetails - Detailed descriptive info from backend
         * @param {Object} preselectedValues - Preselected values from backend
         */
        async init(descriptiveInfo, descriptiveInfoDetails, preselectedValues) {
            this.descriptiveInfo = descriptiveInfo;
            this.descriptiveInfoDetails = descriptiveInfoDetails;
            this.preselectedValues = preselectedValues || {};

            // Call storeAndRenderData here (not in mounted) because init is
            // called after mount and data is only available after init sets it.
            await this.storeAndRenderData();
        },

        /**
         * Store data in IndexedDB, load semantic mapping, and render forms.
         * Main entry point called from mounted() lifecycle hook.
         */
        async storeAndRenderData() {
            try {
                await FlyoverDB.initDB();

                await FlyoverDB.saveData('metadata', {
                    key: 'descriptive_info',
                    data: this.descriptiveInfo,
                    timestamp: new Date().toISOString()
                });
                await FlyoverDB.saveData('metadata', {
                    key: 'descriptive_info_details',
                    data: this.descriptiveInfoDetails,
                    timestamp: new Date().toISOString()
                });
                console.log('Flyover: Stored descriptiveInfo and descriptiveInfoDetails in IndexedDB');

                await JSONLDMapper.loadFromIndexedDB();
                console.log('Flyover: Loaded semantic mapping from IndexedDB');

                // Flag that mapper is loaded so parsedDatabases re-computes
                // with category options and local mappings available
                this.mapperLoaded = true;

                this.renderFormsFromData();
            } catch (error) {
                console.error('Flyover: Error storing/rendering data:', error);
            }
        },

        /**
         * Process descriptiveInfoDetails into Vue-friendly reactive structures.
         * Triggers selectpicker initialization after DOM update and applies preselections.
         */
        renderFormsFromData() {
            // parsedDatabases is computed, so data is already reactive.
            // We just need to trigger selectpicker init and preselections after DOM update.
            this.$nextTick(() => {
                $('.selectpicker').selectpicker();
                this.applyLocalMappingPreselections();
                this.applyPreselectedValues();
                this.bindSelectpickerOverflowEvents();
                this.bindCategoryChangeEvents();
            });
        },

        /**
         * Build a continuous variable data object from raw data.
         * @param {string} database - Database name
         * @param {string} displayName - Variable display name
         * @param {number} dbIdx - Database index
         * @param {number} itemIdx - Item index
         * @returns {Object} Continuous variable data
         * @private
         */
        _buildContinuousVariable(database, displayName, dbIdx, itemIdx) {
            const localVarMatch = displayName.match(/\(or "([^"]+)"\)/);
            const localVariable = localVarMatch ? localVarMatch[1] : displayName.toLowerCase().replace(/ /g, '_');
            const isMissing = displayName.startsWith('Missing Description');
            const displayLabel = displayName.replace(' (or "', '<br>(or "');

            return {
                type: 'continuous',
                displayName: displayName,
                displayLabel: displayLabel,
                localVariable: localVariable,
                isMissing: isMissing,
                dbIdx: dbIdx,
                itemIdx: itemIdx
            };
        },

        /**
         * Build a categorical variable data object with category metadata.
         * @param {string} database - Database name
         * @param {string} varName - Variable name
         * @param {Array<Object>} categories - Category data array
         * @param {number} dbIdx - Database index
         * @param {number} itemIdx - Item index
         * @returns {Object} Categorical variable data
         * @private
         */
        _buildCategoricalVariable(database, varName, categories, dbIdx, itemIdx) {
            const localVarMatch = varName.match(/\(or "([^"]+)"\)/);
            const localVariable = localVarMatch ? localVarMatch[1] : varName.toLowerCase().replace(/ /g, '_');
            const globalVarName = varName.split(' (or')[0].toLowerCase().replace(/ /g, '_');
            const isMissing = varName.startsWith('Missing Description');
            const displayLabel = varName.replace(' (or "', '<br>(or "');

            const categoryOptions = this.getCategoryOptions(database, globalVarName);
            const localMappings = this.getLocalMappings(database, localVariable, globalVarName);

            // Build value-to-term-key lookup from local mappings
            const valueToTermKey = {};
            for (const [termKey, values] of Object.entries(localMappings)) {
                if (Array.isArray(values)) {
                    values.forEach(val => {
                        if (val !== null && val !== undefined) {
                            valueToTermKey[String(val).trim()] = termKey;
                        }
                    });
                } else if (values !== null && values !== undefined) {
                    valueToTermKey[String(values).trim()] = termKey;
                }
            }

            const processedCategories = [];
            let valueIdx = 0;

            for (const categoryData of categories) {
                valueIdx++;
                const value = categoryData.value !== undefined ? categoryData.value : '';
                const count = categoryData.count || 0;
                const safeValue = String(value).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
                const displayValue = value !== '' ? value : 'Empty cells';

                const termKey = valueToTermKey[String(value).trim()];
                const preselectedValue = termKey
                    ? termKey.charAt(0).toUpperCase() + termKey.slice(1).replace(/_/g, ' ')
                    : '';

                processedCategories.push({
                    value: value,
                    count: count,
                    safeValue: safeValue,
                    displayValue: displayValue,
                    preselectedValue: preselectedValue,
                    selectId: `select_${dbIdx}_${itemIdx}_${valueIdx}`,
                    selectName: `category_select_${dbIdx}_${itemIdx}_${valueIdx}`,
                    commentId: `comment_${dbIdx}_${itemIdx}_${valueIdx}`,
                    commentName: `category_comment_${dbIdx}_${itemIdx}_${valueIdx}`,
                    countId: `count_${dbIdx}_${itemIdx}_${valueIdx}`,
                    countName: `category_count_${dbIdx}_${itemIdx}_${valueIdx}`
                });
            }

            return {
                type: 'categorical',
                displayName: varName,
                displayLabel: displayLabel,
                localVariable: localVariable,
                globalVarName: globalVarName,
                isMissing: isMissing,
                dbIdx: dbIdx,
                itemIdx: itemIdx,
                categoryOptions: categoryOptions,
                categories: processedCategories
            };
        },

        /**
         * Apply local mapping preselections to category select elements.
         * Uses selectpicker to set previously saved values.
         */
        applyLocalMappingPreselections() {
            $('.category-select').each(function () {
                const $select = $(this);
                const preselectedValue = $select.data('previous-selection');
                if (preselectedValue !== undefined && preselectedValue !== null && preselectedValue !== '') {
                    $select.selectpicker('val', preselectedValue);
                }
            });
        },

        /**
         * Apply preselected values from backend to category select elements.
         * Matches selects by their database/variable/category-value composite key.
         */
        applyPreselectedValues() {
            if (!this.preselectedValues) return;

            for (const [key, value] of Object.entries(this.preselectedValues)) {
                $('.category-select').each(function () {
                    const $select = $(this);
                    const database = $select.data('database');
                    const variable = $select.data('variable');
                    const categoryValue = $select.data('category-value');
                    const selectKey = `${database}_${variable}_category_"${categoryValue}"`;

                    if (selectKey === key) {
                        $select.val(value);
                        $select.selectpicker('refresh');
                    }
                });
            }
        },

        /**
         * Get category options for a variable from the semantic mapping.
         * @param {string} database - Database name
         * @param {string} globalVarName - Global variable name
         * @returns {Array<string>} Category option strings
         */
        getCategoryOptions(database, globalVarName) {
            return JSONLDMapper.getCategoryOptionsForVariable(database, globalVarName);
        },

        /**
         * Get local mappings for a variable from the semantic mapping.
         * @param {string} database - Database name
         * @param {string} localVariable - Local variable name
         * @param {string} globalVarName - Global variable name
         * @returns {Object} Local mappings keyed by term
         */
        getLocalMappings(database, localVariable, globalVarName) {
            return JSONLDMapper.getLocalMappingsForVariable(database, localVariable, globalVarName);
        },

        /**
         * Handle category selection change via event delegation.
         * Updates the semantic mapping in IndexedDB via JSONLDMapper.
         * @param {string} database - Database name
         * @param {string} localVariable - Local variable name
         * @param {string} globalVariable - Global variable name
         * @param {string} categoryValue - Category value
         * @param {string} selectedOption - Newly selected option
         * @param {string} previousOption - Previously selected option
         */
        async onCategoryChange(database, localVariable, globalVariable, categoryValue, selectedOption, previousOption) {
            console.log('Flyover: Category selection changed:', {
                database, localVariable, globalVariable, categoryValue, selectedOption, previousOption
            });

            try {
                const success = await JSONLDMapper.updateCategoryMapping(
                    database, localVariable, globalVariable, String(categoryValue), selectedOption, previousOption
                );
                if (success) {
                    console.log('Flyover: Successfully updated category mapping');
                } else {
                    console.warn('Flyover: Failed to update category mapping');
                }
            } catch (error) {
                console.error('Flyover: Error updating category mapping:', error);
            }
        },

        /**
         * Bind event delegation for selectpicker overflow handling.
         * Adjusts container height when dropdowns open to prevent clipping.
         */
        bindSelectpickerOverflowEvents() {
            $(document).off('show.bs.select.vueDetails').on('show.bs.select.vueDetails', '.selectpicker', function () {
                var dropdownMenu = $(this).parent().find('.dropdown-menu');
                var toggleContent = $(this).closest('.toggle-content');
                var newHeight = Math.max(toggleContent.height(), dropdownMenu.height());
                toggleContent.height(newHeight + 53);
                toggleContent.css('overflow-y', 'hidden');
            });

            $(document).off('hide.bs.select.vueDetails').on('hide.bs.select.vueDetails', '.selectpicker', function () {
                var toggleContent = $(this).closest('.toggle-content');
                toggleContent.height('auto');
                toggleContent.css('overflow-y', 'auto');
            });
        },

        /**
         * Bind event delegation for category select change events.
         * Handles comment input enable/disable and triggers mapping update.
         */
        bindCategoryChangeEvents() {
            var self = this;

            $(document).off('change.vueDetails', '.category-select').on('change.vueDetails', '.category-select', function () {
                var commentId = $(this).data('comment-id');
                var commentInput = $('[id="' + commentId + '"]');
                if ($(this).val() === 'Other') {
                    commentInput.removeAttr('disabled');
                } else {
                    commentInput.attr('disabled', 'disabled');
                }

                const database = $(this).data('database');
                const localVariable = $(this).data('variable');
                const globalVariable = $(this).data('global-variable');
                const categoryValue = $(this).data('category-value');
                const selectedOption = $(this).val();
                const previousOption = $(this).data('previous-selection');

                if (!database || !localVariable || !globalVariable || categoryValue === undefined) {
                    return;
                }

                $(this).data('previous-selection', selectedOption);

                self.onCategoryChange(database, localVariable, globalVariable, categoryValue, selectedOption, previousOption);
            });
        },

        /**
         * Handle form submission. Renames field names from indexed format to
         * proper backend format, saves updated descriptive info to IndexedDB,
         * starts loading animation, then allows native form submission.
         * @param {Event} event - Form submit event
         */
        async onFormSubmit(event) {
            // Rename category fields from indexed names to proper backend names
            $('.category-select, .category-item input[type="text"], .category-item input[type="hidden"]').each(function () {
                const $field = $(this);
                const database = $field.data('database');
                const variable = $field.data('variable');
                const categoryValue = $field.data('category-value');

                if (database && variable && categoryValue !== undefined) {
                    let properName = '';
                    const originalName = $field.attr('name');

                    if (originalName.startsWith('category_select_')) {
                        properName = `${database}_${variable}_category_"${categoryValue}"`;
                    } else if (originalName.startsWith('category_comment_')) {
                        properName = `comment_${database}_${variable}_category_"${categoryValue}"`;
                    } else if (originalName.startsWith('category_count_')) {
                        properName = `count_${database}_${variable}_category_"${categoryValue}"`;
                    }

                    if (properName) {
                        $field.attr('name', properName);
                    }
                }
            });

            // Save updated descriptive info to IndexedDB
            try {
                const updatedDescriptiveInfo = JSON.parse(JSON.stringify(this.descriptiveInfo));

                $('input[id$="_notation"]').each(function () {
                    const id = $(this).attr('id').replace('_notation', '');
                    const parts = id.split('_');
                    const variable = parts.pop();
                    const database = parts.join('_');
                    const units = $(`#${id}`).val();

                    if (units && updatedDescriptiveInfo[database]?.[variable]) {
                        updatedDescriptiveInfo[database][variable].units = units;
                    }
                });

                $('.category-select').each(function () {
                    const database = $(this).data('database');
                    const variable = $(this).data('variable');
                    const categoryValue = $(this).data('category-value');
                    const description = $(this).val();
                    const commentId = $(this).data('comment-id');
                    const comment = $(`#${commentId}`).val();
                    const countId = commentId.replace('comment_', 'count_');
                    const count = $(`#${countId}`).val();

                    if (description && updatedDescriptiveInfo[database]?.[variable]) {
                        updatedDescriptiveInfo[database][variable][`Category: ${categoryValue}`] =
                            `Category ${categoryValue}: ${description}, comment: ${comment || 'No comment provided'}, count: ${count || 'No count available'}`;
                    }
                });

                await FlyoverDB.saveData('metadata', {
                    key: 'descriptive_info',
                    data: updatedDescriptiveInfo,
                    timestamp: new Date().toISOString()
                });

                console.log('Flyover: Updated descriptive info stored in IndexedDB');
            } catch (error) {
                console.error('Flyover: Failed to store variable details in IndexedDB:', error);
            }

            this.startLoadingAnimation();
        },

        /**
         * Toggle database section expanded/collapsed state.
         * @param {string} database - Database name to toggle
         */
        toggleDatabase(database) {
            this.expandedDatabases[database] = !this.expandedDatabases[database];
        },

        /**
         * Toggle variable categories section expanded/collapsed state.
         * @param {string} database - Database name
         * @param {number} varIdx - Variable index within the database
         */
        toggleVariable(database, varIdx) {
            if (!this.expandedVariables[database]) {
                this.expandedVariables[database] = {};
            }
            this.expandedVariables[database][varIdx] = !this.expandedVariables[database][varIdx];
        },

        /**
         * Check if a variable's categories section is expanded.
         * @param {string} database - Database name
         * @param {number} varIdx - Variable index within the database
         * @returns {boolean}
         */
        isVariableExpanded(database, varIdx) {
            return !!(this.expandedVariables[database] && this.expandedVariables[database][varIdx]);
        },

        /**
         * Start loading animation on the submit button.
         * Alternates between edit and pen icons with fade effects.
         */
        startLoadingAnimation() {
            this.isProcessing = true;
            this.loadingIconIsPen = false;

            this.loadingInterval = setInterval(() => {
                const icon = $('#submitBtn .loading-icon');
                icon.addClass('icon-fade-out');
                setTimeout(() => {
                    this.loadingIconIsPen = !this.loadingIconIsPen;
                    this.$nextTick(() => {
                        const updatedIcon = $('#submitBtn .loading-icon');
                        updatedIcon.removeClass('icon-fade-out').addClass('icon-fade-in');
                        setTimeout(() => {
                            updatedIcon.removeClass('icon-fade-in');
                        }, 300);
                    });
                }, 150);
            }, 1000);
        },

        /**
         * Stop loading animation and restore submit button state.
         */
        stopLoadingAnimation() {
            if (this.loadingInterval) {
                clearInterval(this.loadingInterval);
                this.loadingInterval = null;
            }
            this.isProcessing = false;
            this.loadingIconIsPen = false;
        }
    },

    /**
     * Lifecycle hook: called after the component is mounted.
     * Data initialization is handled by init() which is called externally
     * after mount, since the backend data is only available at that point.
     */
    async mounted() {
        // No-op: init() is called externally after mount with backend data
    }
});

// Export for global access
window.DescribeVariableDetailsApp = DescribeVariableDetailsApp;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DescribeVariableDetailsApp;
}
