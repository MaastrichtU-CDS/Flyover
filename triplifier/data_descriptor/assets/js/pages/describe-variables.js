/**
 * Describe Variables Page Module
 * Handles the variable description page functionality including pagination,
 * form state caching, and semantic mapping.
 */

const DescribeVariablesPage = {
    // Configuration
    PAGE_SIZE: 10,
    
    // State
    databasePages: {},
    formStateCache: {},
    columnInfoData: null,
    descriptionToDatatype: {},
    preselectedDatatypes: {},
    autoFilledFields: new Set(),
    manualOverrides: new Set(),
    selectedDescriptions: {},
    loadingInterval: null,

    /**
     * Initialize the describe variables page
     * @param {Object} backendColumnInfo - Column info from backend
     */
    init: async function(backendColumnInfo) {
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
            this.renderDatabaseSections(this.columnInfoData);
        }

        await this.loadAndApplySemanticMapping();

        if (this.columnInfoData) {
            for (const database of Object.keys(this.columnInfoData)) {
                this.renderPage(database, 1);
            }
        }

        this.bindFormSubmitHandler();
        this.checkDescriptions();
    },

    /**
     * Initialize IndexedDB
     */
    initializeIndexedDB: async function() {
        if (FlyoverDB.isSupported()) {
            await FlyoverDB.initDB();
        }
    },

    /**
     * Save column info to IndexedDB
     * @param {Object} data - Column info data
     */
    saveColumnInfo: async function(data) {
        await FlyoverDB.saveData('metadata', {
            key: 'column_info',
            data: data,
            timestamp: new Date().toISOString()
        });
    },

    /**
     * Load column info from IndexedDB
     * @returns {Object|null} Column info data
     */
    loadColumnInfo: async function() {
        const result = await FlyoverDB.getData('metadata', 'column_info');
        if (result && result.data) {
            return result.data;
        }
        return null;
    },

    /**
     * Render database sections
     * @param {Object} columnInfo - Column info organized by database
     */
    renderDatabaseSections: function(columnInfo) {
        const self = this;
        const container = $('#databases-container');
        container.empty();

        for (const [database, columns] of Object.entries(columnInfo)) {
            const totalItems = columns.length;
            const totalGroups = Math.ceil(totalItems / this.PAGE_SIZE);

            let paginationHtml = '';
            if (totalGroups > 1) {
                paginationHtml = `
                    <div class="pagination-controls">
                        <button type="button" class="prev-btn" onclick="DescribeVariablesPage.changePage('${database}', -1)" disabled>
                            &#x2190;
                        </button>
                        <span class="page-indicator">Page <span
                                id="current-page-${database}">1</span> of ${totalGroups}</span>
                        <button type="button" class="next-btn" onclick="DescribeVariablesPage.changePage('${database}', 1)">&#x2192;
                        </button>
                    </div>
                `;
            }

            const sectionHtml = `
                <h2 style="display: inline-block;"><i class="fas fa-database"></i> ${database}</h2>
                <button class="toggle-button" type="button" data-database="${database}">
                    <span class="toggle-text">Show more</span>
                    <svg class="angle-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512" width="20" height="20">
                        <path d="M201.4 137.4c12.5-12.5 32.8-12.5 45.3 0l160 160c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L224 205.3 86.6 342.6c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3l160-160z"/>
                    </svg>
                </button>
                <div class="content hidden" data-database="${database}">
                    <div class="variables-container" id="variables-${database}"
                         data-columns='${JSON.stringify(columns)}'
                         data-total="${totalItems}">
                    </div>
                    ${paginationHtml}
                </div>
                <hr>
            `;

            container.append(sectionHtml);
        }

        this.attachToggleHandlers();
    },

    /**
     * Attach toggle handlers for database sections
     */
    attachToggleHandlers: function() {
        const self = this;
        
        $('.toggle-button').off('click').on('click', function() {
            const textElement = $(this).find('.toggle-text');
            const svgElement = $(this).find('.angle-icon path');
            const database = $(this).data('database');
            const content = $(this).next('.content');

            if (textElement.text() === 'Show more') {
                textElement.text('Show less');
                svgElement.attr('d', 'M201.4 374.6c12.5 12.5 32.8 12.5 45.3 0l160-160c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0L224 306.7 86.6 169.4c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3l160 160z');
            } else {
                textElement.text('Show more');
                svgElement.attr('d', 'M201.4 137.4c12.5-12.5 32.8-12.5 45.3 0l160 160c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L224 205.3 86.6 342.6c-12.5 12.5-32.8 12.5-45.3 0s-12.5 32.8 0 45.3l160-160z');
                self.cacheCurrentPageState(database);
            }
            $(this).toggleClass('open');
            content.toggleClass('active');
        });
    },

    /**
     * Load and apply semantic mapping
     */
    loadAndApplySemanticMapping: async function() {
        try {
            await JSONLDMapper.loadFromIndexedDB();
            JSONLDMapper.populateDescriptionOptions();
            this.checkDescriptions();
        } catch (error) {
            console.error('Failed to load semantic mapping:', error);
        }
    },

    /**
     * Bind form submit handler
     */
    bindFormSubmitHandler: function() {
        const self = this;

        $('form').submit(function(e) {
            $('.variables-container').each(function() {
                const database = $(this).attr('id').replace('variables-', '');
                self.cacheCurrentPageState(database);
            });

            const form = $(this);
            for (const [key, cached] of Object.entries(self.formStateCache)) {
                const db = cached.database;
                const item = key.replace(`${db}_`, '');

                if ($(`#ncit_comment_${db}_${item}`).length === 0) {
                    if (cached.description) {
                        form.append(`<input type="hidden" name="ncit_comment_${db}_${item}" value="${cached.description}">`);
                    }
                    if (cached.datatype) {
                        form.append(`<input type="hidden" name="${db}_${item}" value="${cached.datatype}">`);
                    }
                    if (cached.comment) {
                        form.append(`<input type="hidden" name="comment_${db}_${item}" value="${cached.comment}">`);
                    }
                }
            }

            self.startLoadingAnimation('#submitBtn');
        });
    },

    /**
     * Check if any descriptions are selected
     */
    checkDescriptions: function() {
        let hasSelection = false;

        $('select[id^="ncit_comment_"]').each(function() {
            if ($(this).val() && $(this).val() !== '') {
                hasSelection = true;
                return false;
            }
        });

        for (const key in this.formStateCache) {
            if (this.formStateCache[key].description) {
                hasSelection = true;
                break;
            }
        }

        $('#submitBtn').prop('disabled', !hasSelection);
    },

    /**
     * Build selected descriptions object
     */
    buildSelectedDescriptions: function() {
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
        $('select.description-select').each(function() {
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
     * Update description options (disable already selected)
     */
    updateDescriptionOptions: function() {
        this.buildSelectedDescriptions();
        const self = this;

        $('select.description-select').each(function() {
            const $select = $(this);
            const db = $select.data('database');
            const item = $select.data('item');
            const currentKey = `${db}_${item}`;
            const dbSelections = self.selectedDescriptions[db] || {};

            $select.find('option').each(function() {
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
     * Auto-populate datatype based on description
     * @param {string} database - Database name
     * @param {string} item - Item/column name
     */
    autoPopulateDatatype: function(database, item) {
        const descriptionSelect = $(`#ncit_comment_${database}_${item}`);
        const datatypeSelect = $(`#${database}_${item}`);
        const feedback = $(`#feedback_${database}_${item}`);
        const fieldKey = `${database}_${item}`;

        const selectedDescription = descriptionSelect.val();

        if (selectedDescription && selectedDescription !== '' && selectedDescription !== 'Other') {
            const suggestedDatatype = this.preselectedDatatypes[fieldKey] || this.descriptionToDatatype[selectedDescription];

            if (suggestedDatatype) {
                if (datatypeSelect.val() === '' || !this.manualOverrides.has(fieldKey)) {
                    try {
                    if (datatypeSelect.length > 0) {
                        datatypeSelect.selectpicker('val', suggestedDatatype);
                    }
                } catch (e) {
                    console.error('Failed to set datatype value:', e);
                }
                    this.autoFilledFields.add(fieldKey);

                    feedback.show();
                    setTimeout(function() {
                        feedback.fadeOut();
                    }, 3000);
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
     * Show manual override confirmation
     * @param {string} database - Database name
     * @param {string} item - Item/column name
     */
    showOverrideConfirmation: function(database, item) {
        const feedback = $(`#feedback_${database}_${item}`);
        feedback.html('<i class="fas fa-user-edit"></i> Manually overridden').css('color', '#ffc107').show();
        setTimeout(function() {
            feedback.fadeOut();
        }, 2000);
    },

    /**
     * Cache current page state
     * @param {string} database - Database name
     */
    cacheCurrentPageState: function(database) {
        const self = this;

        $(`#variables-${database} .variable-row`).each(function() {
            const row = $(this);
            const item = row.find('.description-select').data('item');
            const key = `${database}_${item}`;

            const domDescription = row.find('.description-select').val();
            const domDatatype = row.find('.datatype-select').val();
            const domComment = row.find(`#comment_${database}_${item}`).val();

            const existing = self.formStateCache[key] || {};

            self.formStateCache[key] = {
                database: database,
                description: domDescription || existing.description || '',
                datatype: domDatatype || existing.datatype || '',
                comment: domComment || existing.comment || ''
            };
        });

        this.syncToIndexedDB();
    },

    /**
     * Sync form state to IndexedDB
     */
    syncToIndexedDB: async function() {
        try {
            await JSONLDMapper.updateMappingFromForm(this.formStateCache);
        } catch (error) {
            console.error('Failed to sync to IndexedDB:', error);
        }
    },

    /**
     * Render a specific page of variables
     * @param {string} database - Database name
     * @param {number} pageNumber - Page number (1-based)
     */
    renderPage: function(database, pageNumber) {
        const self = this;
        const container = $(`#variables-${database}`);
        const columns = JSON.parse(container.attr('data-columns'));
        const totalItems = parseInt(container.attr('data-total'));
        const totalPages = Math.ceil(totalItems / this.PAGE_SIZE);

        if (pageNumber < 1 || pageNumber > totalPages) return;

        this.cacheCurrentPageState(database);

        container.empty();

        const startIdx = (pageNumber - 1) * this.PAGE_SIZE;
        const endIdx = Math.min(startIdx + this.PAGE_SIZE, totalItems);

        for (let i = startIdx; i < endIdx; i++) {
            const item = columns[i];
            const key = `${database}_${item}`;
            const cached = this.formStateCache[key] || {};

            const row = $(`
                <div class="variable-row">
                    <div class="variable-label">${item}</div>
                    <div class="variable-controls">
                        <select id="ncit_comment_${database}_${item}"
                                name="ncit_comment_${database}_${item}"
                                class="selectpicker description-select" data-live-search="true"
                                data-database="${database}" data-item="${item}">
                            <option value="">Description</option>
                            <option value="Other">Other</option>
                        </select>

                        <input class="form-control" type="text" id="comment_${database}_${item}"
                               name="comment_${database}_${item}"
                               placeholder="If other, please specify" disabled='disabled'/>

                        <div class="datatype-container">
                            <select id="${database}_${item}" name="${database}_${item}"
                                    class="selectpicker datatype-select" data-live-search="true"
                                    data-database="${database}" data-item="${item}">
                                <option value="">Data type</option>
                                <option value="categorical">Categorical</option>
                                <option value="continuous">Continuous</option>
                                <option value="identifier">Identifier</option>
                                <option value="standardised">Standardised</option>
                            </select>
                            <small class="datatype-feedback" id="feedback_${database}_${item}"
                                   style="display: none; color: #28a745;">
                                <i class="fas fa-magic"></i> Auto-filled based on description
                            </small>
                        </div>
                    </div>
                </div>
            `);

            container.append(row);
        }

        this.databasePages[database] = pageNumber;

        const globalNames = JSONLDMapper.getGlobalVariableNames();
        container.find('.description-select').each(function() {
            const $select = $(this);
            globalNames.forEach(name => {
                if (name !== "Other") {
                    $select.append(`<option value="${name}">${name}</option>`);
                }
            });
        });

        // Initialize selectpicker with error handling
        try {
            if (container.find('.selectpicker').length > 0) {
                container.find('.selectpicker').selectpicker();
            }
        } catch (e) {
            console.error('Failed to initialize selectpicker:', e);
        }

        for (let i = startIdx; i < endIdx; i++) {
            const item = columns[i];
            const key = `${database}_${item}`;
            const cached = this.formStateCache[key];

            if (cached) {
                if (cached.description) {
                    $(`#ncit_comment_${database}_${item}`).selectpicker('val', cached.description);
                }
                if (cached.datatype) {
                    $(`#${database}_${item}`).selectpicker('val', cached.datatype);
                }
                if (cached.comment) {
                    $(`#comment_${database}_${item}`).val(cached.comment);
                    $(`#comment_${database}_${item}`).prop('disabled', false);
                }
            }
        }

        this.attachEventHandlers(database);
        this.applyPreselectionsToCurrentPage(database);
        this.checkDescriptions();
        this.updateDescriptionOptions();
    },

    /**
     * Attach event handlers for a database section
     * @param {string} database - Database name
     */
    attachEventHandlers: function(database) {
        const self = this;
        const container = $(`#variables-${database}`);

        container.find('.description-select').on('changed.bs.select', function() {
            const db = $(this).data('database');
            const item = $(this).data('item');
            const commentBox = $(`#comment_${db}_${item}`);
            const key = `${db}_${item}`;

            if ($(this).val() === "Other") {
                commentBox.removeAttr("disabled");
            } else {
                commentBox.attr("disabled", "disabled");
            }

            if (!self.formStateCache[key]) {
                self.formStateCache[key] = {};
            }
            self.formStateCache[key].database = db;
            self.formStateCache[key].description = $(this).val();

            self.autoPopulateDatatype(db, item);
            self.checkDescriptions();
            self.updateDescriptionOptions();
            self.syncToIndexedDB();
        });

        container.find('.datatype-select').on('changed.bs.select', function() {
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

        container.find('input[id^="comment_"]').on('change', function() {
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
     * Apply preselections to current page
     * @param {string} database - Database name
     */
    applyPreselectionsToCurrentPage: function(database) {
        const self = this;
        const { preselectedDescriptions, preselectedDatatypes, descriptionToDatatype } = JSONLDMapper.computePreselections();

        this.descriptionToDatatype = descriptionToDatatype;
        this.preselectedDatatypes = preselectedDatatypes;

        $(`#variables-${database} .description-select`).each(function() {
            const $select = $(this);
            const db = $select.data('database');
            const item = $select.data('item');
            const key = `${db}_${item}`;

            if (key in preselectedDescriptions && !(key in self.formStateCache)) {
                const value = preselectedDescriptions[key];
                try {
                    if ($select.length > 0) {
                        $select.selectpicker('val', value);
                    }
                } catch (e) {
                    console.error('Failed to set select value:', e);
                }
            }
        });

        $(`#variables-${database} .datatype-select`).each(function() {
            const $select = $(this);
            const db = $select.data('database');
            const item = $select.data('item');
            const key = `${db}_${item}`;

            if (key in preselectedDatatypes && !(key in self.formStateCache)) {
                const value = preselectedDatatypes[key];
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
     * Change page
     * @param {string} database - Database name
     * @param {number} direction - Direction (+1 or -1)
     */
    changePage: function(database, direction) {
        const container = $(`#variables-${database}`);
        const totalItems = parseInt(container.attr('data-total'));
        const totalPages = Math.ceil(totalItems / this.PAGE_SIZE);
        const currentPage = this.databasePages[database] || 1;
        const newPage = currentPage + direction;

        if (newPage >= 1 && newPage <= totalPages) {
            this.renderPage(database, newPage);

            $(`#current-page-${database}`).text(newPage);

            const prevBtn = container.closest('.content').find('.prev-btn');
            const nextBtn = container.closest('.content').find('.next-btn');
            prevBtn.prop('disabled', newPage === 1);
            nextBtn.prop('disabled', newPage === totalPages);
        }
    },

    /**
     * Start loading animation on a button
     * @param {string} buttonSelector - jQuery selector for the button
     */
    startLoadingAnimation: function(buttonSelector) {
        const button = $(buttonSelector);
        const originalText = button.html();

        button.prop('disabled', true).addClass('processing');

        let isPen = false;
        button.html('<i class="fas fa-edit loading-icon"></i> Processing descriptions...');

        this.loadingInterval = setInterval(function() {
            const icon = button.find('.loading-icon');

            icon.addClass('icon-fade-out');

            setTimeout(function() {
                if (isPen) {
                    icon.removeClass('fa-pen').addClass('fa-edit');
                } else {
                    icon.removeClass('fa-edit').addClass('fa-pen');
                }
                isPen = !isPen;

                icon.removeClass('icon-fade-out').addClass('icon-fade-in');

                setTimeout(function() {
                    icon.removeClass('icon-fade-in');
                }, 300);
            }, 150);
        }, 1000);

        button.data('original-text', originalText);
    },

    /**
     * Stop loading animation on a button
     * @param {string} buttonSelector - jQuery selector for the button
     */
    stopLoadingAnimation: function(buttonSelector) {
        const button = $(buttonSelector);

        if (this.loadingInterval) {
            clearInterval(this.loadingInterval);
            this.loadingInterval = null;
        }

        const originalText = button.data('original-text');
        if (originalText) {
            button.html(originalText);
        }
        button.prop('disabled', false).removeClass('processing');
    }
};

// Global function for pagination (called from onclick handlers)
function changePage(database, direction) {
    DescribeVariablesPage.changePage(database, direction);
}

// Export for global access
window.DescribeVariablesPage = DescribeVariablesPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DescribeVariablesPage;
}
