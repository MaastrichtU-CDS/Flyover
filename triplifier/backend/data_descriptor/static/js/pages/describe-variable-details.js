/**
 * Describe Variable Details Page Module
 * Handles the variable details page functionality including categorical/continuous
 * variable rendering, category selection, and form submission with IndexedDB persistence.
 */

const DescribeVariableDetailsPage = {
    // State
    loadingInterval: null,
    descriptiveInfo: null,
    descriptiveInfoDetails: null,
    preselectedValues: null,

    /**
     * Initialize the describe variable details page
     * @param {Object} descriptiveInfo - Descriptive info from backend
     * @param {Object} descriptiveInfoDetails - Detailed descriptive info from backend
     * @param {Object} preselectedValues - Preselected values from backend
     */
    init: function(descriptiveInfo, descriptiveInfoDetails, preselectedValues) {
        this.descriptiveInfo = descriptiveInfo;
        this.descriptiveInfoDetails = descriptiveInfoDetails;
        this.preselectedValues = preselectedValues;

        this.storeAndRenderData();
        this.bindGlobalEvents();
    },

    /**
     * Start loading animation on a button
     */
    startLoadingAnimation: function(buttonSelector) {
        var self = this;
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
    },

    /**
     * Store data in IndexedDB and render forms
     */
    storeAndRenderData: async function() {
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

            await this.renderFormsFromData();
        } catch (error) {
            console.error('Flyover: Error storing/rendering data:', error);
        }
    },

    /**
     * Render forms from descriptive info details
     */
    renderFormsFromData: async function() {
        const container = $('#databases-container');
        let dbIdx = 0;

        for (const [database, variables] of Object.entries(this.descriptiveInfoDetails)) {
            dbIdx++;
            if (!variables || variables.length === 0) continue;

            const dbSection = $('<div>').addClass('database-section');
            dbSection.append(`
                <h2 style="display: inline-block;">
                    <i class="fas fa-database"></i> ${database}
                </h2>
                <button class="toggle-button" type="button">
                    <span class="toggle-text">Show more</span>
                    <svg class="angle-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512" width="20" height="20">
                        <path d="M201.4 137.4c12.5-12.5 32.8-12.5 45.3 0l160 160c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L224 205.3 86.6 342.6c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3l160-160z"/>
                    </svg>
                </button>
                <div class="content hidden variables-container"></div>
                <hr>
            `);

            const varsContainer = dbSection.find('.variables-container');
            let itemIdx = 0;

            for (const variable of variables) {
                itemIdx++;
                if (typeof variable === 'string') {
                    this.renderContinuousVariable(varsContainer, database, variable, dbIdx, itemIdx);
                } else if (typeof variable === 'object') {
                    for (const [varName, categories] of Object.entries(variable)) {
                        this.renderCategoricalVariable(varsContainer, database, varName, categories, dbIdx, itemIdx);
                        itemIdx++;
                    }
                }
            }

            container.append(dbSection);
        }

        this.bindToggleEvents();
        $('.selectpicker').selectpicker();
        this.applyLocalMappingPreselections();
        this.applyPreselectedValues();
    },

    /**
     * Apply local mapping preselections to category selects
     */
    applyLocalMappingPreselections: function() {
        $('.category-select').each(function() {
            const $select = $(this);
            const preselectedValue = $select.data('previous-selection');
            if (preselectedValue !== undefined && preselectedValue !== null && preselectedValue !== '') {
                $select.selectpicker('val', preselectedValue);
            }
        });
    },

    /**
     * Render a continuous variable row
     */
    renderContinuousVariable: function(container, database, displayName, dbIdx, itemIdx) {
        const localVarMatch = displayName.match(/\(or "([^"]+)"\)/);
        const localVariable = localVarMatch ? localVarMatch[1] : displayName.toLowerCase().replace(/ /g, '_');
        const isMissing = displayName.startsWith('Missing Description');

        const varRow = $(`
            <div class="variable-row">
                <div class="variable-label">
                    ${isMissing ? '<span class="missing-description">' + displayName.replace(' (or "', '<br>(or "') + '</span>' : displayName.replace(' (or "', '<br>(or "')}
                </div>
                <div class="variable-controls">
                    <input id="${database}_${localVariable}" type="text"
                           name="${database}_${localVariable}"
                           placeholder="Type unit here" class="form-control" />
                    <input id="${database}_${localVariable}_notation" type="text"
                           name="${database}_${localVariable}_notation_missing_or_unspecified"
                           placeholder="Type missing value notation here" class="form-control" />
                </div>
            </div>
        `);

        container.append(varRow);
    },

    /**
     * Render a categorical variable row with its categories
     */
    renderCategoricalVariable: function(container, database, varName, categories, dbIdx, itemIdx) {
        const localVarMatch = varName.match(/\(or "([^"]+)"\)/);
        const localVariable = localVarMatch ? localVarMatch[1] : varName.toLowerCase().replace(/ /g, '_');
        const globalVarName = varName.split(' (or')[0].toLowerCase().replace(/ /g, '_');
        const isMissing = varName.startsWith('Missing Description');

        const varRow = $(`
            <div class="variable-row">
                <div class="variable-label">
                    ${isMissing ? '<span class="missing-description">' + varName.replace(' (or "', '<br>(or "') + '</span>' : varName.replace(' (or "', '<br>(or "')}
                </div>
                <div class="variable-controls">
                    <button class="item-toggle-button" type="button"></button>
                </div>
            </div>
        `);

        const categoriesSection = $('<div>').addClass('toggle-content hidden categorical-section');

        const categoryOptions = JSONLDMapper.getCategoryOptionsForVariable(database, globalVarName);
        console.log('Flyover: Category options for', globalVarName, ':', categoryOptions);

        const localMappings = JSONLDMapper.getLocalMappingsForVariable(database, localVariable, globalVarName);
        console.log('Flyover: Local mappings for', localVariable, ':', localMappings);

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
        console.log('Flyover: Value to term key mapping:', valueToTermKey);
        console.log('Flyover: Raw local mappings:', localMappings);

        let valueIdx = 0;
        for (const categoryData of categories) {
            valueIdx++;
            const value = categoryData.value !== undefined ? categoryData.value : '';
            const count = categoryData.count || 0;
            const safeValue = String(value).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
            const displayValue = value !== '' ? value : 'Empty cells';

            let optionsHtml = '<option value="">Description</option>';

            if (categoryOptions.length > 0) {
                categoryOptions.forEach(option => {
                    optionsHtml += `<option value="${option}">${option}</option>`;
                });
                optionsHtml += '<option value="Other">Other</option>';
            } else {
                optionsHtml += `
                    <option value="Yes">Yes</option>
                    <option value="No">No</option>
                    <option value="Male">Male sex</option>
                    <option value="Female">Female sex</option>
                    <option value="Primary Education">Primary education</option>
                    <option value="Secondary Education">Secondary education</option>
                    <option value="Tertiary Education">Tertiary education</option>
                    <option value="Missing">Missing value</option>
                    <option value="Other">Other</option>
                `;
            }

            const termKey = valueToTermKey[String(value).trim()];
            const preselectedValue = termKey ? termKey.charAt(0).toUpperCase() + termKey.slice(1).replace(/_/g, ' ') : '';
            console.log('Flyover: Preselected value for', value, ':', preselectedValue);

            const categoryItem = $(`
                <div class="category-item">
                    <div class="category-label">
                        ${displayValue} (counted: ${count})
                    </div>
                    <div class="category-controls">
                        <select id="select_${dbIdx}_${itemIdx}_${valueIdx}"
                                name="category_select_${dbIdx}_${itemIdx}_${valueIdx}"
                                class="selectpicker form-control category-select"
                                data-comment-id="comment_${dbIdx}_${itemIdx}_${valueIdx}"
                                data-database="${database}"
                                data-variable="${localVariable}"
                                data-global-variable="${globalVarName}"
                                data-category-value="${safeValue}"
                                data-previous-selection="${preselectedValue}"
                                data-live-search="true">
                            ${optionsHtml}
                        </select>
                        <input class="form-control" type="text"
                               id="comment_${dbIdx}_${itemIdx}_${valueIdx}"
                               name="category_comment_${dbIdx}_${itemIdx}_${valueIdx}"
                               data-database="${database}"
                               data-variable="${localVariable}"
                               data-category-value="${safeValue}"
                               placeholder="If other, please specify"
                               disabled='disabled'
                               style="width: 200px;" />
                    </div>
                </div>
                <input type="hidden"
                       id="count_${dbIdx}_${itemIdx}_${valueIdx}"
                       name="category_count_${dbIdx}_${itemIdx}_${valueIdx}"
                       data-database="${database}"
                       data-variable="${localVariable}"
                       data-category-value="${safeValue}"
                       value="${count}" />
            `);

            categoriesSection.append(categoryItem);
        }

        container.append(varRow);
        container.append(categoriesSection);
    },

    /**
     * Apply preselected values from backend
     */
    applyPreselectedValues: function() {
        for (const [key, value] of Object.entries(this.preselectedValues)) {
            $('.category-select').each(function() {
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
     * Bind toggle events for database sections and item toggles
     */
    bindToggleEvents: function() {
        $('.toggle-button').off('click').on('click', function () {
            var textElement = $(this).find('.toggle-text');
            var svgElement = $(this).find('.angle-icon path');
            if (textElement.text() === 'Show more') {
                textElement.text('Show less');
                svgElement.attr('d', 'M201.4 374.6c12.5 12.5 32.8 12.5 45.3 0l160-160c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0L224 306.7 86.6 169.4c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3l160 160z');
            } else {
                textElement.text('Show more');
                svgElement.attr('d', 'M201.4 137.4c12.5-12.5 32.8-12.5 45.3 0l160 160c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L224 205.3 86.6 342.6c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3l160-160z');
            }
            $(this).toggleClass('open');
            $(this).next('.content').toggleClass('active');
            $(this).next('.content').removeClass('hidden');
        });

        $('.item-toggle-button').off('click').on('click', function () {
            $(this).toggleClass('open');
            $(this).closest('.variable-row').next('.toggle-content').toggleClass('active');
        });
    },

    /**
     * Bind global events (selectpicker, category changes, form submission)
     */
    bindGlobalEvents: function() {
        var self = this;

        $(document).on('show.bs.select', '.selectpicker', function (e) {
            var dropdownMenu = $(this).parent().find('.dropdown-menu');
            var toggleContent = $(this).closest('.toggle-content');
            var newHeight = Math.max(toggleContent.height(), dropdownMenu.height());
            toggleContent.height(newHeight + 53);
            toggleContent.css('overflow-y', 'hidden');
        });

        $(document).on('hide.bs.select', '.selectpicker', function (e) {
            var toggleContent = $(this).closest('.toggle-content');
            toggleContent.height('auto');
            toggleContent.css('overflow-y', 'auto');
        });

        $(document).on('change', '.category-select', function () {
            var commentId = $(this).data('comment-id');
            var commentInput = $('[id="' + commentId + '"]');
            if ($(this).val() === "Other") {
                commentInput.removeAttr("disabled");
            } else {
                commentInput.attr("disabled", "disabled");
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

            console.log('Flyover: Category selection changed:', {
                database: database,
                localVariable: localVariable,
                globalVariable: globalVariable,
                categoryValue: categoryValue,
                selectedOption: selectedOption,
                previousOption: previousOption
            });

            $(this).data('previous-selection', selectedOption);

            JSONLDMapper.updateCategoryMapping(database, localVariable, globalVariable, String(categoryValue), selectedOption, previousOption)
                .then(success => {
                    if (success) {
                        console.log('Flyover: Successfully updated category mapping');
                    } else {
                        console.warn('Flyover: Failed to update category mapping');
                    }
                })
                .catch(error => {
                    console.error('Flyover: Error updating category mapping:', error);
                });
        });

        $('form').submit(async function(e) {
            $('.category-select, .category-item input[type="text"], .category-item input[type="hidden"]').each(function() {
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

            try {
                const updatedDescriptiveInfo = JSON.parse(JSON.stringify(self.descriptiveInfo));

                $('input[id$="_notation"]').each(function() {
                    const id = $(this).attr('id').replace('_notation', '');
                    const parts = id.split('_');
                    const variable = parts.pop();
                    const database = parts.join('_');
                    const units = $(`#${id}`).val();

                    if (units && updatedDescriptiveInfo[database]?.[variable]) {
                        updatedDescriptiveInfo[database][variable].units = units;
                    }
                });

                $('.category-select').each(function() {
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

            self.startLoadingAnimation('#submitBtn');
        });
    }
};

// Export for global access
window.DescribeVariableDetailsPage = DescribeVariableDetailsPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DescribeVariableDetailsPage;
}
