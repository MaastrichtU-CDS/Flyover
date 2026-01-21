const JSONLDMapper = {
    mapping: null,

    formatToTitleCase: function(str) {
        return str.charAt(0).toUpperCase() + str.slice(1).replace(/_/g, ' ');
    },

    formatToSnakeCase: function(str) {
        if (str === null || str === undefined) return '';
        return String(str).toLowerCase().replace(/\s+/g, '_');
    },

    formatDataTypeDisplay: function(dataType) {
        return dataType.toLowerCase();
    },

    getVariableKeyFromColumn: function(colData) {
        return colData.variable || colData.mapsTo?.split('/').pop();
    },

    normalizeLocalMappings: function(localMappings) {
        for (const [key, value] of Object.entries(localMappings)) {
            if (!Array.isArray(value)) {
                localMappings[key] = (value !== null && value !== undefined) ? [value] : [];
            }
        }
        return localMappings;
    },

    saveMapping: async function(mappingData) {
        try {
            await FlyoverDB.saveData('metadata', {
                key: 'semantic_map',
                data: mappingData || this.mapping,
                timestamp: new Date().toISOString()
            });
            return true;
        } catch (error) {
            return false;
        }
    },

    getUniqueVariableName: function(baseName, usedVariables) {
        if (!(baseName in usedVariables)) {
            usedVariables[baseName] = 0;
            return baseName;
        }

        const suffix = usedVariables[baseName] + 1;
        usedVariables[baseName] = suffix;
        return `${baseName}_${suffix}`;
    },

    forEachColumn: function(databases, callback) {
        for (const [dbKey, dbData] of Object.entries(databases)) {
            if (!dbData?.tables) continue;

            for (const [tableKey, tableData] of Object.entries(dbData.tables)) {
                if (!tableData?.columns) continue;

                for (const [colKey, colData] of Object.entries(tableData.columns)) {
                    const shouldContinue = callback(colData, colKey, tableData, tableKey, dbData, dbKey);
                    if (shouldContinue === false) return;
                }
            }
        }
    },

    findColumnForVariable: function(databases, globalVarName, localVariable = null, matchingDatabase = null) {
        let foundColumn = null;

        this.forEachColumn(databases, (colData, colKey, tableData, tableKey, dbData, dbKey) => {
            if (matchingDatabase && !this.graphDatabaseFindNameMatch(dbData.name, matchingDatabase)) {
                return;
            }

            const varKey = this.getVariableKeyFromColumn(colData);
            if (varKey === globalVarName) {
                if (localVariable) {
                    if (colData.localColumn === localVariable) {
                        foundColumn = { colData, colKey, tableData, tableKey, dbData, dbKey };
                        return false;
                    }
                } else {
                    foundColumn = { colData, colKey, tableData, tableKey, dbData, dbKey };
                    return false;
                }
            }
        });

        return foundColumn;
    },

    loadFromIndexedDB: async function() {
        const result = await FlyoverDB.getData('metadata', 'semantic_map');

        if (result && result.data) {
            this.mapping = result.data;
            return true;
        }
        return false;
    },

    initializeEmptyMapping: function() {
        this.mapping = {
            '@context': {
                'schema': 'mapping:schema/',
                'mapping': 'http://example.org/mapping#'
            },
            '@id': 'mapping:root',
            '@type': 'mapping:SemanticMapping',
            schema: {
                '@id': 'schema:root',
                '@type': 'mapping:Schema',
                variables: {}
            },
            databases: {}
        };
    },

    getFirstDatabaseName: function() {
        if (!this.mapping || !this.mapping.databases) {
            return null;
        }

        const dbKeys = Object.keys(this.mapping.databases);
        const dbName = dbKeys.length > 0 ? this.mapping.databases[dbKeys[0]].name : null;
        return dbName;
    },

    getAllVariableKeys: function() {
        if (!this.mapping?.schema?.variables) {
            return [];
        }

        const keys = Object.keys(this.mapping.schema.variables);
        return keys;
    },

    getVariable: function(varKey) {
        const variable = this.mapping?.schema?.variables?.[varKey] || null;
        return variable;
    },

    getLocalColumn: function(varKey) {
        if (!this.mapping?.databases) {
            return varKey;
        }

        let result = varKey;
        this.forEachColumn(this.mapping.databases, (colData) => {
            if (colData.variable === varKey || colData.mapsTo === `schema:variable/${varKey}`) {
                result = colData.localColumn || varKey;
                return false;
            }
        });
        return result;
    },

    graphDatabaseFindNameMatch: function(mapDbName, targetDb) {
        if (!mapDbName || mapDbName === '') return true;
        if (mapDbName === targetDb) return true;

        const mapNoExt = mapDbName.endsWith('.csv') ? mapDbName.slice(0, -4) : mapDbName;
        const targetNoExt = targetDb.endsWith('.csv') ? targetDb.slice(0, -4) : targetDb;
        return mapNoExt === targetNoExt;
    },

    getDatabasesFromDOM: function() {
        const databases = [];
        $('h2:has(.fa-database)').each(function() {
            const dbName = $(this).text().trim();
            if (dbName) databases.push(dbName);
        });
        return databases;
    },

    computePreselections: function() {
        const preselectedDescriptions = {};
        const preselectedDatatypes = {};
        const descriptionToDatatype = {};

        if (!this.mapping) {
            return { preselectedDescriptions, preselectedDatatypes, descriptionToDatatype };
        }

        const databases = this.getDatabasesFromDOM();

        for (const varName of this.getAllVariableKeys()) {
            const varInfo = this.getVariable(varName);
            if (!varInfo) {
                continue;
            }

            const descriptionDisplay = this.formatToTitleCase(varName);
            const datatypeDisplay = varInfo.dataType ? this.formatDataTypeDisplay(varInfo.dataType) : null;

            if (datatypeDisplay) {
                descriptionToDatatype[descriptionDisplay] = datatypeDisplay;
            }

            const localDef = this.getLocalColumn(varName);

            for (const db of databases) {
                const key = `${db}_${localDef}`;

                preselectedDescriptions[key] = descriptionDisplay;

                if (datatypeDisplay) {
                    preselectedDatatypes[key] = datatypeDisplay;
                }
            }
        }

        return { preselectedDescriptions, preselectedDatatypes, descriptionToDatatype };
    },

    getGlobalVariableNames: function() {
        if (!this.mapping?.schema?.variables) {
            return ['Other'];
        }

        const variableNames = this.getAllVariableKeys().map(name => {
            return this.formatToTitleCase(name);
        });

        return variableNames.concat(['Other']);
    },

    populateDescriptionOptions: function() {
        const globalNames = this.getGlobalVariableNames();

        $('select.description-select').each(function() {
            const $select = $(this);
            const currentValue = $select.val();

            $select.empty();
            $select.append('<option value="">Description</option>');

            globalNames.forEach(name => {
                $select.append(`<option value="${name}">${name}</option>`);
            });

            if (currentValue && globalNames.includes(currentValue)) {
                $select.val(currentValue);
            }
        });

        $('select.description-select').selectpicker();
        $('select.datatype-select').selectpicker();
    },

    applySelectionsToElements: function(selector, preselections) {
        let count = 0;
        $(selector).each(function() {
            const $select = $(this);
            const key = `${$select.data('database')}_${$select.data('item')}`;

            if (key in preselections) {
                $select.selectpicker('val', preselections[key]);
                count++;
            }
        });
        return count;
    },

    applyPreselections: function(preselectedDescriptions, preselectedDatatypes, descriptionToDatatype) {
        this.applySelectionsToElements('select.description-select', preselectedDescriptions);
        this.applySelectionsToElements('select.datatype-select', preselectedDatatypes);
        window.descriptionToDatatype = descriptionToDatatype;
    },

    updateMappingFromForm: async function(formData) {
        if (!this.mapping) {
            this.initializeEmptyMapping();
        }

        for (const [key, data] of Object.entries(formData)) {
            const descriptionDisplay = data.description;
            if (!descriptionDisplay || descriptionDisplay === 'Other') {
                continue;
            }

            const databaseName = data.database;
            if (!databaseName) {
                continue;
            }

            const prefix = databaseName + '_';
            const localColumnName = key.startsWith(prefix) ? key.substring(prefix.length) : key;

            const varName = this.formatToSnakeCase(descriptionDisplay);

            if (!this.mapping.schema.variables[varName]) {
                this.mapping.schema.variables[varName] = {
                    name: varName,
                    description: descriptionDisplay,
                    dataType: data.datatype || null
                };
            } else if (data.datatype) {
                this.mapping.schema.variables[varName].dataType = data.datatype;
            }

            this.forEachColumn(this.mapping.databases || {}, (colData, colKey, tableData) => {
                const colVarKey = this.getVariableKeyFromColumn(colData);

                if (colVarKey !== varName && colData.mapsTo !== `schema:variable/${varName}`) {
                    if (colData.localColumn === localColumnName) {
                        delete tableData.columns[colKey];
                    }
                }
            });

            let columnFound = false;
            this.forEachColumn(this.mapping.databases || {}, (colData, colKey, tableData, tableKey, dbData) => {
                if (columnFound) return false;
                if (!this.graphDatabaseFindNameMatch(dbData.name, databaseName)) return;

                const colVarKey = this.getVariableKeyFromColumn(colData);

                if (colVarKey === varName || colData.mapsTo === `schema:variable/${varName}`) {
                    colData.localColumn = localColumnName;

                    columnFound = true;
                    return false;
                }
            });

            if (!columnFound) {
                let dbFound = false;
                this.forEachColumn(this.mapping.databases || {}, (colData, colKey, tableData, tableKey, dbData) => {
                    if (!this.graphDatabaseFindNameMatch(dbData.name, databaseName)) return;

                    dbFound = true;
                    const newColKey = varName;
                    tableData.columns[newColKey] = {
                        mapsTo: `schema:variable/${varName}`,
                        variable: varName,
                        localColumn: localColumnName
                    };
                    return false;
                });

                if (!dbFound) {
                    this.ensureDatabaseExists(databaseName);
                    const dbKey = this.formatToSnakeCase(databaseName);
                    const tableKey = Object.keys(this.mapping.databases[dbKey].tables)[0];
                    this.mapping.databases[dbKey].tables[tableKey].columns[varName] = {
                        mapsTo: `schema:variable/${varName}`,
                        variable: varName,
                        localColumn: localColumnName
                    };
                }
            }
        }

        return await this.saveMapping();
    },

    ensureDatabaseExists: function(databaseName) {
        if (!this.mapping.databases) {
            this.mapping.databases = {};
        }

        const dbKey = this.formatToSnakeCase(databaseName);

        let dbExists = false;
        for (const [key, dbData] of Object.entries(this.mapping.databases)) {
            if (this.graphDatabaseFindNameMatch(dbData.name, databaseName)) {
                dbExists = true;
                break;
            }
        }

        if (!dbExists) {
            const tableKey = 'data';
            this.mapping.databases[dbKey] = {
                '@id': `mapping:database/${dbKey}`,
                '@type': 'mapping:Database',
                name: databaseName,
                description: '',
                tables: {
                    [tableKey]: {
                        '@id': `mapping:table/${dbKey}/${tableKey}`,
                        '@type': 'mapping:Table',
                        sourceFile: databaseName,
                        description: '',
                        columns: {}
                    }
                }
            };
        }
    },

    formulateLocalSemanticMap: async function(database) {
        if (!this.mapping) {
            return null;
        }

        const modifiedMapping = JSON.parse(JSON.stringify(this.mapping));

        this.forEachColumn(modifiedMapping.databases || {}, (colData, colKey, tableData, tableKey, dbData) => {
            if (!this.graphDatabaseFindNameMatch(dbData.name, database)) return;

            colData.localColumn = null;

            if (colData.localMappings) {
                for (const termKey of Object.keys(colData.localMappings)) {
                    colData.localMappings[termKey] = null;
                }
            }
        });

        if (typeof descriptiveInfo === 'undefined') {
            return null;
        }

        if (!descriptiveInfo[database]) {
            return modifiedMapping;
        }

        const usedGlobalVariables = {};

        for (const [localVariable, localValue] of Object.entries(descriptiveInfo[database])) {
            if (!localValue.description?.trim()) {
                continue;
            }

            const descParts = localValue.description.split('Variable description: ');
            if (descParts.length < 2) {
                continue;
            }

            const globalVariable = this.formatToSnakeCase(descParts[1]);

            if (!modifiedMapping.schema?.variables?.[globalVariable]) {
                continue;
            }

            const newGlobalVariable = this.getUniqueVariableName(globalVariable, usedGlobalVariables);

            if (newGlobalVariable !== globalVariable) {
                modifiedMapping.schema.variables[newGlobalVariable] =
                    JSON.parse(JSON.stringify(modifiedMapping.schema.variables[globalVariable]));
            }

            if (localValue.type) {
                const typeParts = localValue.type.split('Variable type: ');
                if (typeParts.length >= 2) {
                    const dataType = this.formatToSnakeCase(typeParts[1]);
                    if (dataType?.trim()) {
                        modifiedMapping.schema.variables[newGlobalVariable].dataType = dataType;
                    }
                }
            }

            this.forEachColumn(modifiedMapping.databases || {}, (colData) => {
                const varKey = this.getVariableKeyFromColumn(colData);

                if (varKey === globalVariable || varKey === newGlobalVariable) {
                    colData.localColumn = localVariable;

                    if (newGlobalVariable !== globalVariable) {
                        colData.variable = newGlobalVariable;
                        colData.mapsTo = `schema:variable/${newGlobalVariable}`;
                    }

                    const varInfo = modifiedMapping.schema.variables[newGlobalVariable];

                    if (varInfo.valueMapping?.terms) {
                        if (!colData.localMappings) {
                            colData.localMappings = {};
                        }

                        for (const termKey of Object.keys(varInfo.valueMapping.terms)) {
                            colData.localMappings[termKey] = null;
                        }

                        const usedGlobalTerms = {};

                        for (const [category, value] of Object.entries(localValue)) {
                            if (!category.startsWith('Category: ')) continue;
                            if (!value?.trim()) continue;

                            try {
                                const valueParts = value.split(': ');
                                if (valueParts.length < 2) {
                                    continue;
                                }

                                const globalTerm = valueParts[1]
                                    .split(', comment')[0]
                                    .toLowerCase()
                                    .replace(/ /g, '_');

                                const localTerm = String(category.split(': ')[1]);

                                if (!varInfo.valueMapping.terms[globalTerm]) {
                                    continue;
                                }

                                const newGlobalTerm = this.getUniqueVariableName(globalTerm, usedGlobalTerms);

                                colData.localMappings[newGlobalTerm] = localTerm;
                            } catch (error) {
                            }
                        }
                    }
                }
            });
        }

        this.mapping = modifiedMapping;

        return await this.saveMapping(modifiedMapping);
    },

    getCategoryOptionsForVariable: function(database, globalVarName) {
        if (!this.mapping?.schema?.variables) {
            return [];
        }

        let varInfo = this.mapping.schema.variables[globalVarName];

        if (!varInfo) {
            const normalizedName = globalVarName.toLowerCase().replace(/\s+/g, '_');
            for (const [key, value] of Object.entries(this.mapping.schema.variables)) {
                if (key.toLowerCase().replace(/\s+/g, '_') === normalizedName) {
                    varInfo = value;
                    break;
                }
            }
        }

        if (!varInfo?.valueMapping?.terms) {
            return [];
        }

        const options = Object.keys(varInfo.valueMapping.terms).map(termKey => {
            return this.formatToTitleCase(termKey);
        });

        return options;
    },

    getLocalMappingsForVariable: function(database, localVariable, globalVarName) {
        if (!this.mapping?.databases) {
            return {};
        }

        let result = this.findColumnForVariable(this.mapping.databases, globalVarName, localVariable, database);

        if (!result) {
            result = this.findColumnForVariable(this.mapping.databases, globalVarName, null, database);
        }

        if (!result) {
            result = this.findColumnForVariable(this.mapping.databases, globalVarName, null, null);
        }

        if (result) {
            const localMappings = result.colData.localMappings || {};
            return this.normalizeLocalMappings(localMappings);
        }

        return {};
    },

    updateCategoryMapping: async function(database, localVariable, globalVarName, localValue, selectedOption, previousOption) {
        if (!this.mapping?.databases) {
            return false;
        }

        localValue = String(localValue);

        const termKey = selectedOption ? this.formatToSnakeCase(selectedOption) : null;
        const previousTermKey = previousOption ? this.formatToSnakeCase(previousOption) : null;

        const result = this.findColumnForVariable(this.mapping.databases, globalVarName, localVariable, database);

        if (!result) {
            return false;
        }

        const colData = result.colData;

        if (!colData.localMappings) {
            colData.localMappings = {};
        }

        if (previousTermKey && (colData.localMappings[previousTermKey] !== null && colData.localMappings[previousTermKey] !== undefined)) {
            if (!Array.isArray(colData.localMappings[previousTermKey])) {
                colData.localMappings[previousTermKey] = [colData.localMappings[previousTermKey]];
            }

            const prevIndex = colData.localMappings[previousTermKey].indexOf(localValue);
            if (prevIndex > -1) {
                colData.localMappings[previousTermKey].splice(prevIndex, 1);

                if (colData.localMappings[previousTermKey].length === 0) {
                    delete colData.localMappings[previousTermKey];
                }
            }
        }

        if (termKey) {
            if (!colData.localMappings[termKey]) {
                colData.localMappings[termKey] = [];
            } else if (!Array.isArray(colData.localMappings[termKey])) {
                colData.localMappings[termKey] = [colData.localMappings[termKey]];
            }

            if (!colData.localMappings[termKey].includes(localValue)) {
                colData.localMappings[termKey].push(localValue);
            }
        }

        return await this.saveMapping();
    }
};

if (typeof module !== 'undefined' && module.exports) {
    module.exports = JSONLDMapper;
}
