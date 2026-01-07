/**
 * IndexedDB Utility Wrapper for Flyover
 * Provides simple interface for client-side data storage
 */

const FlyoverDB = {
    dbName: 'FlyoverDB',
    version: 1,
    db: null,

    /**
     * Initialize the IndexedDB database
     * Creates object stores for different data types
     * @returns {Promise<IDBDatabase>}
     */
    async initDB() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(this.dbName, this.version);

            request.onerror = () => {
                console.error('IndexedDB: Failed to open database', request.error);
                reject(request.error);
            };

            request.onsuccess = () => {
                this.db = request.result;
                console.log('IndexedDB: Database initialized successfully');
                resolve(this.db);
            };

            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                console.log('IndexedDB: Upgrading database schema...');

                // Create object store for variable data
                if (!db.objectStoreNames.contains('variables')) {
                    const variableStore = db.createObjectStore('variables', { keyPath: 'id' });
                    variableStore.createIndex('database', 'database', { unique: false });
                    variableStore.createIndex('timestamp', 'timestamp', { unique: false });
                    console.log('IndexedDB: Created "variables" object store');
                }

                // Create object store for metadata (cache version, timestamps, etc.)
                if (!db.objectStoreNames.contains('metadata')) {
                    db.createObjectStore('metadata', { keyPath: 'key' });
                    console.log('IndexedDB: Created "metadata" object store');
                }

                // Create object store for descriptions mapping
                if (!db.objectStoreNames.contains('descriptions')) {
                    const descStore = db.createObjectStore('descriptions', { keyPath: 'id' });
                    descStore.createIndex('timestamp', 'timestamp', { unique: false });
                    console.log('IndexedDB: Created "descriptions" object store');
                }
            };
        });
    },

    /**
     * Save data to a specific object store
     * @param {string} storeName - Name of the object store
     * @param {object} data - Data to save (must include the keyPath field)
     * @returns {Promise<void>}
     */
    async saveData(storeName, data) {
        if (!this.db) {
            await this.initDB();
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readwrite');
            const store = transaction.objectStore(storeName);
            const request = store.put(data);

            request.onsuccess = () => {
                console.log(`IndexedDB: Saved data to "${storeName}"`, data);
                resolve();
            };

            request.onerror = () => {
                console.error(`IndexedDB: Failed to save data to "${storeName}"`, request.error);
                reject(request.error);
            };
        });
    },

    /**
     * Save multiple data items to a specific object store
     * @param {string} storeName - Name of the object store
     * @param {Array} dataArray - Array of data objects to save
     * @returns {Promise<void>}
     */
    async saveBulkData(storeName, dataArray) {
        if (!this.db) {
            await this.initDB();
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readwrite');
            const store = transaction.objectStore(storeName);

            let successCount = 0;
            let errorOccurred = false;

            dataArray.forEach((data, index) => {
                const request = store.put(data);

                request.onsuccess = () => {
                    successCount++;
                    if (successCount === dataArray.length && !errorOccurred) {
                        console.log(`IndexedDB: Saved ${successCount} items to "${storeName}"`);
                        resolve();
                    }
                };

                request.onerror = () => {
                    if (!errorOccurred) {
                        errorOccurred = true;
                        console.error(`IndexedDB: Failed to save bulk data to "${storeName}"`, request.error);
                        reject(request.error);
                    }
                };
            });
        });
    },

    /**
     * Get data from a specific object store by key
     * @param {string} storeName - Name of the object store
     * @param {string|number} key - Key to retrieve
     * @returns {Promise<object|null>}
     */
    async getData(storeName, key) {
        if (!this.db) {
            await this.initDB();
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readonly');
            const store = transaction.objectStore(storeName);
            const request = store.get(key);

            request.onsuccess = () => {
                console.log(`IndexedDB: Retrieved data from "${storeName}" with key "${key}"`, request.result);
                resolve(request.result || null);
            };

            request.onerror = () => {
                console.error(`IndexedDB: Failed to get data from "${storeName}"`, request.error);
                reject(request.error);
            };
        });
    },

    /**
     * Get all data from a specific object store
     * @param {string} storeName - Name of the object store
     * @returns {Promise<Array>}
     */
    async getAllData(storeName) {
        if (!this.db) {
            await this.initDB();
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readonly');
            const store = transaction.objectStore(storeName);
            const request = store.getAll();

            request.onsuccess = () => {
                console.log(`IndexedDB: Retrieved all data from "${storeName}"`, request.result.length, 'items');
                resolve(request.result);
            };

            request.onerror = () => {
                console.error(`IndexedDB: Failed to get all data from "${storeName}"`, request.error);
                reject(request.error);
            };
        });
    },

    /**
     * Get data from object store by index
     * @param {string} storeName - Name of the object store
     * @param {string} indexName - Name of the index
     * @param {any} indexValue - Value to search for in the index
     * @returns {Promise<Array>}
     */
    async getDataByIndex(storeName, indexName, indexValue) {
        if (!this.db) {
            await this.initDB();
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readonly');
            const store = transaction.objectStore(storeName);
            const index = store.index(indexName);
            const request = index.getAll(indexValue);

            request.onsuccess = () => {
                console.log(`IndexedDB: Retrieved data from "${storeName}" by index "${indexName}=${indexValue}"`, request.result.length, 'items');
                resolve(request.result);
            };

            request.onerror = () => {
                console.error(`IndexedDB: Failed to get data by index from "${storeName}"`, request.error);
                reject(request.error);
            };
        });
    },

    /**
     * Clear all data from a specific object store
     * @param {string} storeName - Name of the object store
     * @returns {Promise<void>}
     */
    async clearData(storeName) {
        if (!this.db) {
            await this.initDB();
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readwrite');
            const store = transaction.objectStore(storeName);
            const request = store.clear();

            request.onsuccess = () => {
                console.log(`IndexedDB: Cleared all data from "${storeName}"`);
                resolve();
            };

            request.onerror = () => {
                console.error(`IndexedDB: Failed to clear data from "${storeName}"`, request.error);
                reject(request.error);
            };
        });
    },

    /**
     * Delete a specific item from an object store
     * @param {string} storeName - Name of the object store
     * @param {string|number} key - Key to delete
     * @returns {Promise<void>}
     */
    async deleteData(storeName, key) {
        if (!this.db) {
            await this.initDB();
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readwrite');
            const store = transaction.objectStore(storeName);
            const request = store.delete(key);

            request.onsuccess = () => {
                console.log(`IndexedDB: Deleted data from "${storeName}" with key "${key}"`);
                resolve();
            };

            request.onerror = () => {
                console.error(`IndexedDB: Failed to delete data from "${storeName}"`, request.error);
                reject(request.error);
            };
        });
    },

    /**
     * Check if IndexedDB is supported by the browser
     * @returns {boolean}
     */
    isSupported() {
        const supported = 'indexedDB' in window;
        if (!supported) {
            console.warn('IndexedDB: Not supported by this browser');
        }
        return supported;
    },

    /**
     * Get database statistics
     * @returns {Promise<object>}
     */
    async getStats() {
        if (!this.db) {
            await this.initDB();
        }

        const stats = {};
        const storeNames = Array.from(this.db.objectStoreNames);

        for (const storeName of storeNames) {
            const data = await this.getAllData(storeName);
            stats[storeName] = {
                count: data.length,
                size: JSON.stringify(data).length // Approximate size in bytes
            };
        }

        console.log('IndexedDB: Database statistics', stats);
        return stats;
    },

    /**
     * Check if cached data is stale based on timestamp and max age
     * @param {string} storeName - Name of the object store
     * @param {string} key - Key to check
     * @param {number} maxAgeMinutes - Maximum age in minutes (default: 60)
     * @returns {Promise<boolean>} True if cache is stale
     */
    async isCacheStale(storeName, key, maxAgeMinutes = 60) {
        try {
            const data = await this.getData(storeName, key);
            
            if (!data || !data.timestamp) {
                console.log(`IndexedDB: No cached data found for "${key}" in "${storeName}"`);
                return true; // No cache means it's stale
            }

            const cacheTime = new Date(data.timestamp);
            const now = new Date();
            const ageMinutes = (now - cacheTime) / (1000 * 60);

            const isStale = ageMinutes > maxAgeMinutes;
            
            if (isStale) {
                console.log(`IndexedDB: Cache stale for "${key}" (${ageMinutes.toFixed(1)} minutes old, max ${maxAgeMinutes})`);
            } else {
                console.log(`IndexedDB: Cache fresh for "${key}" (${ageMinutes.toFixed(1)} minutes old)`);
            }

            return isStale;
        } catch (error) {
            console.error(`IndexedDB: Error checking cache staleness:`, error);
            return true; // Assume stale on error
        }
    },

    /**
     * Check if cached data version matches expected version
     * @param {string} storeName - Name of the object store
     * @param {string} key - Key to check
     * @param {number} expectedVersion - Expected version number
     * @returns {Promise<boolean>} True if version matches
     */
    async isCacheVersionValid(storeName, key, expectedVersion) {
        try {
            const data = await this.getData(storeName, key);
            
            if (!data) {
                console.log(`IndexedDB: No cached data found for version check`);
                return false;
            }

            const currentVersion = data.version || 0;
            const isValid = currentVersion === expectedVersion;
            
            if (!isValid) {
                console.log(`IndexedDB: Version mismatch for "${key}" (cached: ${currentVersion}, expected: ${expectedVersion})`);
            } else {
                console.log(`IndexedDB: Version valid for "${key}" (version ${currentVersion})`);
            }

            return isValid;
        } catch (error) {
            console.error(`IndexedDB: Error checking cache version:`, error);
            return false;
        }
    },

    /**
     * Get cache metadata including age and version
     * @param {string} storeName - Name of the object store
     * @param {string} key - Key to check
     * @returns {Promise<object|null>} Cache metadata or null
     */
    async getCacheMetadata(storeName, key) {
        try {
            const data = await this.getData(storeName, key);
            
            if (!data) {
                return null;
            }

            const cacheTime = data.timestamp ? new Date(data.timestamp) : null;
            const now = new Date();
            const ageMinutes = cacheTime ? (now - cacheTime) / (1000 * 60) : null;

            return {
                exists: true,
                timestamp: data.timestamp,
                cacheTime: cacheTime,
                ageMinutes: ageMinutes,
                version: data.version || 0,
                key: key,
                storeName: storeName
            };
        } catch (error) {
            console.error(`IndexedDB: Error getting cache metadata:`, error);
            return null;
        }
    },

    /**
     * Invalidate (delete) stale cache entries across all stores
     * @param {number} maxAgeMinutes - Maximum age in minutes
     * @returns {Promise<number>} Number of entries deleted
     */
    async invalidateStaleCache(maxAgeMinutes = 60) {
        if (!this.db) {
            await this.initDB();
        }

        let deletedCount = 0;
        const storeNames = Array.from(this.db.objectStoreNames);

        for (const storeName of storeNames) {
            const allData = await this.getAllData(storeName);
            
            for (const item of allData) {
                if (item.timestamp) {
                    const cacheTime = new Date(item.timestamp);
                    const now = new Date();
                    const ageMinutes = (now - cacheTime) / (1000 * 60);

                    if (ageMinutes > maxAgeMinutes) {
                        await this.deleteData(storeName, item.id || item.key);
                        deletedCount++;
                    }
                }
            }
        }

        console.log(`IndexedDB: Invalidated ${deletedCount} stale cache entries`);
        return deletedCount;
    },

    /**
     * Update cache timestamp to mark as fresh
     * @param {string} storeName - Name of the object store
     * @param {string} key - Key to update
     * @returns {Promise<boolean>}
     */
    async touchCache(storeName, key) {
        try {
            const data = await this.getData(storeName, key);
            
            if (!data) {
                return false;
            }

            data.timestamp = new Date().toISOString();
            await this.saveData(storeName, data);
            console.log(`IndexedDB: Updated cache timestamp for "${key}"`);
            return true;
        } catch (error) {
            console.error(`IndexedDB: Error updating cache timestamp:`, error);
            return false;
        }
    }
};

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = FlyoverDB;
}
