// IndexedDB wrapper for Flyover. Ported from static/js/db-utils.js (the
// global FlyoverDB object) into an ES module — same shape, same store names.

const DB_NAME = 'FlyoverDB'
const DB_VERSION = 2

let dbPromise = null

function openDb() {
  if (dbPromise) return dbPromise
  dbPromise = new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION)
    req.onerror = () => reject(req.error)
    req.onsuccess = () => resolve(req.result)
    req.onupgradeneeded = (e) => {
      const db = e.target.result
      if (!db.objectStoreNames.contains('variables')) {
        const s = db.createObjectStore('variables', { keyPath: 'id' })
        s.createIndex('database', 'database', { unique: false })
        s.createIndex('timestamp', 'timestamp', { unique: false })
      }
      if (!db.objectStoreNames.contains('metadata')) {
        db.createObjectStore('metadata', { keyPath: 'key' })
      }
      if (!db.objectStoreNames.contains('descriptions')) {
        const s = db.createObjectStore('descriptions', { keyPath: 'id' })
        s.createIndex('timestamp', 'timestamp', { unique: false })
      }
    }
  })
  return dbPromise
}

async function tx(storeName, mode, fn) {
  const db = await openDb()
  return new Promise((resolve, reject) => {
    const t = db.transaction([storeName], mode)
    const store = t.objectStore(storeName)
    const result = fn(store)
    t.oncomplete = () => resolve(result?.result ?? result)
    t.onerror = () => reject(t.error)
    t.onabort = () => reject(t.error)
  })
}

export async function saveData(storeName, data) {
  return tx(storeName, 'readwrite', (s) => s.put(data))
}

export async function saveBulkData(storeName, dataArray) {
  return tx(storeName, 'readwrite', (s) => {
    for (const d of dataArray) s.put(d)
  })
}

export async function getData(storeName, key) {
  const db = await openDb()
  return new Promise((resolve, reject) => {
    const t = db.transaction([storeName], 'readonly')
    const req = t.objectStore(storeName).get(key)
    req.onsuccess = () => resolve(req.result || null)
    req.onerror = () => reject(req.error)
  })
}

export async function getAllData(storeName) {
  const db = await openDb()
  return new Promise((resolve, reject) => {
    const t = db.transaction([storeName], 'readonly')
    const req = t.objectStore(storeName).getAll()
    req.onsuccess = () => resolve(req.result || [])
    req.onerror = () => reject(req.error)
  })
}

export async function getDataByIndex(storeName, indexName, indexValue) {
  const db = await openDb()
  return new Promise((resolve, reject) => {
    const t = db.transaction([storeName], 'readonly')
    const idx = t.objectStore(storeName).index(indexName)
    const req = idx.getAll(indexValue)
    req.onsuccess = () => resolve(req.result || [])
    req.onerror = () => reject(req.error)
  })
}

export async function clearData(storeName) {
  return tx(storeName, 'readwrite', (s) => s.clear())
}

export async function deleteData(storeName, key) {
  return tx(storeName, 'readwrite', (s) => s.delete(key))
}

export function isSupported() {
  return typeof indexedDB !== 'undefined'
}

export async function getCacheMetadata(storeName, key) {
  const data = await getData(storeName, key)
  if (!data) return null
  const cacheTime = data.timestamp ? new Date(data.timestamp) : null
  const ageMinutes = cacheTime ? (Date.now() - cacheTime.getTime()) / 60000 : null
  return {
    exists: true,
    timestamp: data.timestamp,
    cacheTime,
    ageMinutes,
    version: data.version || 0,
    key,
    storeName,
  }
}

export async function isCacheStale(storeName, key, maxAgeMinutes = 60) {
  const meta = await getCacheMetadata(storeName, key)
  if (!meta || meta.ageMinutes == null) return true
  return meta.ageMinutes > maxAgeMinutes
}
