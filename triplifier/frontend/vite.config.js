import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath, URL } from 'node:url'

export default defineConfig({
  plugins: [
    vue({
      template: {
        transformAssetUrls: {
          // Don't try to bundle absolute /static/* paths — they're served by Flask.
          includeAbsolute: false,
        },
      },
    }),
  ],
  base: '/app/',
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:5000',
      '/static': 'http://localhost:5000',
      '/downloadOntology': 'http://localhost:5000',
      '/downloadSemanticMap': 'http://localhost:5000',
      '/upload': 'http://localhost:5000',
      '/upload-semantic-map': 'http://localhost:5000',
      '/upload-annotation-json': 'http://localhost:5000',
      '/submit-indexeddb-semantic-map': 'http://localhost:5000',
      '/start-annotation': 'http://localhost:5000',
      '/verify-annotation-ask': 'http://localhost:5000',
      '/units': 'http://localhost:5000',
      '/end': 'http://localhost:5000',
      '/get-existing-graph-structure': 'http://localhost:5000',
    },
  },
})
