import { defineConfig, mergeConfig } from 'vitest/config'
import viteConfig from './vite.config.js'

export default mergeConfig(
  viteConfig,
  defineConfig({
    test: {
      environment: 'happy-dom',
      include: ['tests/unit/**/*.spec.{js,ts}'],
      globals: true,
      // `npm run test:unit:coverage` (CI) measures how much of src/ the unit
      // tests run, and fails below these floors: main's figures on
      // 2026-11-09, rounded down. Raise them as coverage grows.
      coverage: {
        provider: 'v8',
        include: ['src/**/*.{js,vue}'],
        reporter: ['text-summary', 'json-summary'],
        reportsDirectory: 'coverage',
        thresholds: { lines: 73, statements: 73, functions: 74, branches: 82 },
      },
    },
  })
)
