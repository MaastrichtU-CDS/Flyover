import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "/v2/",
  plugins: [react()],
  server: {
    proxy: { "/api": "http://localhost:5000" },
  },
});
