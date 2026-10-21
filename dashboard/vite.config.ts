import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    proxy: {
      "/api/v3-control": process.env.V3_OWNER_CONTROL_URL || "http://127.0.0.1:8786",
      "/api/v3": process.env.V3_DASHBOARD_API_URL || "http://127.0.0.1:8766",
    },
  },
});
