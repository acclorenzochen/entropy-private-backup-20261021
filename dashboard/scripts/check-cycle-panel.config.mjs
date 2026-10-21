import { fileURLToPath } from 'node:url';
export default {
  root: fileURLToPath(new URL('.', import.meta.url)), base: '/qa/',
  build: { outDir: '/tmp/entropy-dashboard-panel-tests', emptyOutDir: true,
    rollupOptions: { input: fileURLToPath(new URL('check-cycle-panel.html', import.meta.url)) } },
};
