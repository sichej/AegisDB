import { defineConfig } from 'vite';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [tailwindcss()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/admin': {
        target: 'http://127.0.0.1:5140',
      },
    },
  },
});
