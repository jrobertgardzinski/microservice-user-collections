import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// React Native's component model in the browser: every `react-native` import resolves to
// react-native-web. The web build is deliberate — a native app never meets CORS; the browser
// build is what exercises the collections service's cross-origin edge.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { 'react-native': 'react-native-web' },
  },
  server: { port: 5173 },
});
