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
  server: {
    port: 5173,
    // the dev-server twin of nginx.conf.template's /memes/ location: the favourites view resolves
    // its references same-origin, because only a real HTTP status can tell "the gallery does not
    // have this" apart from "the gallery did not answer" (src/refs.ts)
    proxy: { '/memes': 'http://localhost:8083' },
  },
});
