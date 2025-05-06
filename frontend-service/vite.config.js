import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
// import fronta

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(),tailwindcss()],
  // vite.config.js
server: {
  host: '0.0.0.0',
  port: 5173
}
})

