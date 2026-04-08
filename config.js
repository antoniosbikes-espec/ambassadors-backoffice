/**
 * Ambassadors — Configuración de entorno
 *
 * INSTRUCCIONES:
 * 1. Despliega el backend en Railway
 * 2. Copia la URL pública que Railway te da (ej: https://ambassadors-production.up.railway.app)
 * 3. Pégala abajo en RAILWAY_URL (sin barra final)
 * 4. Sube los cambios a Netlify (git push o drag & drop)
 */

const RAILWAY_URL = 'PEGA_AQUI_TU_URL_DE_RAILWAY'; // ← edita esto

// ─── No tocar lo de abajo ──────────────────────────────────
const CONFIG = {
  API_URL: (function () {
    const isLocal = window.location.hostname === 'localhost'
                 || window.location.hostname === '127.0.0.1'
                 || window.location.protocol === 'file:';

    if (isLocal) return 'http://localhost:8787/api';

    if (!RAILWAY_URL || RAILWAY_URL === 'PEGA_AQUI_TU_URL_DE_RAILWAY') {
      console.warn('[CONFIG] ⚠️ Railway URL no configurada. Edita config.js');
      return 'http://localhost:8787/api'; // fallback
    }

    return RAILWAY_URL.replace(/\/$/, '') + '/api';
  })()
};
