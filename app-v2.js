/* ============================================
   AMBASSADORS SAAS — APP.JS
   Frontend SPA conectado a API REST (SQLite)
   ============================================ */

'use strict';

// API_URL viene de config.js (local → localhost:8787, producción → Railway)
const API = (typeof CONFIG !== 'undefined') ? CONFIG.API_URL : 'http://localhost:8787/api';

// ─── Estado global ───────────────────────────────────────────
let LISTS = {};          // { platform: [{id,value,code},...], ... }
let selectedAmbassadorId = null;
let currentDetailTab = 'overview';
let chartInstances = {};
let filteredAmbassadors = [];
let AUTH_USER = null;

// ─── Bootstrap ───────────────────────────────────────────────
async function init() {
  console.log("🚀 Ambassadors App — Active");
  document.title = "Ambassadors";

  // 1. Verificar sesión
  const token = localStorage.getItem('ambassadors_token');
  const user = localStorage.getItem('ambassadors_user');

  if (!token) {
    showLogin();
    return;
  }
  AUTH_USER = JSON.parse(user || '{}');
  updateUserUI();

  await loadLists();

  // Rellenar filtros principales...
  const filterCountry = document.getElementById('amb-filter-country');
  const filterPlatform = document.getElementById('amb-filter-platform');
  const filterStatus = document.getElementById('amb-filter-status');

  if (filterCountry) filterCountry.innerHTML = listCodeOptions('country', '— País —');
  if (filterPlatform) filterPlatform.innerHTML = listCodeOptions('platform', '— Plataforma —');
  if (filterStatus) filterStatus.innerHTML = listCodeOptions('contract_status', '— Estado —');

  const postFilterPlatform = document.getElementById('posts-filter-platform');
  const postFilterMention = document.getElementById('posts-filter-mention');
  if (postFilterPlatform) postFilterPlatform.innerHTML = listCodeOptions('platform', 'Plataforma');
  if (postFilterMention) postFilterMention.innerHTML = listCodeOptions('mention_type', 'Tipo mención');

  // Rellenar filtros globales...
  const gCountry = document.getElementById('filter-country');
  const gNiche = document.getElementById('filter-niche');
  const gPlatform = document.getElementById('filter-platform');

  if (gCountry) gCountry.innerHTML = listCodeOptions('country', 'Todos los países');
  if (gNiche) gNiche.innerHTML = listCodeOptions('niche', 'Todos los nichos');
  if (gPlatform) gPlatform.innerHTML = listCodeOptions('platform', 'Todas las plataformas');

  // Listeners para filtros globales
  ['filter-date', 'filter-country', 'filter-niche', 'filter-platform'].forEach(id => {
    document.getElementById(id)?.addEventListener('change', () => {
      const activePage = document.querySelector('.page.active').id;
      if (activePage === 'page-dashboard') renderDashboard();
      if (activePage === 'page-analytics') renderAnalytics();
      if (activePage === 'page-ambassadors') renderAmbassadors();
      if (activePage === 'page-posts') renderPosts();
    });
  });

  renderDashboard();
  navigateTo('dashboard');
}

function showLogin() {
  document.getElementById('login-screen').style.display = 'flex';
  document.getElementById('login-form').onsubmit = async (e) => {
    e.preventDefault();
    const username = e.target.username.value;
    const password = e.target.password.value;
    const errorEl = document.getElementById('login-error');
    errorEl.style.display = 'none';

    try {
      const res = await fetch(`${API}/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password })
      });
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || 'Credenciales incorrectas');
      }
      const data = await res.json();
      localStorage.setItem('ambassadors_token', data.token);
      localStorage.setItem('ambassadors_user', JSON.stringify(data.user));
      window.location.reload();
    } catch (err) {
      errorEl.textContent = err.message;
      errorEl.style.display = 'block';
    }
  };
}

function logout() {
  localStorage.removeItem('ambassadors_token');
  localStorage.removeItem('ambassadors_user');
  window.location.reload();
}

function updateUserUI() {
  if (AUTH_USER) {
    document.querySelector('.user-name').textContent = AUTH_USER.username;
    document.querySelector('.user-avatar').textContent = (AUTH_USER.username || 'A')[0].toUpperCase();

    // Añadir botón de logout si no existe
    if (!document.getElementById('btn-logout')) {
      const footer = document.querySelector('.sidebar-footer');
      const btn = document.createElement('button');
      btn.id = 'btn-logout';
      btn.className = 'btn-logout';
      btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg> Salir';
      btn.onclick = logout;
      footer.appendChild(btn);
    }
  }
}

// ─────────────────────────────────────────────────────────────
// API HELPERS
// ─────────────────────────────────────────────────────────────
async function api(method, endpoint, body) {
  try {
    const token = localStorage.getItem('ambassadors_token');
    const opts = {
      method,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`
      },
    };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(`${API}${endpoint}`, opts);
    if (res.status === 401) {
      logout();
      return;
    }
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || `HTTP ${res.status}`);
    }
    return res.json();
  } catch (e) {
    console.error(`API ${method} ${endpoint}:`, e);
    throw e;
  }
}

const GET = (ep, qs) => {
  const params = new URLSearchParams(qs || {});
  params.set('_t', Date.now()); // Forzar datos frescos
  return api('GET', `${ep}?${params.toString()}`);
};
const POST = (ep, b) => api('POST', ep, b);
const PUT = (ep, b) => api('PUT', ep, b);
const DELETE = (ep) => api('DELETE', ep);

// ─────────────────────────────────────────────────────────────
// LISTS
// ─────────────────────────────────────────────────────────────
async function loadLists() {
  try {
    const data = await GET('/lists?t=' + Date.now());
    console.log("📥 Listas recibidas del servidor:", data);
    if (!Array.isArray(data)) {
      console.error("❌ Error: El servidor no ha devuelto un array de listas.");
      return;
    }
    // Reiniciar LISTS para evitar duplicados si se llama varias veces
    Object.keys(LISTS).forEach(k => delete LISTS[k]);

    data.forEach(item => {
      const key = item.list_name;
      if (!LISTS[key]) LISTS[key] = [];
      LISTS[key].push(item);
    });
    console.log("✅ LISTS poblado correctamente:", LISTS);
  } catch (err) {
    console.error("❌ Error cargando listas:", err);
  }
}

function listById(name, id) {
  return (LISTS[name] || []).find(lv => lv.id === id) || null;
}

function listByCode(name, code) {
  return (LISTS[name] || []).find(lv => lv.code === code) || null;
}

function listOptions(name, placeholder = '', selectedValue = null) {
  const items = LISTS[name] || [];
  if (name === 'mention_type') console.log(`🛠 [listOptions:${name}] Renderizando ${items.length} items. Seleccionado: ${selectedValue}`);
  const ph = placeholder ? `<option value="">${placeholder}</option>` : '';
  return ph + items.map(lv => {
    const selected = String(lv.id) === String(selectedValue) ? 'selected' : '';
    if (name === 'mention_type' && selected) console.log(`   ✅ Encontrado seleccionado: ${lv.value} (ID:${lv.id})`);
    return `<option value="${lv.id}" ${selected}>${lv.value}</option>`;
  }).join('');
}

function listCodeOptions(name, placeholder = '') {
  const items = LISTS[name] || [];
  const ph = placeholder ? `<option value="">${placeholder}</option>` : '';
  return ph + items.map(lv => `<option value="${lv.code}">${lv.value}</option>`).join('');
}

// ─────────────────────────────────────────────────────────────
// UTILS
// ─────────────────────────────────────────────────────────────
function fmt(n, format) {
  n = Number(n) || 0;
  if (format === 'compact') {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
    if (n >= 1_000) return (n / 1_000).toFixed(0) + 'K';
    return n.toLocaleString('es-ES');
  }
  if (format === 'currency') return '€' + n.toLocaleString('es-ES', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  return n.toLocaleString('es-ES');
}

function initials(name) {
  return (name || '?').split(' ').map(w => w[0]).slice(0, 2).join('').toUpperCase();
}

function platformBadge(code, label) {
  const icons = {
    youtube: '<svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M23 7s-.3-1.9-1.2-2.7c-1.1-1.2-2.4-1.2-3-1.3C16.2 3 12 3 12 3s-4.2 0-6.8.1c-.6.1-1.9.1-3 1.3C1.3 5.1 1 7 1 7S.7 9.1.7 11.2v2c0 2.1.3 4.2.3 4.2s.3 1.9 1.2 2.7c1.1 1.2 2.6 1.1 3.3 1.2C7.4 21.4 12 21.4 12 21.4s4.2 0 6.8-.2c.6-.1 1.9-.1 3-1.3.9-.8 1.2-2.7 1.2-2.7s.3-2.1.3-4.2v-2C23.3 9.1 23 7 23 7zM9.7 15.5V8.4l8.1 3.6-8.1 3.5z"/></svg>',
    instagram: '<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="2" width="20" height="20" rx="5"/><circle cx="12" cy="12" r="5"/><circle cx="17.5" cy="6.5" r="1.5" fill="currentColor" stroke="none"/></svg>',
    tiktok: '<svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M19.59 6.69a4.83 4.83 0 0 1-3.77-2.75V13a6 6 0 1 1-4.64-5.85v3.56a2.47 2.47 0 1 0 1.73 2.36V2h3.44a4.83 4.83 0 0 0 4.83 4.83z"/></svg>',
    linkedin: '<svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M16 8a6 6 0 0 1 6 6v7h-4v-7a2 2 0 0 0-2-2 2 2 0 0 0-2 2v7h-4v-7a6 6 0 0 1 6-6zM2 9h4v12H2z"/><circle cx="4" cy="4" r="2"/></svg>',
    twitch: '<svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M11.571 4.714h1.715v5.143H11.57zm4.715 0H18v5.143h-1.714zM6 0L1.714 4.286v15.428h5.143V24l4.286-4.286h3.428L22.286 12V0zm14.571 11.143l-3.428 3.428h-3.429l-3 3v-3H6.857V1.714h13.714z"/></svg>',
  };
  const labels = { youtube: 'YouTube', instagram: 'Instagram', tiktok: 'TikTok', linkedin: 'LinkedIn', twitch: 'Twitch' };
  const c = code || '';
  return `<span class="platform-badge platform-${c}">${icons[c] || ''} ${label || labels[c] || c}</span>`;
}

function statusBadge(code, label) {
  const cls = {
    signed: 'badge-active', active: 'badge-active', offered: 'badge-pending', pending: 'badge-pending',
    draft: 'badge-inactive', expired: 'badge-expired', cancelled: 'badge-expired',
    inactive: 'badge-inactive'
  }[code] || 'badge-inactive';
  return `<span class="badge ${cls}">${label || code}</span>`;
}

function scoreBar(score) {
  score = Number(score) || 0;
  // Si por error llegara de 0-100 en vez de 0-1, lo normalizamos
  const normalized = score > 1 ? score / 100 : score;
  const pct = Math.min(Math.round(normalized * 100), 100);
  const color = normalized >= 0.9 ? '#22c55e' : normalized >= 0.75 ? '#8b5cf6' : '#f97316';
  return `<div class="score-cell">
    <div class="score-bar-wrap"><div class="score-bar" style="width:${pct}%;background:${color}"></div></div>
    <span class="score-value">${normalized.toFixed(2)}</span>
  </div>`;
}

// ─────────────────────────────────────────────────────────────
// ROUTER
// ─────────────────────────────────────────────────────────────
function navigateTo(page) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  const pg = document.getElementById(`page-${page}`);
  if (pg) pg.classList.add('active');
  const nav = document.getElementById(`nav-${page}`);
  if (nav) nav.classList.add('active');
  document.getElementById('breadcrumb-text').textContent =
    {
      dashboard: 'Dashboard', ambassadors: 'Ambassadors', posts: 'Posts',
      analytics: 'Analytics', revenue: 'Revenue', settings: 'Settings'
    }[page] || page;
  if (page === 'dashboard') renderDashboard();
  if (page === 'ambassadors') renderAmbassadors();
  if (page === 'posts') renderPosts();
  if (page === 'analytics') renderAnalytics();
  if (page === 'revenue') renderRevenue();
  if (page === 'settings') renderSettings();
}

// ── Mobile sidebar drawer ────────────────────────────────────
(function () {
  const hamburger = document.getElementById('hamburger-btn');
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('sidebar-overlay');

  function openSidebar() {
    sidebar.classList.add('open');
    overlay.classList.add('visible');
    document.body.style.overflow = 'hidden';
  }

  function closeSidebar() {
    sidebar.classList.remove('open');
    overlay.classList.remove('visible');
    document.body.style.overflow = '';
  }

  if (hamburger) hamburger.addEventListener('click', () => {
    sidebar.classList.contains('open') ? closeSidebar() : openSidebar();
  });

  if (overlay) overlay.addEventListener('click', closeSidebar);

  // Close sidebar when nav item clicked on mobile
  sidebar.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', () => {
      if (window.innerWidth <= 640) closeSidebar();
    });
  });
})();

document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', e => {
    e.preventDefault();
    if (item.dataset.page) navigateTo(item.dataset.page);
  });
});

document.querySelectorAll('a[data-page]').forEach(link => {
  link.addEventListener('click', e => {
    e.preventDefault();
    if (link.dataset.page) navigateTo(link.dataset.page);
  });
});

// ─────────────────────────────────────────────────────────────
// CHARTS UTILS
// ─────────────────────────────────────────────────────────────
function destroyChart(id) {
  if (chartInstances[id]) { chartInstances[id].destroy(); delete chartInstances[id]; }
}

function baseOpts() {
  return {
    responsive: true,
    maintainAspectRatio: true,
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: '#1f2330', borderColor: 'rgba(255,255,255,0.1)',
        borderWidth: 1, titleColor: '#f0f2f8', bodyColor: '#8b92a8', padding: 10, cornerRadius: 8
      }
    }
  };
}

function xAxis() {
  return { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#545c72', font: { size: 11, family: 'Inter' }, maxTicksLimit: 7 }, border: { display: false } };
}
function yAxis(fmt_type) {
  return { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#545c72', font: { size: 11, family: 'Inter' }, callback: v => fmt(v, fmt_type || 'compact') }, border: { display: false } };
}

// ─────────────────────────────────────────────────────────────
// DASHBOARD
// ─────────────────────────────────────────────────────────────
async function renderDashboard() {
  const qs = {};
  const days = document.getElementById('filter-date')?.value || '30';
  const country = document.getElementById('filter-country')?.value;
  const niche = document.getElementById('filter-niche')?.value;
  const platform = document.getElementById('filter-platform')?.value;

  if (days) qs.days = days;
  if (country) qs.country_code = country;
  if (niche) qs.niche_code = niche;
  if (platform) qs.platform_code = platform;

  const data = await GET('/dashboard', qs).catch(() => null);
  if (!data) return;

  // KPI counters animation
  const kpis = [
    { id: 'kpi-active-ambassadors', val: data.kpis.total_ambassadors, fmt: '' },
    { id: 'kpi-active-profiles', val: data.kpis.total_profiles, fmt: '' },
    { id: 'kpi-signed-contracts', val: data.kpis.signed_contracts, fmt: '' },
    { id: 'kpi-total-views', val: data.kpis.total_views, fmt: 'compact' },
  ];
  kpis.forEach(k => {
    const el = document.querySelector(`#${k.id} .kpi-value`);
    if (!el) return;
    animateCounter(el, k.val, k.fmt);
  });
  // Revenue wide card
  const revEl = document.querySelector('#kpi-revenue .kpi-value');
  if (revEl) animateCounter(revEl, data.kpis.expected_revenue, 'currency');
  const realRevEl = document.querySelector('#kpi-revenue .kpi-subvalue');
  if (realRevEl) realRevEl.textContent = fmt(data.kpis.real_revenue, 'currency');
  const delta = data.kpis.expected_revenue > 0
    ? ((data.kpis.real_revenue - data.kpis.expected_revenue) / data.kpis.expected_revenue * 100).toFixed(1)
    : 0;
  const deltaEl = document.querySelector('#kpi-revenue .kpi-delta');
  if (deltaEl) deltaEl.innerHTML = `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="${delta >= 0 ? '18 15 12 9 6 15' : '6 9 12 15 18 9'}"/></svg> Δ ${delta >= 0 ? '+' : ''}${delta}%`;

  // Charts
  renderViewsTrend(data.views_trend);
  renderPlatformSplit(data.platform_split);
  renderTopTable(data.top_ambassadors);
}

function animateCounter(el, target, format, duration = 900) {
  let start = 0;
  const step = 16;
  const inc = target / (duration / step);
  const timer = setInterval(() => {
    start = Math.min(start + inc, target);
    el.textContent = fmt(Math.round(start), format);
    if (start >= target) clearInterval(timer);
  }, step);
}

function renderViewsTrend(trend) {
  destroyChart('views');
  const ctx = document.getElementById('canvas-views');
  if (!ctx) return;
  // Fill last 30 days with API data or zeros
  const labels = [], values = [];
  const now = new Date();
  for (let i = 29; i >= 0; i--) {
    const d = new Date(now); d.setDate(d.getDate() - i);
    const key = d.toISOString().slice(0, 10);
    labels.push(d.toLocaleDateString('es-ES', { month: 'short', day: 'numeric' }));
    const found = (trend || []).find(t => t.views_date === key);
    values.push(found ? found.views : 0);
  }
  // If all zeros, use dummy data for visual
  const hasData = values.some(v => v > 0);
  const displayValues = hasData ? values : values.map((_, i) => Math.round(100000 + Math.sin(i / 3) * 60000 + Math.random() * 20000));

  chartInstances['views'] = new Chart(ctx, {
    type: 'line', data: {
      labels, datasets: [{
        label: 'Views', data: displayValues,
        borderColor: '#8b5cf6', backgroundColor: 'rgba(139,92,246,0.08)', fill: true,
        tension: 0.4, pointRadius: 0, borderWidth: 2
      }]
    },
    options: { ...baseOpts(), scales: { x: xAxis(), y: yAxis() } }
  });
}

function renderPlatformSplit(split) {
  destroyChart('platforms');
  const ctx = document.getElementById('canvas-platforms');
  if (!ctx) return;
  const labels = (split || []).map(s => s.platform);
  const values = (split || []).map(s => s.count);
  const colors = { YouTube: '#ef4444', Instagram: '#e1306c', TikTok: '#766dff', LinkedIn: '#0077b5', Twitch: '#9147ff' };
  chartInstances['platforms'] = new Chart(ctx, {
    type: 'doughnut', data: {
      labels, datasets: [{
        data: values.length ? values : [1],
        backgroundColor: labels.length ? labels.map(l => colors[l] || '#8b5cf6') : ['#333'],
        borderWidth: 0, hoverOffset: 6
      }]
    },
    options: {
      ...baseOpts(), cutout: '65%',
      plugins: { legend: { position: 'bottom', labels: { color: '#8b92a8', font: { size: 12, family: 'Inter' }, padding: 12 } } }
    }
  });
}

function renderTopTable(ambassadors) {
  const tbody = document.getElementById('top-ambassadors-body');
  if (!tbody) return;
  tbody.innerHTML = (ambassadors || []).map(a => `
    <tr>
      <td><div class="avatar-cell"><div class="table-avatar">${initials(a.name)}</div>${a.name}</div></td>
      <td><span class="badge badge-country">${a.country_code || '—'}</span></td>
      <td>${platformBadge(a.platform_code)}</td>
      <td>${fmt(a.total_views, 'compact')}</td>
      <td>${scoreBar(a.avg_score)}</td>
      <td>${a.contract_status ? statusBadge(a.contract_status) : '<span style="color:var(--text-tertiary)">—</span>'}</td>
    </tr>
  `).join('');
}

// ─────────────────────────────────────────────────────────────
// AMBASSADORS
// ─────────────────────────────────────────────────────────────
async function renderAmbassadors() {
  const qs = buildAmbassadorFilters();
  const ambassadors = await GET('/ambassadors', qs).catch(() => []);
  filteredAmbassadors = ambassadors;
  const tbody = document.getElementById('ambassadors-body');
  tbody.innerHTML = ambassadors.map(a => `
    <tr data-id="${a.id}" class="${selectedAmbassadorId === a.id ? 'selected' : ''}">
      <td><div class="avatar-cell"><div class="table-avatar">${initials(a.first_name + ' ' + (a.last_name || ''))}</div>${a.first_name} ${a.last_name || ''}</div></td>
      <td><span class="badge badge-country">${a.country_code || '—'}</span></td>
      <td><span class="badge badge-lang">${a.language_code || '—'}</span></td>
      <td><strong>${a.profile_count || 0}</strong></td>
      <td>${a.latest_contract_status ? statusBadge(a.latest_contract_status) : '<span style="color:var(--text-tertiary)">—</span>'}</td>
    </tr>
  `).join('');

  tbody.querySelectorAll('tr').forEach(row => {
    row.addEventListener('click', () => {
      tbody.querySelectorAll('tr').forEach(r => r.classList.remove('selected'));
      row.classList.add('selected');
      selectedAmbassadorId = parseInt(row.dataset.id, 10);
      openAmbassadorDetail(selectedAmbassadorId);
    });
  });

  if (selectedAmbassadorId) await openAmbassadorDetail(selectedAmbassadorId);
}

function buildAmbassadorFilters() {
  const qs = {};
  const search = document.getElementById('ambassador-search')?.value.trim();
  const country = document.getElementById('amb-filter-country')?.value || document.getElementById('filter-country')?.value;
  const platform = document.getElementById('amb-filter-platform')?.value || document.getElementById('filter-platform')?.value;
  const status = document.getElementById('amb-filter-status')?.value;
  const niche = document.getElementById('filter-niche')?.value;

  if (search) qs.search = search;
  if (country) qs.country_code = country;
  if (platform) qs.platform_code = platform;
  if (status) qs.status_code = status;
  if (niche) qs.niche_code = niche;

  return Object.keys(qs).length ? qs : null;
}

document.getElementById('ambassador-search').addEventListener('input', debounce(renderAmbassadors, 300));
document.getElementById('amb-filter-country').addEventListener('change', renderAmbassadors);
document.getElementById('amb-filter-status').addEventListener('change', renderAmbassadors);
document.getElementById('amb-filter-platform').addEventListener('change', renderAmbassadors);

function debounce(fn, ms) {
  let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

async function openAmbassadorDetail(id) {
  const a = await GET(`/ambassadors/${id}`).catch(() => null);
  if (!a) return;

  document.getElementById('detail-empty').style.display = 'none';
  document.getElementById('detail-content').style.display = 'block';

  const fullName = `${a.first_name} ${a.last_name || ''}`.trim();
  document.getElementById('detail-avatar').textContent = initials(fullName);
  document.getElementById('detail-name').textContent = fullName;
  document.getElementById('detail-email').textContent = a.email;
  document.getElementById('detail-country-badge').textContent = a.country_code || '—';
  document.getElementById('detail-lang-badge').textContent = a.language_code || '—';

  switchDetailTab('overview');
}

// ── Detail Tabs ─────────────────────────────────────────────
document.querySelectorAll('.tabs .tab[data-tab]').forEach(tab => {
  tab.addEventListener('click', () => switchDetailTab(tab.dataset.tab));
});

async function switchDetailTab(tabId) {
  currentDetailTab = tabId;
  document.querySelectorAll('.tabs .tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => { c.style.display = 'none'; });
  const activeTab = document.querySelector(`.tabs .tab[data-tab="${tabId}"]`);
  if (activeTab) activeTab.classList.add('active');
  const panel = document.getElementById(`tab-${tabId}`);
  if (panel) panel.style.display = 'block';

  if (!selectedAmbassadorId) return;

  if (tabId === 'overview') await renderDetailOverview();
  if (tabId === 'profiles') await renderDetailProfiles();
  if (tabId === 'contracts') await renderDetailContracts();
  if (tabId === 'content') await renderDetailContent();
}

// ── Revenue formula constants (mirrors server.py) ────────────
const COUNTRY_RPM_MULT = {
  MX: 1.00, ES: 1.33, CO: 0.53, CL: 0.53, BR: 0.33, DE: 3.00,
  FR: 1.47, IT: 2.67, PT: 0.72, UK: 3.00, US: 5.00, AU: 3.67,
  CA: 3.67, AR: 0.40, IE: 3.00
};
const LATAM_CODES = new Set(['AR', 'CO', 'CL', 'PE', 'EC', 'VE', 'UY', 'PY', 'BO', 'GT', 'HN', 'SV', 'NI', 'CR', 'PA', 'DO', 'PR']);
const CACHE_MULT = { LOW: 0.8, MID: 1.0, HIGH: 1.2 };

function calcExpectedRevenue(pa, platformCode, monthlyStd, monthlyTop, countryCode) {
  const ev = pa?.expected_views || 0;
  if (ev === 0) return 0;
  const c_code = (countryCode || '').toUpperCase();
  const countryMult = COUNTRY_RPM_MULT[c_code] ?? (LATAM_CODES.has(c_code) ? 0.40 : 0.12);
  const cacheMult = CACHE_MULT[(pa?.cache_score || 'MID').toUpperCase()] ?? 1.0;
  const cts = pa?.content_target_score ?? 1.0;
  const cots = Math.min((pa?.country_target_score ?? 0.6) / 0.6, 1.0);
  const base = (ev / 1000) * 42 * countryMult * cacheMult * cts * cots;
  const pCode = (platformCode || '').toLowerCase();
  if (pCode === 'youtube') return (base * 2.5 * (monthlyStd || 0)) + (base * 4.0 * (monthlyTop || 0));
  if (pCode === 'tiktok') return base * 1.0 * ((monthlyStd || 0) + (monthlyTop || 0));
  return 0; // other platforms: 0 per spec
}

async function renderDetailOverview() {
  const a = await GET(`/ambassadors/${selectedAmbassadorId}`).catch(() => ({}));
  const posts = await GET('/posts', { ambassador_id: selectedAmbassadorId }).catch(() => []);
  const contracts = await GET('/contracts', { ambassador_id: selectedAmbassadorId }).catch(() => []);

  const totalViews = posts.reduce((s, p) => s + (p.total_views || 0), 0);
  const avgScore = posts.length ? posts.reduce((s, p) => s + (p.content_score || 0), 0) / posts.length : 0;

  // ── Expected revenue via official formula ──────────────────
  let expectedRev = 0;
  await Promise.all(contracts.map(async c => {
    const paList = await GET('/profile_analyses', { profile_id: c.profile_id }).catch(() => []);
    const pa = paList[0] || null;
    const profile = await GET(`/profiles/${c.profile_id}`).catch(() => null);
    expectedRev += calcExpectedRevenue(
      pa, profile?.platform_code,
      c.monthly_standard_posts, c.monthly_top_posts, a.country_code
    );
  }));

  document.getElementById('ov-views').textContent = fmt(totalViews, 'compact');
  document.getElementById('ov-posts').textContent = posts.length;
  document.getElementById('ov-score').textContent = avgScore.toFixed(2);
  document.getElementById('ov-revenue').textContent = fmt(expectedRev, 'currency');

  // ── Status badge ──────────────────────────────────────────
  const latestContract = contracts[0];
  const statusCode = latestContract?.status_code || '';
  const statusLabel = latestContract?.status || '—';
  document.getElementById('detail-status-badge').innerHTML = latestContract
    ? statusBadge(statusCode, statusLabel)
    : '<span class="badge badge-inactive">Sin contrato</span>';

  // ── Mini bar chart: real daily views ─────────────────────
  destroyChart('amb-views');
  const ctx = document.getElementById('canvas-ambassador-views');
  if (ctx) {
    const dailyMap = {};
    await Promise.all(posts.map(async p => {
      const views = await GET('/post_views', { post_id: p.id }).catch(() => []);
      views.forEach(v => {
        dailyMap[v.views_date] = (dailyMap[v.views_date] || 0) + (v.new_views || 0);
      });
    }));
    const labels = [], values = [];
    const now = new Date();
    for (let i = 29; i >= 0; i--) {
      const d = new Date(now); d.setDate(d.getDate() - i);
      const key = d.toISOString().slice(0, 10);
      labels.push(d.toLocaleDateString('es-ES', { month: 'short', day: 'numeric' }));
      values.push(dailyMap[key] || 0);
    }
    chartInstances['amb-views'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels, datasets: [{
          label: 'Views', data: values,
          backgroundColor: 'rgba(139,92,246,0.5)', borderRadius: 3, borderSkipped: false
        }]
      },
      options: { ...baseOpts(), scales: { x: { display: false }, y: yAxis() } }
    });
  }
}

async function renderDetailProfiles() {
  const profiles = await GET('/profiles', { ambassador_id: selectedAmbassadorId }).catch(() => []);
  const container = document.getElementById('profiles-list');
  container.innerHTML = profiles.length
    ? profiles.map(p => `
        <div class="profile-item">
          ${platformBadge(p.platform_code, p.platform)}
          <div class="profile-meta">
            <div class="profile-name">${p.handle || p.url}</div>
            <div class="profile-url">${p.url}</div>
            <div class="profile-stats">${fmt(p.total_views || 0, 'compact')} views · ${p.niche || '—'}</div>
          </div>
          <div class="header-actions">
            <button class="btn-icon" onclick="editProfile(${p.id})" title="Editar canal">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
            </button>
            <button class="btn-icon" onclick="deleteProfile(${p.id})" title="Eliminar canal" style="color:var(--danger)">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6M14 11v6"/><path d="M9 6V4h6v2"/></svg>
            </button>
          </div>
        </div>
      `).join('')
    : '<p style="color:var(--text-tertiary);font-size:13px;text-align:center;padding:20px">Sin canales añadidos</p>';
}

window.deleteProfile = async (pid) => {
  if (!confirm('¿Eliminar este canal?')) return;
  try {
    await DELETE(`/profiles/${pid}`);
    await renderDetailProfiles();
    await renderAmbassadors();
  } catch (e) {
    alert('Error al eliminar canal: ' + e.message);
  }
};

window.editProfile = async (pid) => {
  const p = await GET(`/profiles/${pid}`).catch(() => null);
  if (!p) return;
  openModal('Editar canal', `
    <div class="form-row">
      <div class="form-group"><label class="form-label">Plataforma *</label><select id="ep-platform" class="filter-select" style="width:100%;padding-right:28px">${listOptions('platform', '— Plataforma —', p.platform_id)}</select></div>
      <div class="form-group"><label class="form-label">Handle</label><input type="text" id="ep-handle" value="${p.handle || ''}" /></div>
    </div>
    <div class="form-group"><label class="form-label">URL *</label><input type="text" id="ep-url" value="${p.url || ''}" /></div>
    <div class="form-group"><label class="form-label">Nicho</label><select id="ep-niche" class="filter-select" style="width:100%;padding-right:28px">${listOptions('niche', '— Nicho —', p.niche_id)}</select></div>
  `, async () => {
    const platform_id = document.getElementById('ep-platform').value;
    const handle = document.getElementById('ep-handle').value.trim();
    const url = document.getElementById('ep-url').value.trim();
    const niche_id = document.getElementById('ep-niche').value || null;
    if (!url || !platform_id) { alert('Plataforma y URL son obligatorios'); return false; }
    try {
      await PUT(`/profiles/${pid}`, { platform_id, handle, url, niche_id });
      await renderDetailProfiles();
      await renderAmbassadors();
      return true;
    } catch (e) {
      alert('Error al actualizar: ' + e.message);
      return false;
    }
  });
};

async function renderDetailContracts() {
  const contracts = await GET('/contracts', { ambassador_id: selectedAmbassadorId }).catch(() => []);
  const container = document.getElementById('contracts-list');
  container.innerHTML = contracts.length
    ? contracts.map(c => {
      const monthly = (c.price_per_standard_post || 0) * (c.monthly_standard_posts || 0) +
        (c.price_per_top_post || 0) * (c.monthly_top_posts || 0);
      return `<div class="contract-item">
          ${platformBadge(c.platform_code, c.platform)}
          <div class="contract-meta">
            <div class="contract-type">${c.handle || '—'}</div>
            <div class="contract-dates">${c.signing_at ? c.signing_at.slice(0, 10) : '—'} → ${c.end_at ? c.end_at.slice(0, 10) : '—'}</div>
          </div>
          <span class="contract-value">${fmt(monthly * 12, 'currency')}/año</span>
          ${statusBadge(c.status_code, c.status)}
          <div class="header-actions" style="margin-left: 10px;">
            ${c.pdf_url ? `<a href="${c.pdf_url}" download="contrato.pdf" class="btn-icon" title="Descargar PDF"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg></a>` : ''}
            <button class="btn-icon" onclick="editContract(${c.id})" title="Editar">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
            </button>
            <button class="btn-icon" onclick="deleteContract(${c.id})" title="Eliminar" style="color:var(--danger)">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6M14 11v6"/><path d="M9 6V4h6v2"/></svg>
            </button>
          </div>
        </div>`;
    }).join('')
    : '<p style="color:var(--text-tertiary);font-size:13px;text-align:center;padding:20px">Sin contratos</p>';
}

window.deleteContract = async (cid) => {
  if (!confirm('¿Eliminar este contrato?')) return;
  try {
    await DELETE(`/contracts/${cid}`);
    await renderDetailContracts();
  } catch (e) {
    alert('Error al eliminar contrato: ' + e.message);
  }
};

window.editContract = async (cid) => {
  const c = await GET(`/contracts/${cid}`).catch(() => null);
  if (!c) return;

  const updateDatesVisibility = () => {
    const sel = document.getElementById('ec-status');
    const txt = sel.options[sel.selectedIndex]?.text?.toLowerCase() || '';
    const dateRow = document.getElementById('ec-dates-row');
    if (txt.includes('firmado') || txt.includes('signed') || txt.includes('activo')) {
      dateRow.style.display = 'flex';
    } else {
      dateRow.style.display = 'none';
      document.getElementById('ec-sign').value = '';
      document.getElementById('ec-end').value = '';
    }
  };
  window.updateDatesVisibility = updateDatesVisibility;

  openModal('Editar contrato', `
    <div class="form-group"><label class="form-label" style="color:var(--text-secondary)">Canal: ${c.handle || c.platform}</label></div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Estado</label><select id="ec-status" class="filter-select" style="width:100%" onchange="updateDatesVisibility()">${listOptions('contract_status', '', c.status_id)}</select></div>
      <div class="form-group"><label class="form-label">Moneda</label><select id="ec-currency" class="filter-select" style="width:100%">${listOptions('currency', '', c.currency_id)}</select></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">€ / post estándar</label><input type="number" id="ec-pstd" value="${c.price_per_standard_post || ''}" min="0" step="0.01" /></div>
      <div class="form-group"><label class="form-label">Posts estándar/mes</label><input type="number" id="ec-mstd" value="${c.monthly_standard_posts || ''}" min="0" /></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">€ / post top</label><input type="number" id="ec-ptop" value="${c.price_per_top_post || ''}" min="0" step="0.01" /></div>
      <div class="form-group"><label class="form-label">Posts top/mes</label><input type="number" id="ec-mtop" value="${c.monthly_top_posts || ''}" min="0" /></div>
    </div>
    <div class="form-row" id="ec-dates-row" style="display:none">
      <div class="form-group"><label class="form-label">Firma</label><input type="date" id="ec-sign" value="${c.signing_at ? c.signing_at.slice(0, 10) : ''}" /></div>
      <div class="form-group"><label class="form-label">Fin</label><input type="date" id="ec-end" value="${c.end_at ? c.end_at.slice(0, 10) : ''}" /></div>
    </div>
    <div class="form-group">
      <label class="form-label">Subir Contrato (.pdf)</label>
      <input type="file" id="ec-pdf" accept=".pdf" class="filter-select" style="width:100%; padding: 4px;" />
      ${c.pdf_url ? '<div style="margin-top:4px;font-size:11px"><a href="' + c.pdf_url + '" download="contrato.pdf" style="color:var(--accent-purple)">Ver PDF actual</a></div>' : ''}
    </div>
  `, async () => {
    const status_id = parseInt(document.getElementById('ec-status').value);
    const currency_id = parseInt(document.getElementById('ec-currency').value) || null;
    const price_per_standard_post = parseFloat(document.getElementById('ec-pstd').value) || 0;
    const monthly_standard_posts = parseInt(document.getElementById('ec-mstd').value) || 0;
    const price_per_top_post = parseFloat(document.getElementById('ec-ptop').value) || 0;
    const monthly_top_posts = parseInt(document.getElementById('ec-mtop').value) || 0;
    const signing_at = document.getElementById('ec-sign').value || null;
    const end_at = document.getElementById('ec-end').value || null;
    if (!status_id) { alert('Estado es obligatorio'); return false; }
    
    let pdf_url = c.pdf_url;
    const pdfFile = document.getElementById('ec-pdf').files[0];
    if (pdfFile) {
       const reader = new FileReader();
       pdf_url = await new Promise(res => { reader.onload = () => res(reader.result); reader.readAsDataURL(pdfFile); });
    }

    try {
      await PUT(`/contracts/${cid}`, {
        status_id, currency_id,
        price_per_standard_post, monthly_standard_posts,
        price_per_top_post, monthly_top_posts, signing_at, end_at, pdf_url
      });
      await renderDetailContracts();
      return true;
    } catch (e) {
      alert('Error al guardar: ' + e.message);
      return false;
    }
  });
  updateDatesVisibility();
};

async function renderDetailContent() {
  const posts = await GET('/posts', { ambassador_id: selectedAmbassadorId }).catch(() => []);
  const container = document.getElementById('content-list');
  const postsHtml = posts.length
    ? posts.map(p => `
        <div class="content-item">
          ${platformBadge(p.platform_code, p.platform)}
          <div class="profile-meta">
            <div class="profile-name">${p.mention_type || p.mention_type_code || '—'}</div>
            <div class="profile-url">${p.published_at ? p.published_at.slice(0, 10) : '—'}</div>
            <div class="profile-stats">${fmt(p.total_views || 0, 'compact')} views · Score: ${Number(p.content_score || 0).toFixed(2)}</div>
          </div>
          <div class="header-actions" style="margin-left: auto;">
            <button class="btn-icon" onclick="editPost(${p.id})" title="Editar">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
            </button>
            <button class="btn-icon" onclick="deletePost(${p.id})" title="Eliminar" style="color:var(--danger)">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6M14 11v6"/><path d="M9 6V4h6v2"/></svg>
            </button>
          </div>
        </div>
      `).join('')
    : '<p style="color:var(--text-tertiary);font-size:13px;text-align:center;padding:20px">Sin posts registrados</p>';

  container.innerHTML = postsHtml +
    `<button class="btn-secondary btn-block" id="btn-add-content-detail" style="margin-top:12px">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
      Registrar post
    </button>`;
}

window.deletePost = async (pid) => {
  if (!confirm('¿Eliminar este post?')) return;
  try {
    await DELETE(`/posts/${pid}`);
    await renderDetailContent();
  } catch (e) {
    alert('Error al eliminar post: ' + e.message);
  }
};

window.editPost = async (pid) => {
  const p = await GET(`/posts/${pid}`).catch(() => null);
  if (!p) return;

  const profiles = await GET('/profiles', { ambassador_id: selectedAmbassadorId });

  openModal('Editar post', `
    <div class="form-group"><label class="form-label">Canal *</label>
      <select id="ep-profile" class="filter-select" style="width:100%;padding-right:28px">
        ${profiles.map(pr => `<option value="${pr.id}" ${pr.id === p.profile_id ? 'selected' : ''}>${pr.handle || pr.url} (${pr.platform})</option>`).join('')}
      </select>
    </div>
    <div class="form-group"><label class="form-label">URL del post *</label><input type="text" id="ep-url" value="${p.url || ''}" /></div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Tipo mención</label><select id="ep-mention" class="filter-select" style="width:100%;padding-right:28px">${listOptions('mention_type', '', p.mention_type_id)}</select></div>
      <div class="form-group"><label class="form-label">Fecha publicación</label><input type="date" id="ep-date" value="${p.published_at ? p.published_at.slice(0, 10) : ''}" /></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Content Score (0-10)</label><input type="number" id="ep-score" value="${p.content_score || 0}" min="0" max="10" step="0.1" /></div>
      <div class="form-group"><label class="form-label">Offset mención (seg.)</label><input type="number" id="ep-offset" value="${p.mention_offset || 0}" min="0" /></div>
    </div>
  `, async () => {
    const profile_id = parseInt(document.getElementById('ep-profile').value);
    const url = document.getElementById('ep-url').value.trim();
    const mention_type_id = parseInt(document.getElementById('ep-mention').value) || null;
    const published_at = document.getElementById('ep-date').value || null;
    const content_score = parseFloat(document.getElementById('ep-score').value) || null;
    const mention_offset = parseInt(document.getElementById('ep-offset').value) || 0;
    
    if (!url) { alert('URL es obligatoria'); return false; }
    try {
      await PUT(`/posts/${pid}`, { profile_id, url, mention_type_id, published_at, content_score, mention_offset });
      await renderDetailContent();
      return true;
    } catch (e) {
      alert('Error al actualizar: ' + e.message);
      return false;
    }
  });
};

// ── Delegación en tab-content ──────────────────────────────
document.getElementById('tab-content').addEventListener('click', e => {
  if (e.target.closest('#btn-add-content-detail')) openNewPostModal();
});

// ─────────────────────────────────────────────────────────────
// AMBASSADOR — ADD BUTTONS
// ─────────────────────────────────────────────────────────────
document.getElementById('btn-new-ambassador').addEventListener('click', () => {
  openModal('Nuevo embajador', `
    <div class="form-row">
      <div class="form-group"><label class="form-label">Nombre *</label><input type="text" id="nf-name" placeholder="Nombre" /></div>
      <div class="form-group"><label class="form-label">Apellido</label><input type="text" id="nf-last" placeholder="Apellido" /></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Email *</label><input type="email" id="nf-email" placeholder="email@ejemplo.com" /></div>
    </div>

    <div class="form-row">
      <div class="form-group"><label class="form-label">País</label><select id="nf-country" class="filter-select" style="width:100%;padding-right:28px">${listOptions('country', '— País —')}</select></div>
      <div class="form-group"><label class="form-label">Idioma</label><select id="nf-lang" class="filter-select" style="width:100%;padding-right:28px">${listOptions('language', '— Idioma —')}</select></div>
    </div>
  `, async () => {
    const first_name = document.getElementById('nf-name').value.trim();
    const last_name = document.getElementById('nf-last').value.trim();
    const email = document.getElementById('nf-email').value.trim();

    const country_id = document.getElementById('nf-country').value || null;
    const lang_id = document.getElementById('nf-lang').value || null;
    if (!first_name || !email) { alert('Nombre y email son obligatorios'); return false; }
    try {
      const res = await POST('/ambassadors', { email, first_name, last_name, country_id, primary_language_id: lang_id });
      if (res && res.id) selectedAmbassadorId = res.id;
      await renderAmbassadors();
      return true;
    } catch (e) {
      alert('Error al guardar: ' + e.message);
      return false;
    }
  });
});

document.getElementById('btn-edit-ambassador').addEventListener('click', async () => {
  if (!selectedAmbassadorId) return;
  const a = await GET(`/ambassadors/${selectedAmbassadorId}`).catch(() => null);
  if (!a) return;

  openModal('Editar embajador', `
    <div class="form-row">
      <div class="form-group"><label class="form-label">Nombre *</label><input type="text" id="ef-name" value="${a.first_name}" /></div>
      <div class="form-group"><label class="form-label">Apellido</label><input type="text" id="ef-last" value="${a.last_name || ''}" /></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Email *</label><input type="email" id="ef-email" value="${a.email}" /></div>
    </div>

    <div class="form-row">
      <div class="form-group"><label class="form-label">País</label><select id="ef-country" class="filter-select" style="width:100%">${listOptions('country', '— País —', a.country_id)}</select></div>
      <div class="form-group"><label class="form-label">Idioma</label><select id="ef-lang" class="filter-select" style="width:100%">${listOptions('language', '— Idioma —', a.primary_language_id)}</select></div>
    </div>
  `, async () => {
    const first_name = document.getElementById('ef-name').value.trim();
    const last_name = document.getElementById('ef-last').value.trim();
    const email = document.getElementById('ef-email').value.trim();

    const country_id = document.getElementById('ef-country').value || null;
    const lang_id = document.getElementById('ef-lang').value || null;

    if (!first_name || !email) { alert('Nombre y email son obligatorios'); return false; }
    try {
      await PUT(`/ambassadors/${selectedAmbassadorId}`, { email, first_name, last_name, country_id, primary_language_id: lang_id });
      await renderAmbassadors();
      return true;
    } catch (e) {
      alert('Error al guardar: ' + e.message);
      return false;
    }
  });
});

document.getElementById('btn-delete-ambassador').addEventListener('click', async () => {
  if (!selectedAmbassadorId) return;
  if (!confirm('¿Seguro que quieres eliminar este embajador? Se borrarán todos sus canales y contratos.')) return;
  try {
    await DELETE(`/ambassadors/${selectedAmbassadorId}`);
    selectedAmbassadorId = null;
    document.getElementById('detail-empty').style.display = 'flex';
    document.getElementById('detail-content').style.display = 'none';
    renderAmbassadors();
  } catch (e) {
    alert('Error al eliminar embajador: ' + e.message);
  }
});

document.getElementById('btn-add-profile').addEventListener('click', () => {
  if (!selectedAmbassadorId) return;
  openModal('Añadir canal', `
    <div class="form-row">
      <div class="form-group"><label class="form-label">Plataforma *</label><select id="ap-platform" class="filter-select" style="width:100%;padding-right:28px">${listOptions('platform', '— Plataforma —')}</select></div>
      <div class="form-group"><label class="form-label">Handle</label><input type="text" id="ap-handle" placeholder="@usuario" /></div>
    </div>
    <div class="form-group"><label class="form-label">URL *</label><input type="text" id="ap-url" placeholder="https://..." /></div>
    <div class="form-group"><label class="form-label">Nicho</label><select id="ap-niche" class="filter-select" style="width:100%;padding-right:28px">${listOptions('niche', '— Nicho —')}</select></div>
  `, async () => {
    const platform_id = document.getElementById('ap-platform').value;
    const handle = document.getElementById('ap-handle').value.trim();
    const url = document.getElementById('ap-url').value.trim();
    const niche_id = document.getElementById('ap-niche').value || null;
    if (!url || !platform_id) { alert('Plataforma y URL son obligatorios'); return false; }
    try {
      await POST('/profiles', { ambassador_id: selectedAmbassadorId, platform_id, handle, url, niche_id });
      await renderDetailProfiles();
      await renderAmbassadors();
      return true;
    } catch (e) {
      alert('Error al guardar: ' + e.message);
      return false;
    }
  });
});

document.getElementById('btn-add-contract').addEventListener('click', async () => {
  if (!selectedAmbassadorId) return;
  const profiles = await GET('/profiles', { ambassador_id: selectedAmbassadorId }).catch(() => []);
  if (profiles.length === 0) { alert('Añade un canal primero'); return; }

  const profilesHtml = profiles.map(p => `
    <div class="contract-profile-block" style="border:1px solid rgba(255,255,255,0.1); padding:10px; border-radius:8px; margin-bottom:12px;">
      <h4 style="margin-top:0;margin-bottom:10px;font-size:13px;color:var(--text-primary)">
        <input type="checkbox" class="ac-profile-check" value="${p.id}" checked />
        Incluir ${p.handle || p.url} (${p.platform})
      </h4>
      <div class="form-row">
        <div class="form-group"><label class="form-label">€ / post estándar</label><input type="number" class="ac-pstd" data-pid="${p.id}" placeholder="0.00" min="0" step="0.01" /></div>
        <div class="form-group"><label class="form-label">Posts estándar/mes</label><input type="number" class="ac-mstd" data-pid="${p.id}" placeholder="0" min="0" /></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label class="form-label">€ / post top</label><input type="number" class="ac-ptop" data-pid="${p.id}" placeholder="0.00" min="0" step="0.01" /></div>
        <div class="form-group"><label class="form-label">Posts top/mes</label><input type="number" class="ac-mtop" data-pid="${p.id}" placeholder="0" min="0" /></div>
      </div>
    </div>
  `).join('');

  const updateDatesVisibility = () => {
    const sel = document.getElementById('ac-status');
    const txt = sel.options[sel.selectedIndex]?.text?.toLowerCase() || '';
    const dateRow = document.getElementById('ac-dates-row');
    if (txt.includes('firmado') || txt.includes('signed') || txt.includes('activo')) {
      dateRow.style.display = 'flex';
    } else {
      dateRow.style.display = 'none';
      document.getElementById('ac-sign').value = '';
      document.getElementById('ac-end').value = '';
    }
  };
  window.updateDatesVisibility = updateDatesVisibility;

  openModal('Nuevo contrato', `
    <div class="form-row">
      <div class="form-group"><label class="form-label">Estado común</label><select id="ac-status" class="filter-select" style="width:100%" onchange="updateDatesVisibility()">${listOptions('contract_status')}</select></div>
      <div class="form-group"><label class="form-label">Moneda</label><select id="ac-currency" class="filter-select" style="width:100%">${listOptions('currency')}</select></div>
    </div>
    <div class="form-row" id="ac-dates-row" style="display:none">
      <div class="form-group"><label class="form-label">Firma</label><input type="date" id="ac-sign" value="${new Date().toISOString().slice(0, 10)}" /></div>
      <div class="form-group"><label class="form-label">Fin</label><input type="date" id="ac-end" /></div>
    </div>
    <div class="form-group">
      <label class="form-label">Subir Contrato (.pdf)</label>
      <input type="file" id="ac-pdf" accept=".pdf" class="filter-select" style="width:100%; padding: 4px;" />
    </div>
    <hr style="border:0;border-top:1px solid rgba(255,255,255,0.1);margin:15px 0" />
    <h3 style="font-size:14px;margin-bottom:10px;">Canales incluidos</h3>
    ${profilesHtml}
  `, async () => {
    const status_id = parseInt(document.getElementById('ac-status').value);
    const currency_id = parseInt(document.getElementById('ac-currency').value) || null;
    const signing_at = document.getElementById('ac-sign').value || null;
    const end_at = document.getElementById('ac-end').value || null;
    if (!status_id) { alert('Estado es obligatorio'); return false; }
    
    const pdfFile = document.getElementById('ac-pdf').files[0];
    let pdf_url = null;
    if (pdfFile) {
       const reader = new FileReader();
       pdf_url = await new Promise(res => { reader.onload = () => res(reader.result); reader.readAsDataURL(pdfFile); });
    }

    const checks = Array.from(document.querySelectorAll('.ac-profile-check:checked'));
    if (checks.length === 0) { alert('Selecciona al menos un canal'); return false; }

    try {
      for (const chk of checks) {
        const pid = chk.value;
        const price_per_standard_post = parseFloat(document.querySelector(`.ac-pstd[data-pid="${pid}"]`).value) || 0;
        const monthly_standard_posts = parseInt(document.querySelector(`.ac-mstd[data-pid="${pid}"]`).value) || 0;
        const price_per_top_post = parseFloat(document.querySelector(`.ac-ptop[data-pid="${pid}"]`).value) || 0;
        const monthly_top_posts = parseInt(document.querySelector(`.ac-mtop[data-pid="${pid}"]`).value) || 0;
        
        await POST('/contracts', {
          profile_id: pid, status_id, currency_id,
          price_per_standard_post, monthly_standard_posts,
          price_per_top_post, monthly_top_posts, signing_at, end_at, pdf_url
        });
      }
      await renderDetailContracts();
      return true;
    } catch (e) {
      alert('Error al guardar: ' + e.message);
      return false;
    }
  });
  updateDatesVisibility();
});

function openNewPostModal() {
  if (!selectedAmbassadorId) return;
  GET('/profiles', { ambassador_id: selectedAmbassadorId }).then(profiles => {
    const profileOptions = profiles.map(p => `<option value="${p.id}" data-views="${p.expected_views || 0}">${p.handle || p.url} (${p.platform})</option>`).join('');
    
    window.updateExpectedViews = function(selId, inpId) {
      const sel = document.getElementById(selId);
      const opt = sel.options[sel.selectedIndex];
      if (opt && opt.dataset.views) document.getElementById(inpId).value = opt.dataset.views;
    };

    openModal('Registrar post', `
      <div class="form-group"><label class="form-label">Canal *</label>
        <select id="nc-profile" class="filter-select" style="width:100%;padding-right:28px" onchange="updateExpectedViews('nc-profile', 'nc-views')">${profileOptions || '<option value="">— Sin canales —</option>'}</select>
      </div>
      <div class="form-group"><label class="form-label">URL del post *</label><input type="text" id="nc-url" placeholder="https://..." /></div>
      <div class="form-row">
        <div class="form-group"><label class="form-label">Tipo mención</label><select id="nc-mention" class="filter-select" style="width:100%;padding-right:28px">${listOptions('mention_type')}</select></div>
        <div class="form-group"><label class="form-label">Fecha publicación</label><input type="date" id="nc-date" value="${new Date().toISOString().slice(0, 10)}" /></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label class="form-label">Views iniciales</label><input type="number" id="nc-views" placeholder="0" min="0" /></div>
        <div class="form-group"><label class="form-label">Content Score (0-10)</label><input type="number" id="nc-score" placeholder="8.5" min="0" max="10" step="0.1" /></div>
      </div>
      <div class="form-group"><label class="form-label">Offset mención (seg.)</label><input type="number" id="nc-offset" placeholder="0" min="0" /></div>
    `, async () => {
      const profile_id = parseInt(document.getElementById('nc-profile').value);
      const url = document.getElementById('nc-url').value.trim();
      const mention_type_id = parseInt(document.getElementById('nc-mention').value) || null;
      const published_at = document.getElementById('nc-date').value || null;
      const views = parseInt(document.getElementById('nc-views').value) || 0;
      const content_score = parseFloat(document.getElementById('nc-score').value) || null;
      const mention_offset = parseInt(document.getElementById('nc-offset').value) || 0;
      if (!url) { alert('URL es obligatoria'); return false; }
      try {
        const post = await POST('/posts', { profile_id, url, mention_type_id, published_at, content_score, mention_offset });
        if (views > 0) {
          await POST('/post_views', { post_id: post.id, views_date: published_at || new Date().toISOString().slice(0, 10), new_views: views });
        }
        await renderDetailContent();
        return true;
      } catch (e) {
        alert('Error al guardar: ' + e.message);
        return false;
      }
    });
    setTimeout(() => window.updateExpectedViews('nc-profile', 'nc-views'), 100);
  });
}

// ─────────────────────────────────────────────────────────────
// POSTS PAGE
// ─────────────────────────────────────────────────────────────
async function renderPosts() {
  const qs = {};
  const search = document.getElementById('posts-search')?.value.trim();
  const platform = document.getElementById('posts-filter-platform')?.value || document.getElementById('filter-platform')?.value;
  const mention = document.getElementById('posts-filter-mention')?.value;
  const country = document.getElementById('filter-country')?.value;
  const niche = document.getElementById('filter-niche')?.value;
  
  if (platform) qs.platform_code = platform;
  if (mention) qs.mention_type_code = mention;
  if (country) qs.country_code = country;
  if (niche) qs.niche_code = niche;

  const posts = await GET('/posts', Object.keys(qs).length ? qs : null).catch(() => []);
  const filtered = search
    ? posts.filter(p => p.ambassador_name?.toLowerCase().includes(search.toLowerCase()) || p.url?.includes(search))
    : posts;

  const tbody = document.getElementById('posts-body');
  tbody.innerHTML = filtered.map(p => `
    <tr>
      <td><div class="avatar-cell"><div class="table-avatar">${initials(p.ambassador_name || '?')}</div>${p.ambassador_name || '—'}</div></td>
      <td>${platformBadge(p.platform_code, p.platform)}</td>
      <td>${p.published_at ? p.published_at.slice(0, 10) : '—'}</td>
      <td><span class="badge badge-country">${p.mention_type || p.mention_type_code || '—'}</span></td>
      <td><strong>${fmt(p.total_views || 0, 'compact')}</strong></td>
      <td>${scoreBar(p.content_score)}</td>
      <td><a href="${p.url}" target="_blank" class="btn-link" style="font-size:12px">Ver ↗</a></td>
    </tr>
  `).join('');
  document.getElementById('posts-count').textContent = `${filtered.length} posts`;
}

document.getElementById('posts-search').addEventListener('input', debounce(renderPosts, 300));
document.getElementById('posts-filter-platform').addEventListener('change', renderPosts);
document.getElementById('posts-filter-mention').addEventListener('change', renderPosts);

document.getElementById('btn-new-post').addEventListener('click', () => {
  GET('/profiles').then(profiles => {
    window.updateExpectedViews = function(selId, inpId) {
      const sel = document.getElementById(selId);
      const opt = sel.options[sel.selectedIndex];
      if (opt && opt.dataset.views) document.getElementById(inpId).value = opt.dataset.views;
    };

    openModal('Registrar post', `
      <div class="form-group"><label class="form-label">Canal *</label>
        <select id="np-profile" class="filter-select" style="width:100%;padding-right:28px" onchange="updateExpectedViews('np-profile', 'np-views')">
          ${profiles.map(p => `<option value="${p.id}" data-views="${p.expected_views || 0}">${p.ambassador_name} — ${p.handle || p.url} (${p.platform})</option>`).join('')}
        </select>
      </div>
      <div class="form-group"><label class="form-label">URL del post *</label><input type="text" id="np-url" placeholder="https://..." /></div>
      <div class="form-row">
        <div class="form-group"><label class="form-label">Tipo mención</label><select id="np-mention" class="filter-select" style="width:100%;padding-right:28px">${listOptions('mention_type')}</select></div>
        <div class="form-group"><label class="form-label">Fecha publicación</label><input type="date" id="np-date" value="${new Date().toISOString().slice(0, 10)}" /></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label class="form-label">Views iniciales</label><input type="number" id="np-views" placeholder="0" min="0" /></div>
        <div class="form-group"><label class="form-label">Content Score (0-10)</label><input type="number" id="np-score" placeholder="8.5" min="0" max="10" step="0.1" /></div>
      </div>
    `, async () => {
      const profile_id = parseInt(document.getElementById('np-profile').value);
      const url = document.getElementById('np-url').value.trim();
      const mention_type_id = parseInt(document.getElementById('np-mention').value) || null;
      const published_at = document.getElementById('np-date').value || null;
      const views = parseInt(document.getElementById('np-views').value) || 0;
      const content_score = parseFloat(document.getElementById('np-score').value) || null;
      if (!url || !profile_id) { alert('Canal y URL son obligatorios'); return false; }
      try {
        const post = await POST('/posts', { profile_id, url, mention_type_id, published_at, content_score, mention_offset: 0 });
        if (views > 0) {
          await POST('/post_views', { post_id: post.id, views_date: published_at || new Date().toISOString().slice(0, 10), new_views: views });
        }
        await renderPosts();
        return true;
      } catch (e) {
        alert('Error al guardar: ' + e.message);
        return false;
      }
    });
    setTimeout(() => window.updateExpectedViews('np-profile', 'np-views'), 100);
  });
});

// ─────────────────────────────────────────────────────────────
// ANALYTICS
// ─────────────────────────────────────────────────────────────
async function renderAnalytics() {
  const groupBy = document.querySelector('#group-by .pill.active')?.dataset.groupby || 'ambassador';
  const metricX = document.getElementById('metric-x').value;
  const metricY = document.getElementById('metric-y').value;

  // Filtros globales
  const qs = {};
  const country = document.getElementById('filter-country')?.value;
  const niche = document.getElementById('filter-niche')?.value;
  const platform = document.getElementById('filter-platform')?.value;

  if (country) qs.country_code = country;
  if (niche) qs.niche_code = niche;
  if (platform) qs.platform_code = platform;

  const [ambassadors, profiles, posts, contracts] = await Promise.all([
    GET('/ambassadors', qs),
    GET('/profiles', qs),
    GET('/posts', qs),
    GET('/contracts', qs)
  ]).catch(() => [[], [], [], []]);

  const groups = buildAnalyticsGroups(ambassadors, profiles, posts, contracts, groupBy);
  renderScatterChart(groups, metricX, metricY);
  renderCumulativeChart(groups);
  renderAnalyticsTable(groups);
}

function buildAnalyticsGroups(ambassadors, profiles, posts, contracts, groupBy) {
  const map = {};
  console.log('Building analytics groups by:', groupBy);

  if (['niche', 'platform', 'profile'].includes(groupBy)) {
    // Agrupar por canales individuales para mayor precisión en nichos/plataformas
    profiles.forEach(p => {
      const amb = ambassadors.find(a => a.id == p.ambassador_id);
      if (!amb) return;

      const profPosts = posts.filter(po => po.profile_id == p.id);
      const profContracts = contracts.filter(c => c.profile_id == p.id);
      const revenue = profContracts.reduce((s, c) =>
        s + ((c.price_per_standard_post || 0) * (c.monthly_standard_posts || 0) +
          (c.price_per_top_post || 0) * (c.monthly_top_posts || 0)) * 12, 0);

      let key;
      if (groupBy === 'niche') key = p.niche || p.niche_code || '—';
      else if (groupBy === 'platform') key = p.platform || p.platform_code || '—';
      else key = p.handle || p.url || '—';

      if (!map[key]) map[key] = { label: key, views: 0, posts: 0, scoreSum: 0, scoreCount: 0, revenue: 0 };
      map[key].views += profPosts.reduce((s, po) => s + (po.total_views || 0), 0);
      map[key].posts += profPosts.length;
      map[key].scoreSum += profPosts.reduce((s, po) => s + (po.content_score || 0), 0);
      map[key].scoreCount += profPosts.length;
      map[key].revenue += revenue;
    });
  } else {
    // Agrupar por entidad ambassadors (Embajador o País)
    ambassadors.forEach(a => {
      const ambPosts = posts.filter(p => p.ambassador_id == a.id);
      const ambContracts = contracts.filter(c => c.ambassador_id == a.id);
      const ambProfiles = profiles.filter(p => p.ambassador_id == a.id);
      const revenue = ambContracts.reduce((s, c) =>
        s + ((c.price_per_standard_post || 0) * (c.monthly_standard_posts || 0) +
          (c.price_per_top_post || 0) * (c.monthly_top_posts || 0)) * 12, 0);

      let key;
      if (groupBy === 'country') {
        key = a.country_code || '—';
      } else {
        key = a.first_name + ' ' + (a.last_name || '');
      }

      if (!map[key]) map[key] = { label: key, views: 0, posts: 0, scoreSum: 0, scoreCount: 0, revenue: 0 };
      map[key].views += ambPosts.reduce((s, p) => s + (p.total_views || 0), 0);
      map[key].posts += ambPosts.length;
      map[key].scoreSum += ambPosts.reduce((s, p) => s + (p.content_score || 0), 0);
      map[key].scoreCount += ambPosts.length;
      map[key].revenue += revenue;
    });
  }
  return Object.values(map).map(g => ({
    ...g, avgScore: g.scoreCount ? g.scoreSum / g.scoreCount : 0
  })).sort((a, b) => b.views - a.views);
}

function getMetricVal(g, metric) {
  return { views: g.views, posts: g.posts, content_score: g.avgScore, revenue: g.revenue }[metric] || 0;
}

function renderScatterChart(groups, metricX, metricY) {
  destroyChart('scatter');
  const ctx = document.getElementById('canvas-scatter');
  if (!ctx) return;
  const colors = ['#8b5cf6', '#3b82f6', '#14b8a6', '#22c55e', '#f97316', '#ef4444', '#e1306c', '#766dff'];
  chartInstances['scatter'] = new Chart(ctx, {
    type: 'scatter',
    data: {
      datasets: groups.map((g, i) => ({
        label: g.label,
        data: [{ x: getMetricVal(g, metricX), y: getMetricVal(g, metricY) }],
        backgroundColor: colors[i % colors.length] + 'cc',
        borderColor: colors[i % colors.length],
        pointRadius: 10, pointHoverRadius: 13
      }))
    },
    options: {
      ...baseOpts(),
      plugins: {
        ...baseOpts().plugins,
        legend: { display: true, position: 'right', labels: { color: '#8b92a8', font: { size: 11, family: 'Inter' }, padding: 8, boxWidth: 10 } }
      },
      scales: { x: xAxis(), y: yAxis() }
    }
  });
}

function renderCumulativeChart(groups) {
  destroyChart('cumulative');
  const ctx = document.getElementById('canvas-cumulative');
  if (!ctx) return;
  const colors = ['#8b5cf6', '#3b82f6', '#14b8a6', '#22c55e'];
  const labels = Array.from({ length: 30 }, (_, i) => {
    const d = new Date(); d.setDate(d.getDate() - (29 - i));
    return d.toLocaleDateString('es-ES', { month: 'short', day: 'numeric' });
  });
  chartInstances['cumulative'] = new Chart(ctx, {
    type: 'line',
    data: {
      labels, datasets: groups.slice(0, 4).map((g, i) => {
        const base = g.views / 30;
        const data = []; let cum = 0;
        for (let j = 0; j < 30; j++) { cum += Math.max(0, base + (Math.random() - 0.4) * base); data.push(Math.round(cum)); }
        return { label: g.label, data, borderColor: colors[i], fill: false, tension: 0.4, pointRadius: 0, borderWidth: 2 };
      })
    },
    options: {
      ...baseOpts(),
      plugins: { ...baseOpts().plugins, legend: { display: true, position: 'top', labels: { color: '#8b92a8', font: { size: 11, family: 'Inter' }, padding: 12 } } },
      scales: { x: xAxis(), y: yAxis() }
    }
  });
}

function renderAnalyticsTable(groups) {
  document.getElementById('analytics-body').innerHTML = groups.map((g, i) => `
    <tr>
      <td style="color:var(--text-tertiary);font-weight:600">${i + 1}</td>
      <td><strong>${g.label}</strong></td>
      <td>${fmt(g.views, 'compact')}</td>
      <td>${g.posts}</td>
      <td>${scoreBar(g.avgScore)}</td>
      <td>${fmt(g.revenue, 'currency')}</td>
    </tr>
  `).join('');
}

document.querySelectorAll('#group-by .pill').forEach(p => {
  p.addEventListener('click', () => {
    document.querySelectorAll('#group-by .pill').forEach(x => x.classList.remove('active'));
    p.classList.add('active');
    renderAnalytics();
  });
});
document.getElementById('metric-x').addEventListener('change', renderAnalytics);
document.getElementById('metric-y').addEventListener('change', renderAnalytics);

// ─────────────────────────────────────────────────────────────
// REVENUE
// ─────────────────────────────────────────────────────────────
async function renderRevenue() {
  const [revenues, rpus] = await Promise.all([GET('/revenues'), GET('/rpus')]).catch(() => [[], []]);
  renderRevenueTable(revenues);
  renderRpuTable(rpus);
}

function renderRevenueTable(rows) {
  document.getElementById('revenue-body').innerHTML = (rows || []).map(r => `
    <tr>
      <td>${r.views_date}</td>
      <td><span class="badge badge-country">${r.country_code || r.country || '—'}</span></td>
      <td><span class="badge badge-lang" style="background:var(--accent-purple-alpha)">${r.niche_code || r.niche || '—'}</span></td>
      <td><strong style="color:var(--accent-teal)">${fmt(r.amount, 'currency')}</strong></td>
      <td><button class="btn-icon" style="width:auto;padding:4px 10px;font-size:11px" onclick="deleteRevenue(${r.id})">Eliminar</button></td>
    </tr>
  `).join('');
}

function renderRpuTable(rows) {
  document.getElementById('rpu-body').innerHTML = (rows || []).map(r => `
    <tr>
      <td>${r.views_date}</td>
      <td><span class="badge badge-country">${r.country_code || '—'}</span></td>
      <td><span class="badge badge-lang">${r.niche_code || '—'}</span></td>
      <td><strong style="color:var(--accent-purple)">€${Number(r.rpu || 0).toFixed(4)}</strong></td>
      <td><button class="btn-icon" style="width:auto;padding:4px 10px;font-size:11px" onclick="deleteRpu(${r.id})">Eliminar</button></td>
    </tr>
  `).join('');
}

window.deleteRevenue = async (id) => { await DELETE(`/revenues/${id}`); renderRevenue(); };
window.deleteRpu = async (id) => { await DELETE(`/rpus/${id}`); renderRevenue(); };

document.querySelectorAll('.main-tabs .tab[data-maintab]').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.main-tabs .tab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    document.querySelectorAll('.main-tab-content').forEach(c => c.style.display = 'none');
    document.getElementById(`maintab-${tab.dataset.maintab}`).style.display = 'block';
  });
});

document.getElementById('btn-import-revenue')?.addEventListener('click', () => {
  openModal('Importar Revenue CSV', `
    <div style="margin-bottom:15px;font-size:13px;color:var(--text-secondary);line-height:1.5;">
      Sube un archivo CSV (separado por comas) con el formato:<br><br>
      <code style="background:var(--bg-secondary);padding:4px 8px;border-radius:4px;">Fecha (YYYY-MM-DD), Código País, Código Moneda, Importe</code><br><br>
      <small>* La primera fila se omitirá automáticamente si parece una cabecera.</small>
    </div>
    <div class="form-group">
      <input type="file" id="rv-csv-file" accept=".csv" style="width:100%;color:var(--text-primary);padding:10px;background:var(--bg-secondary);border-radius:8px;" />
    </div>
  `, async () => {
    const fileInput = document.getElementById('rv-csv-file');
    if (!fileInput || !fileInput.files.length) {
      alert('Por favor, selecciona un archivo CSV.');
      return false;
    }

    try {
      document.getElementById('modal-confirm').textContent = 'Importando...';
      const text = await fileInput.files[0].text();
      const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
      let imported = 0;

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        const parts = line.split(',').map(s => s.trim());
        if (i === 0 && isNaN(Date.parse(parts[0]))) continue; // Omitir cabecera
        if (parts.length < 4) {
          if (parts.length < 2) continue; // Al menos fecha e importe (como mínimo absoluto 2)
        }

        const dateStr = parts[0];
        const countryCode = parts[1] || '';
        const currencyCode = parts[2] || '';
        const amountStr = parts[3] !== undefined ? parts[3] : parts[parts.length - 1]; // Fallback

        if (!dateStr || !amountStr) continue;

        const country = (LISTS.country || []).find(c => c.code && c.code.toUpperCase() === countryCode.toUpperCase());
        const currency = (LISTS.currency || []).find(c => c.code && c.code.toUpperCase() === currencyCode.toUpperCase());

        await POST('/revenues', {
          views_date: dateStr,
          country_id: country ? country.id : null,
          currency_id: currency ? currency.id : null,
          amount: parseFloat(amountStr) || 0
        }).catch(e => console.error('Error importando fila:', e));

        imported++;
      }

      alert('Importación completada: ' + imported + ' registros añadidos.');
      document.getElementById('modal-confirm').textContent = 'Guardar';
      renderRevenue();
      return true;
    } catch (e) {
      console.error(e);
      alert('Error procesando el archivo CSV:\n' + e.message);
      document.getElementById('modal-confirm').textContent = 'Guardar';
      return false;
    }
  });
});

document.getElementById('btn-add-revenue').addEventListener('click', () => {
  openModal('Añadir Revenue Real', `
    <div class="form-row">
      <div class="form-group"><label class="form-label">Fecha</label><input type="date" id="rv-date" value="${new Date().toISOString().slice(0, 10)}" /></div>
      <div class="form-group"><label class="form-label">País</label><select id="rv-country" class="filter-select" style="width:100%;padding-right:28px">${listOptions('country')}</select></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Moneda</label><select id="rv-currency" class="filter-select" style="width:100%;padding-right:28px">${listOptions('currency')}</select></div>
      <div class="form-group"><label class="form-label">Importe</label><input type="number" id="rv-amount" placeholder="0.00" min="0" step="0.01" /></div>
    </div>
  `, async () => {
    try {
      await POST('/revenues', {
        views_date: document.getElementById('rv-date').value,
        country_id: parseInt(document.getElementById('rv-country').value),
        currency_id: parseInt(document.getElementById('rv-currency').value) || null,
        amount: parseFloat(document.getElementById('rv-amount').value) || 0,
      });
      await renderRevenue();
      return true;
    } catch (e) {
      alert('Error al guardar: ' + e.message);
      return false;
    }
  });
});

document.getElementById('btn-add-rpu').addEventListener('click', () => {
  openModal('Añadir RPU', `
    <div class="form-row">
      <div class="form-group"><label class="form-label">Fecha</label><input type="date" id="rp-date" value="${new Date().toISOString().slice(0, 10)}" /></div>
      <div class="form-group"><label class="form-label">País</label><select id="rp-country" class="filter-select" style="width:100%;padding-right:28px">${listOptions('country')}</select></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label class="form-label">Nicho</label><select id="rp-niche" class="filter-select" style="width:100%;padding-right:28px">${listOptions('niche')}</select></div>
      <div class="form-group"><label class="form-label">RPU (€/view)</label><input type="number" id="rp-rpu" placeholder="0.0200" min="0" step="0.0001" /></div>
    </div>
  `, async () => {
    try {
      await POST('/rpus', {
        views_date: document.getElementById('rp-date').value,
        country_id: parseInt(document.getElementById('rp-country').value),
        niche_id: parseInt(document.getElementById('rp-niche').value),
        rpu: parseFloat(document.getElementById('rp-rpu').value) || 0,
      });
      await renderRevenue();
      return true;
    } catch (e) {
      alert('Error al guardar: ' + e.message);
      return false;
    }
  });
});

// ─────────────────────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────────────────────
function renderSettings() {
  const categories = [
    { key: 'platform', listId: 'platform-list' },
    { key: 'niche', listId: 'niche-list' },
    { key: 'country', listId: 'country-list' },
    { key: 'language', listId: 'language-list' },
    { key: 'contract_status', listId: 'contract_status-list' },
    { key: 'mention_type', listId: 'mention_type-list' },
    { key: 'currency', listId: 'currency-list' },
  ];
  categories.forEach(({ key, listId }) => {
    const ul = document.getElementById(listId);
    if (!ul) return;
    const items = LISTS[key] || [];
    ul.innerHTML = items.map(lv => `
      <li>
        <span>${lv.value}${lv.code ? ` <small style="color:var(--text-tertiary)">(${lv.code})</small>` : ''}</span>
        <button onclick="deleteListValue(${lv.id}, '${key}')">Eliminar</button>
      </li>
    `).join('') || '<li style="color:var(--text-tertiary);font-size:12px;padding:8px 0">Sin valores</li>';
  });
}

window.deleteListValue = async (id, listName) => {
  if (!confirm('¿Eliminar este valor del catálogo?')) return;
  await DELETE(`/list_values/${id}`);
  await loadLists();
  renderSettings();
};

['platform', 'niche', 'country', 'language', 'contract_status', 'mention_type', 'currency'].forEach(key => {
  const btn = document.getElementById(`btn-add-${key}`);
  if (!btn) return;
  const labels = {
    platform: 'Plataforma', niche: 'Nicho', country: 'País', language: 'Idioma',
    contract_status: 'Estado de contrato', mention_type: 'Tipo de mención', currency: 'Moneda',
  };
  btn.addEventListener('click', () => {
    openModal(`Añadir ${labels[key] || key}`, `
      <div class="form-row">
        <div class="form-group"><label class="form-label">Nombre *</label><input type="text" id="st-value" placeholder="Nombre" /></div>
        <div class="form-group"><label class="form-label">Código</label><input type="text" id="st-code" placeholder="ej: ES" maxlength="10" /></div>
      </div>
    `, async () => {
      const value = document.getElementById('st-value').value.trim();
      const code = document.getElementById('st-code').value.trim() || null;
      if (!value) return false;
      const list = LISTS[key]?.[0];
      const listId = list?.list_id;
      if (!listId) { alert('No se encontró la lista para "' + key + '". Comprueba que exista en la base de datos.'); return false; }
      try {
        await POST('/list_values', { list_id: listId, value, code });
        await loadLists();
        renderSettings();
        return true;
      } catch (e) {
        alert('Error al guardar: ' + e.message);
        return false;
      }
    });
  });
});

// ─────────────────────────────────────────────────────────────
// MODAL SYSTEM
// ─────────────────────────────────────────────────────────────
let modalCallback = null;

function openModal(title, bodyHtml, onConfirm) {
  document.getElementById('modal-title').textContent = title;
  document.getElementById('modal-body').innerHTML = bodyHtml;
  document.getElementById('modal-overlay').classList.add('visible');
  modalCallback = onConfirm;
}

function closeModal() {
  document.getElementById('modal-overlay').classList.remove('visible');
  modalCallback = null;
}

document.getElementById('modal-close').addEventListener('click', closeModal);
document.getElementById('modal-cancel').addEventListener('click', closeModal);
document.getElementById('modal-overlay').addEventListener('click', e => {
  if (e.target === document.getElementById('modal-overlay')) closeModal();
});
document.getElementById('modal-confirm').addEventListener('click', async () => {
  if (modalCallback) {
    const result = await Promise.resolve(modalCallback());
    if (result !== false) closeModal();
  } else closeModal();
});

// ─────────────────────────────────────────────────────────────
// START
// ─────────────────────────────────────────────────────────────
init();
