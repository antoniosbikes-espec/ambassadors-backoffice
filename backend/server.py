#!/usr/bin/env python3
"""
Ambassadors Back Office — REST API Server
Python 3 + SQLite (stdlib only, no pip required)
"""

import sqlite3, json, os, sys, re
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

DB_PATH = os.path.join(os.path.dirname(__file__), 'ambassadors.db')
PORT    = int(os.environ.get('PORT', 8787))

# ─────────────────────────────────────────────────────────────
# DATABASE — SCHEMA + SEED
# ─────────────────────────────────────────────────────────────

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys = ON;

-- ── Catálogos ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lists (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL UNIQUE,   -- e.g. 'platform', 'country', 'language'...
    created_at TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS list_values (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    list_id    INTEGER NOT NULL REFERENCES lists(id),
    value      TEXT    NOT NULL,
    code       TEXT,                      -- e.g. 'ES', 'youtube'
    is_active  INTEGER NOT NULL DEFAULT 1,
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(list_id, value)
);

-- ── Embajadores ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ambassadors (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    email                TEXT    NOT NULL UNIQUE,
    first_name           TEXT    NOT NULL,
    last_name            TEXT,
    primary_language_id  INTEGER REFERENCES list_values(id),
    country_id           INTEGER REFERENCES list_values(id),
    created_at           TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_amb_email    ON ambassadors(email);
CREATE INDEX IF NOT EXISTS idx_amb_lang     ON ambassadors(primary_language_id);
CREATE INDEX IF NOT EXISTS idx_amb_country  ON ambassadors(country_id);

-- ── Perfiles sociales ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS profiles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ambassador_id   INTEGER NOT NULL REFERENCES ambassadors(id),
    platform_id     INTEGER NOT NULL REFERENCES list_values(id),
    handle          TEXT,
    url             TEXT    NOT NULL,
    niche_id        INTEGER REFERENCES list_values(id),
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_prof_ambassador ON profiles(ambassador_id);
CREATE INDEX IF NOT EXISTS idx_prof_platform   ON profiles(platform_id);
CREATE INDEX IF NOT EXISTS idx_prof_niche      ON profiles(niche_id);

-- ── Análisis de perfil (histórico KPIs) ────────────────────
CREATE TABLE IF NOT EXISTS profile_analyses (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id            INTEGER NOT NULL REFERENCES profiles(id),
    expected_views        INTEGER NOT NULL DEFAULT 0,
    total_30d_posts       INTEGER NOT NULL DEFAULT 0,
    cache_score           REAL CHECK(cache_score BETWEEN 0 AND 1),
    content_target_score  REAL CHECK(content_target_score BETWEEN 0 AND 1),
    country_target_score  REAL CHECK(country_target_score BETWEEN 0 AND 1),
    created_at            TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_pa_profile ON profile_analyses(profile_id);

-- ── Contratos ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS contracts (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id               INTEGER NOT NULL REFERENCES profiles(id),
    status_id                INTEGER NOT NULL REFERENCES list_values(id),
    currency_id              INTEGER REFERENCES list_values(id),
    price_per_standard_post  REAL,
    price_per_top_post       REAL,
    monthly_standard_posts   INTEGER DEFAULT 0,
    monthly_top_posts        INTEGER DEFAULT 0,
    last_analysis_id         INTEGER REFERENCES profile_analyses(id),
    signing_at               TEXT,
    end_at                   TEXT,
    created_at               TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_con_profile ON contracts(profile_id);
CREATE INDEX IF NOT EXISTS idx_con_status  ON contracts(status_id);

-- ── Posts (contenido publicado) ────────────────────────────
CREATE TABLE IF NOT EXISTS posts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id       INTEGER NOT NULL REFERENCES profiles(id),
    url              TEXT    NOT NULL UNIQUE,
    mention_type_id  INTEGER REFERENCES list_values(id),
    mention_offset   INTEGER NOT NULL DEFAULT 0,
    content_score    REAL    CHECK(content_score BETWEEN 0 AND 1),
    published_at     TEXT,
    created_at       TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_post_profile ON posts(profile_id);

-- ── Historial de visualizaciones ───────────────────────────
CREATE TABLE IF NOT EXISTS post_views_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id     INTEGER NOT NULL REFERENCES posts(id),
    views_date  TEXT    NOT NULL,
    new_views   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(post_id, views_date)
);
CREATE INDEX IF NOT EXISTS idx_pvh_post ON post_views_history(post_id, views_date);

-- ── Revenue real ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS revenues (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    views_date  TEXT    NOT NULL,
    country_id  INTEGER NOT NULL REFERENCES list_values(id),
    currency_id INTEGER REFERENCES list_values(id),
    amount      REAL    NOT NULL DEFAULT 0,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ── RPUs (Revenue per unit / view) ─────────────────────────
CREATE TABLE IF NOT EXISTS rpus (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    views_date  TEXT    NOT NULL,
    country_id  INTEGER NOT NULL REFERENCES list_values(id),
    niche_id    INTEGER NOT NULL REFERENCES list_values(id),
    rpu         REAL    NOT NULL DEFAULT 0,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(views_date, country_id, niche_id)
);
"""

SEEDS = """
-- Lists
INSERT OR IGNORE INTO lists(name) VALUES
  ('platform'),('country'),('language'),('niche'),
  ('contract_status'),('mention_type'),('currency');

-- Platforms
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'YouTube','youtube'   FROM lists WHERE name='platform';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Instagram','instagram' FROM lists WHERE name='platform';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'TikTok','tiktok'     FROM lists WHERE name='platform';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'LinkedIn','linkedin'  FROM lists WHERE name='platform';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Twitch','twitch'     FROM lists WHERE name='platform';

-- Countries
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'España','ES'          FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'México','MX'          FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Argentina','AR'       FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Estados Unidos','US'  FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Reino Unido','UK'     FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Francia','FR'         FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Alemania','DE'        FROM lists WHERE name='country';

-- Languages
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Español','ES'   FROM lists WHERE name='language';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Inglés','EN'    FROM lists WHERE name='language';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Portugués','PT' FROM lists WHERE name='language';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Francés','FR'   FROM lists WHERE name='language';

-- Niches
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Fashion','fashion'   FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Beauty','beauty'     FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Fitness','fitness'   FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Tech','tech'         FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Food','food'         FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Travel','travel'     FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Gaming','gaming'     FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Lifestyle','lifestyle' FROM lists WHERE name='niche';

-- Contract statuses
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Borrador','draft'       FROM lists WHERE name='contract_status';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Ofertado','offered'     FROM lists WHERE name='contract_status';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Firmado','signed'       FROM lists WHERE name='contract_status';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Expirado','expired'     FROM lists WHERE name='contract_status';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Cancelado','cancelled'  FROM lists WHERE name='contract_status';

-- Mention types
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Dedicado','dedicated'    FROM lists WHERE name='mention_type';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Integrado','integrated'  FROM lists WHERE name='mention_type';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Orgánico','organic'      FROM lists WHERE name='mention_type';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Patrocinado','sponsored' FROM lists WHERE name='mention_type';

-- Currencies
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Euro','EUR'              FROM lists WHERE name='currency';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Dólar USD','USD'         FROM lists WHERE name='currency';
INSERT OR IGNORE INTO list_values(list_id,value,code) SELECT id,'Libra esterlina','GBP'   FROM lists WHERE name='currency';
"""

DEMO_DATA = """
-- Demo ambassadors (only if table is empty)
INSERT OR IGNORE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'carlos@ejemplo.com','Carlos','Martínez',
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-01-10 00:00:00'
);
INSERT OR IGNORE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'laura@ejemplo.com','Laura','Gómez',
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-02-14 00:00:00'
);
INSERT OR IGNORE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'miguel@ejemplo.com','Miguel','Torres',
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE code='MX' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-01-28 00:00:00'
);
INSERT OR IGNORE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'sofia@ejemplo.com','Sofia','Ruiz',
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE code='AR' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-03-01 00:00:00'
);
INSERT OR IGNORE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'james@ejemplo.com','James','Wilson',
  (SELECT id FROM list_values WHERE code='EN' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE code='US' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-03-15 00:00:00'
);
INSERT OR IGNORE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'emma@ejemplo.com','Emma','Johnson',
  (SELECT id FROM list_values WHERE code='EN' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE code='UK' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-02-20 00:00:00'
);
INSERT OR IGNORE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'pablo@ejemplo.com','Pablo','Díaz',
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-01-05 00:00:00'
);
INSERT OR IGNORE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'ana@ejemplo.com','Ana','López',
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE code='MX' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2023-12-01 00:00:00'
);

-- Demo profiles
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='carlos@ejemplo.com'),
  (SELECT id FROM list_values WHERE code='youtube' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@carlosfitness','https://youtube.com/@carlosfitness',
  (SELECT id FROM list_values WHERE code='fitness' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-01-10 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='carlos@ejemplo.com'),
  (SELECT id FROM list_values WHERE code='instagram' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@carlos.fit','https://instagram.com/carlos.fit',
  (SELECT id FROM list_values WHERE code='fitness' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-01-10 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='laura@ejemplo.com'),
  (SELECT id FROM list_values WHERE code='youtube' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@laurabeauty','https://youtube.com/@laurabeauty',
  (SELECT id FROM list_values WHERE code='beauty' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-02-14 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='sofia@ejemplo.com'),
  (SELECT id FROM list_values WHERE code='instagram' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@sofia.fashion','https://instagram.com/sofia.fashion',
  (SELECT id FROM list_values WHERE code='fashion' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-03-01 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='james@ejemplo.com'),
  (SELECT id FROM list_values WHERE code='youtube' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@jameswilsontravel','https://youtube.com/@jamestravel',
  (SELECT id FROM list_values WHERE code='travel' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-03-15 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='emma@ejemplo.com'),
  (SELECT id FROM list_values WHERE code='youtube' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@emmafooduk','https://youtube.com/@emmafooduk',
  (SELECT id FROM list_values WHERE code='food' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-02-20 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='pablo@ejemplo.com'),
  (SELECT id FROM list_values WHERE code='youtube' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@pablofit','https://youtube.com/@pablofit',
  (SELECT id FROM list_values WHERE code='fitness' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-01-05 00:00:00'
);

-- Demo contracts
INSERT OR IGNORE INTO contracts(profile_id,status_id,currency_id,price_per_standard_post,price_per_top_post,monthly_standard_posts,monthly_top_posts,signing_at,end_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@carlosfitness'),
  (SELECT id FROM list_values WHERE code='signed' AND list_id=(SELECT id FROM lists WHERE name='contract_status')),
  (SELECT id FROM list_values WHERE code='EUR' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  1000.00,2000.00,4,1,'2024-01-10 00:00:00','2024-12-31 00:00:00'
);
INSERT OR IGNORE INTO contracts(profile_id,status_id,currency_id,price_per_standard_post,price_per_top_post,monthly_standard_posts,monthly_top_posts,signing_at,end_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@laurabeauty'),
  (SELECT id FROM list_values WHERE code='signed' AND list_id=(SELECT id FROM lists WHERE name='contract_status')),
  (SELECT id FROM list_values WHERE code='EUR' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  2000.00,4000.00,5,1,'2024-02-14 00:00:00','2024-12-31 00:00:00'
);
INSERT OR IGNORE INTO contracts(profile_id,status_id,currency_id,price_per_standard_post,monthly_standard_posts,signing_at,end_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@jameswilsontravel'),
  (SELECT id FROM list_values WHERE code='offered' AND list_id=(SELECT id FROM lists WHERE name='contract_status')),
  (SELECT id FROM list_values WHERE code='USD' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  3000.00,3,'2024-03-15 00:00:00','2025-03-15 00:00:00'
);
INSERT OR IGNORE INTO contracts(profile_id,status_id,currency_id,price_per_standard_post,monthly_standard_posts,signing_at,end_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@emmafooduk'),
  (SELECT id FROM list_values WHERE code='signed' AND list_id=(SELECT id FROM lists WHERE name='contract_status')),
  (SELECT id FROM list_values WHERE code='GBP' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  1600.00,3,'2024-02-20 00:00:00','2024-12-31 00:00:00'
);

-- Demo posts
INSERT OR IGNORE INTO posts(profile_id,url,mention_type_id,mention_offset,content_score,published_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@carlosfitness'),
  'https://youtu.be/abc001',
  (SELECT id FROM list_values WHERE code='dedicated' AND list_id=(SELECT id FROM lists WHERE name='mention_type')),
  30, 0.92,'2024-03-15 00:00:00'
);
INSERT OR IGNORE INTO posts(profile_id,url,mention_type_id,mention_offset,content_score,published_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@laurabeauty'),
  'https://youtu.be/abc002',
  (SELECT id FROM list_values WHERE code='integrated' AND list_id=(SELECT id FROM lists WHERE name='mention_type')),
  120,0.88,'2024-03-20 00:00:00'
);
INSERT OR IGNORE INTO posts(profile_id,url,mention_type_id,mention_offset,content_score,published_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@jameswilsontravel'),
  'https://youtu.be/abc003',
  (SELECT id FROM list_values WHERE code='dedicated' AND list_id=(SELECT id FROM lists WHERE name='mention_type')),
  0,0.91,'2024-03-18 00:00:00'
);
INSERT OR IGNORE INTO posts(profile_id,url,mention_type_id,mention_offset,content_score,published_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@emmafooduk'),
  'https://youtu.be/abc004',
  (SELECT id FROM list_values WHERE code='integrated' AND list_id=(SELECT id FROM lists WHERE name='mention_type')),
  60,0.75,'2024-03-22 00:00:00'
);

-- Demo post_views_history
INSERT OR IGNORE INTO post_views_history(post_id,views_date,new_views) VALUES(
  (SELECT id FROM posts WHERE url='https://youtu.be/abc001'), '2024-03-15', 185000
);
INSERT OR IGNORE INTO post_views_history(post_id,views_date,new_views) VALUES(
  (SELECT id FROM posts WHERE url='https://youtu.be/abc002'), '2024-03-20', 420000
);
INSERT OR IGNORE INTO post_views_history(post_id,views_date,new_views) VALUES(
  (SELECT id FROM posts WHERE url='https://youtu.be/abc003'), '2024-03-18', 540000
);
INSERT OR IGNORE INTO post_views_history(post_id,views_date,new_views) VALUES(
  (SELECT id FROM posts WHERE url='https://youtu.be/abc004'), '2024-03-22', 72000
);

-- Demo revenues
INSERT OR IGNORE INTO revenues(views_date,country_id,currency_id,amount) VALUES(
  '2024-03-01',
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE code='EUR' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  12300.00
);
INSERT OR IGNORE INTO revenues(views_date,country_id,currency_id,amount) VALUES(
  '2024-03-01',
  (SELECT id FROM list_values WHERE code='MX' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE code='USD' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  4800.00
);
INSERT OR IGNORE INTO revenues(views_date,country_id,currency_id,amount) VALUES(
  '2024-03-01',
  (SELECT id FROM list_values WHERE code='US' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE code='USD' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  18600.00
);
INSERT OR IGNORE INTO revenues(views_date,country_id,currency_id,amount) VALUES(
  '2024-03-01',
  (SELECT id FROM list_values WHERE code='UK' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE code='GBP' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  6840.00
);

-- Demo RPUs
INSERT OR IGNORE INTO rpus(views_date,country_id,niche_id,rpu)
SELECT '2024-03-01',
  (SELECT id FROM list_values WHERE code='ES' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE code='fitness' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  0.0220
WHERE NOT EXISTS(SELECT 1 FROM rpus LIMIT 1);

INSERT OR IGNORE INTO rpus(views_date,country_id,niche_id,rpu)
SELECT '2024-03-01',
  (SELECT id FROM list_values WHERE code='US' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE code='travel' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  0.0460
WHERE (SELECT COUNT(*) FROM rpus) < 2;
"""


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = get_db()
    conn.executescript(SCHEMA)
    conn.executescript(SEEDS)
    conn.executescript(DEMO_DATA)
    conn.commit()
    conn.close()
    print(f"[DB] Initialised at {DB_PATH}")

def rows_to_list(rows):
    return [dict(r) for r in rows]

# ─────────────────────────────────────────────────────────────
# HTTP HANDLER
# ─────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.command}] {self.path} → {args[1] if len(args)>1 else ''}")

    def send_json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(body)

    def send_err(self, msg, code=400):
        self.send_json({'error': msg}, code)

    def read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        if length == 0:
            return {}
        try:
            return json.loads(self.rfile.read(length))
        except:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):  self.route('GET')
    def do_POST(self): self.route('POST')
    def do_PUT(self):  self.route('PUT')
    def do_DELETE(self): self.route('DELETE')

    def route(self, method):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        qs     = parse_qs(parsed.query)

        # helper: extract id from path like /ambassadors/5
        def path_id(base):
            m = re.match(rf'^{base}/(\d+)$', path)
            return int(m.group(1)) if m else None

        try:
            # ── /api/lists ──────────────────────────────────
            if path == '/api/lists' and method == 'GET':
                return self.get_lists()
            if path == '/api/list_values' and method == 'GET':
                list_name = qs.get('list', [None])[0]
                return self.get_list_values(list_name)
            m = re.match(r'^/api/list_values/(\d+)$', path)
            if m:
                lv_id = int(m.group(1))
                if method == 'PUT':  return self.update_list_value(lv_id)
                if method == 'DELETE': return self.delete_list_value(lv_id)
            if path == '/api/list_values' and method == 'POST':
                return self.create_list_value()

            # ── /api/ambassadors ────────────────────────────
            if path == '/api/ambassadors' and method == 'GET':
                return self.get_ambassadors(qs)
            if path == '/api/ambassadors' and method == 'POST':
                return self.create_ambassador()
            pid = path_id('/api/ambassadors')
            if pid:
                if method == 'GET':    return self.get_ambassador(pid)
                if method == 'PUT':    return self.update_ambassador(pid)
                if method == 'DELETE': return self.delete_ambassador(pid)

            # ── /api/profiles ───────────────────────────────
            if path == '/api/profiles' and method == 'GET':
                return self.get_profiles(qs)
            if path == '/api/profiles' and method == 'POST':
                return self.create_profile()
            pid = path_id('/api/profiles')
            if pid:
                if method == 'GET':    return self.get_profile(pid)
                if method == 'PUT':    return self.update_profile(pid)
                if method == 'DELETE': return self.delete_profile(pid)

            # ── /api/profile_analyses ───────────────────────
            if path == '/api/profile_analyses' and method == 'GET':
                return self.get_profile_analyses(qs)
            if path == '/api/profile_analyses' and method == 'POST':
                return self.create_profile_analysis()

            # ── /api/contracts ──────────────────────────────
            if path == '/api/contracts' and method == 'GET':
                return self.get_contracts(qs)
            if path == '/api/contracts' and method == 'POST':
                return self.create_contract()
            pid = path_id('/api/contracts')
            if pid:
                if method == 'GET':    return self.get_contract(pid)
                if method == 'PUT':    return self.update_contract(pid)
                if method == 'DELETE': return self.delete_contract(pid)

            # ── /api/posts ──────────────────────────────────
            if path == '/api/posts' and method == 'GET':
                return self.get_posts(qs)
            if path == '/api/posts' and method == 'POST':
                return self.create_post()
            pid = path_id('/api/posts')
            if pid:
                if method == 'GET':    return self.get_post(pid)
                if method == 'PUT':    return self.update_post(pid)
                if method == 'DELETE': return self.delete_post(pid)

            # ── /api/post_views ──────────────────────────────
            if path == '/api/post_views' and method == 'GET':
                return self.get_post_views(qs)
            if path == '/api/post_views' and method == 'POST':
                return self.create_post_views()

            # ── /api/revenues ────────────────────────────────
            if path == '/api/revenues' and method == 'GET':
                return self.get_revenues(qs)
            if path == '/api/revenues' and method == 'POST':
                return self.create_revenue()
            pid = path_id('/api/revenues')
            if pid and method == 'DELETE': return self.delete_revenue(pid)

            # ── /api/rpus ────────────────────────────────────
            if path == '/api/rpus' and method == 'GET':
                return self.get_rpus(qs)
            if path == '/api/rpus' and method == 'POST':
                return self.create_rpu()
            pid = path_id('/api/rpus')
            if pid and method == 'DELETE': return self.delete_rpu(pid)

            # ── /api/dashboard ──────────────────────────────
            if path == '/api/dashboard' and method == 'GET':
                return self.get_dashboard()

            self.send_err('Not found', 404)

        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_err(str(e), 500)

    # ── LIST ENDPOINTS ───────────────────────────────────────
    def get_lists(self):
        db = get_db()
        rows = db.execute("SELECT * FROM lists ORDER BY name").fetchall()
        db.close()
        self.send_json(rows_to_list(rows))

    def get_list_values(self, list_name=None):
        db = get_db()
        if list_name:
            rows = db.execute("""
                SELECT lv.*, l.name as list_name FROM list_values lv
                JOIN lists l ON l.id = lv.list_id
                WHERE l.name = ? AND lv.is_active = 1
                ORDER BY lv.value
            """, (list_name,)).fetchall()
        else:
            rows = db.execute("""
                SELECT lv.*, l.name as list_name FROM list_values lv
                JOIN lists l ON l.id = lv.list_id
                WHERE lv.is_active = 1
                ORDER BY l.name, lv.value
            """).fetchall()
        db.close()
        self.send_json(rows_to_list(rows))

    def create_list_value(self):
        body = self.read_body()
        db = get_db()
        cur = db.execute(
            "INSERT INTO list_values(list_id,value,code) VALUES(?,?,?)",
            (body.get('list_id'), body.get('value'), body.get('code'))
        )
        db.commit()
        row = db.execute("SELECT lv.*,l.name as list_name FROM list_values lv JOIN lists l ON l.id=lv.list_id WHERE lv.id=?", (cur.lastrowid,)).fetchone()
        db.close()
        self.send_json(dict(row), 201)

    def update_list_value(self, lv_id):
        body = self.read_body()
        db = get_db()
        db.execute("UPDATE list_values SET value=?,code=?,is_active=? WHERE id=?",
                   (body.get('value'), body.get('code'), body.get('is_active', 1), lv_id))
        db.commit()
        row = db.execute("SELECT lv.*,l.name as list_name FROM list_values lv JOIN lists l ON l.id=lv.list_id WHERE lv.id=?", (lv_id,)).fetchone()
        db.close()
        self.send_json(dict(row))

    def delete_list_value(self, lv_id):
        db = get_db()
        db.execute("UPDATE list_values SET is_active=0 WHERE id=?", (lv_id,))
        db.commit()
        db.close()
        self.send_json({'deleted': lv_id})

    # ── AMBASSADOR ENDPOINTS ─────────────────────────────────
    def get_ambassadors(self, qs={}):
        db = get_db()
        sql = """
            SELECT a.*,
              lv_lang.value  AS language,
              lv_lang.code   AS language_code,
              lv_country.value AS country,
              lv_country.code  AS country_code,
              (SELECT COUNT(*) FROM profiles p WHERE p.ambassador_id = a.id) AS profile_count,
              (SELECT status_id FROM contracts c JOIN profiles p2 ON p2.id=c.profile_id
               WHERE p2.ambassador_id=a.id ORDER BY c.created_at DESC LIMIT 1) AS latest_contract_status_id,
              (SELECT lv.code FROM contracts c
               JOIN profiles p2 ON p2.id=c.profile_id
               JOIN list_values lv ON lv.id=c.status_id
               WHERE p2.ambassador_id=a.id ORDER BY c.created_at DESC LIMIT 1) AS latest_contract_status
            FROM ambassadors a
            LEFT JOIN list_values lv_lang    ON lv_lang.id    = a.primary_language_id
            LEFT JOIN list_values lv_country ON lv_country.id = a.country_id
        """
        params = []
        where = []
        if qs.get('country_id'):
            where.append('a.country_id = ?'); params.append(qs['country_id'][0])
        if qs.get('language_id'):
            where.append('a.primary_language_id = ?'); params.append(qs['language_id'][0])
        if qs.get('search'):
            where.append("(a.first_name || ' ' || COALESCE(a.last_name,'') || ' ' || a.email LIKE ?)")
            params.append(f'%{qs["search"][0]}%')
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' ORDER BY a.first_name'
        rows = db.execute(sql, params).fetchall()
        db.close()
        self.send_json(rows_to_list(rows))

    def get_ambassador(self, aid):
        db = get_db()
        row = db.execute("""
            SELECT a.*,
              lv_lang.value  AS language, lv_lang.code AS language_code,
              lv_country.value AS country, lv_country.code AS country_code
            FROM ambassadors a
            LEFT JOIN list_values lv_lang    ON lv_lang.id    = a.primary_language_id
            LEFT JOIN list_values lv_country ON lv_country.id = a.country_id
            WHERE a.id = ?
        """, (aid,)).fetchone()
        db.close()
        if not row: return self.send_err('Not found', 404)
        self.send_json(dict(row))

    def create_ambassador(self):
        body = self.read_body()
        db = get_db()
        cur = db.execute(
            "INSERT INTO ambassadors(email,first_name,last_name,primary_language_id,country_id) VALUES(?,?,?,?,?)",
            (body.get('email'), body.get('first_name'), body.get('last_name'),
             body.get('primary_language_id'), body.get('country_id'))
        )
        db.commit()
        row = db.execute("SELECT * FROM ambassadors WHERE id=?", (cur.lastrowid,)).fetchone()
        db.close()
        self.send_json(dict(row), 201)

    def update_ambassador(self, aid):
        body = self.read_body()
        db = get_db()
        db.execute("""UPDATE ambassadors SET email=?,first_name=?,last_name=?,
                      primary_language_id=?,country_id=? WHERE id=?""",
                   (body.get('email'), body.get('first_name'), body.get('last_name'),
                    body.get('primary_language_id'), body.get('country_id'), aid))
        db.commit()
        row = db.execute("SELECT * FROM ambassadors WHERE id=?", (aid,)).fetchone()
        db.close()
        self.send_json(dict(row))

    def delete_ambassador(self, aid):
        db = get_db()
        db.execute("DELETE FROM ambassadors WHERE id=?", (aid,))
        db.commit()
        db.close()
        self.send_json({'deleted': aid})

    # ── PROFILE ENDPOINTS ────────────────────────────────────
    def get_profiles(self, qs={}):
        db = get_db()
        sql = """
            SELECT p.*,
              a.first_name || ' ' || COALESCE(a.last_name,'') AS ambassador_name,
              lv_plat.value AS platform, lv_plat.code AS platform_code,
              lv_niche.value AS niche,   lv_niche.code AS niche_code,
              (SELECT pa.expected_views FROM profile_analyses pa WHERE pa.profile_id=p.id
               ORDER BY pa.created_at DESC LIMIT 1) AS expected_views,
              (SELECT pa.content_target_score FROM profile_analyses pa WHERE pa.profile_id=p.id
               ORDER BY pa.created_at DESC LIMIT 1) AS content_score,
              (SELECT SUM(pvh.new_views) FROM post_views_history pvh
               JOIN posts po ON po.id=pvh.post_id WHERE po.profile_id=p.id) AS total_views
            FROM profiles p
            JOIN ambassadors a ON a.id = p.ambassador_id
            LEFT JOIN list_values lv_plat  ON lv_plat.id  = p.platform_id
            LEFT JOIN list_values lv_niche ON lv_niche.id = p.niche_id
        """
        params = []
        where = []
        if qs.get('ambassador_id'):
            where.append('p.ambassador_id=?'); params.append(qs['ambassador_id'][0])
        if qs.get('platform_id'):
            where.append('p.platform_id=?'); params.append(qs['platform_id'][0])
        if qs.get('niche_id'):
            where.append('p.niche_id=?'); params.append(qs['niche_id'][0])
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' ORDER BY a.first_name, p.id'
        rows = db.execute(sql, params).fetchall()
        db.close()
        self.send_json(rows_to_list(rows))

    def get_profile(self, pid):
        db = get_db()
        row = db.execute("""
            SELECT p.*,
              a.first_name || ' ' || COALESCE(a.last_name,'') AS ambassador_name,
              lv_plat.value AS platform, lv_plat.code AS platform_code,
              lv_niche.value AS niche, lv_niche.code AS niche_code
            FROM profiles p
            JOIN ambassadors a ON a.id=p.ambassador_id
            LEFT JOIN list_values lv_plat  ON lv_plat.id  = p.platform_id
            LEFT JOIN list_values lv_niche ON lv_niche.id = p.niche_id
            WHERE p.id=?
        """, (pid,)).fetchone()
        db.close()
        if not row: return self.send_err('Not found', 404)
        self.send_json(dict(row))

    def create_profile(self):
        body = self.read_body()
        db = get_db()
        cur = db.execute(
            "INSERT INTO profiles(ambassador_id,platform_id,handle,url,niche_id) VALUES(?,?,?,?,?)",
            (body.get('ambassador_id'), body.get('platform_id'),
             body.get('handle'), body.get('url'), body.get('niche_id'))
        )
        db.commit()
        row = db.execute("SELECT * FROM profiles WHERE id=?", (cur.lastrowid,)).fetchone()
        db.close()
        self.send_json(dict(row), 201)

    def update_profile(self, pid):
        body = self.read_body()
        db = get_db()
        db.execute("UPDATE profiles SET platform_id=?,handle=?,url=?,niche_id=? WHERE id=?",
                   (body.get('platform_id'), body.get('handle'), body.get('url'),
                    body.get('niche_id'), pid))
        db.commit()
        row = db.execute("SELECT * FROM profiles WHERE id=?", (pid,)).fetchone()
        db.close()
        self.send_json(dict(row))

    def delete_profile(self, pid):
        db = get_db()
        db.execute("DELETE FROM profiles WHERE id=?", (pid,))
        db.commit()
        db.close()
        self.send_json({'deleted': pid})

    # ── PROFILE ANALYSES ─────────────────────────────────────
    def get_profile_analyses(self, qs={}):
        db = get_db()
        sql = "SELECT * FROM profile_analyses"
        params = []
        if qs.get('profile_id'):
            sql += ' WHERE profile_id=?'; params.append(qs['profile_id'][0])
        sql += ' ORDER BY created_at DESC'
        rows = db.execute(sql, params).fetchall()
        db.close()
        self.send_json(rows_to_list(rows))

    def create_profile_analysis(self):
        body = self.read_body()
        db = get_db()
        cur = db.execute("""
            INSERT INTO profile_analyses(profile_id,expected_views,total_30d_posts,
              cache_score,content_target_score,country_target_score)
            VALUES(?,?,?,?,?,?)""",
            (body.get('profile_id'), body.get('expected_views',0),
             body.get('total_30d_posts',0), body.get('cache_score'),
             body.get('content_target_score'), body.get('country_target_score'))
        )
        db.commit()
        row = db.execute("SELECT * FROM profile_analyses WHERE id=?", (cur.lastrowid,)).fetchone()
        db.close()
        self.send_json(dict(row), 201)

    # ── CONTRACT ENDPOINTS ───────────────────────────────────
    def get_contracts(self, qs={}):
        db = get_db()
        sql = """
            SELECT c.*,
              lv_st.value AS status, lv_st.code AS status_code,
              lv_cur.value AS currency, lv_cur.code AS currency_code,
              p.handle, p.ambassador_id,
              a.first_name || ' ' || COALESCE(a.last_name,'') AS ambassador_name,
              lv_plat.value AS platform, lv_plat.code AS platform_code,
              (c.price_per_standard_post * c.monthly_standard_posts +
               COALESCE(c.price_per_top_post,0) * COALESCE(c.monthly_top_posts,0)) * 12
               AS expected_annual_revenue
            FROM contracts c
            JOIN profiles p ON p.id = c.profile_id
            JOIN ambassadors a ON a.id = p.ambassador_id
            LEFT JOIN list_values lv_st   ON lv_st.id   = c.status_id
            LEFT JOIN list_values lv_cur  ON lv_cur.id  = c.currency_id
            LEFT JOIN list_values lv_plat ON lv_plat.id = p.platform_id
        """
        params = []
        where = []
        if qs.get('profile_id'):
            where.append('c.profile_id=?'); params.append(qs['profile_id'][0])
        if qs.get('ambassador_id'):
            where.append('p.ambassador_id=?'); params.append(qs['ambassador_id'][0])
        if qs.get('status_code'):
            where.append('lv_st.code=?'); params.append(qs['status_code'][0])
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' ORDER BY c.created_at DESC'
        rows = db.execute(sql, params).fetchall()
        db.close()
        self.send_json(rows_to_list(rows))

    def get_contract(self, cid):
        db = get_db()
        row = db.execute("""
            SELECT c.*, lv_st.value AS status, lv_st.code AS status_code,
              lv_cur.value AS currency, lv_cur.code AS currency_code,
              p.handle, p.ambassador_id
            FROM contracts c
            JOIN profiles p ON p.id=c.profile_id
            LEFT JOIN list_values lv_st  ON lv_st.id  = c.status_id
            LEFT JOIN list_values lv_cur ON lv_cur.id = c.currency_id
            WHERE c.id=?
        """, (cid,)).fetchone()
        db.close()
        if not row: return self.send_err('Not found', 404)
        self.send_json(dict(row))

    def create_contract(self):
        body = self.read_body()
        db = get_db()
        cur = db.execute("""
            INSERT INTO contracts(profile_id,status_id,currency_id,
              price_per_standard_post,price_per_top_post,
              monthly_standard_posts,monthly_top_posts,signing_at,end_at)
            VALUES(?,?,?,?,?,?,?,?,?)""",
            (body.get('profile_id'), body.get('status_id'), body.get('currency_id'),
             body.get('price_per_standard_post'), body.get('price_per_top_post'),
             body.get('monthly_standard_posts',0), body.get('monthly_top_posts',0),
             body.get('signing_at'), body.get('end_at'))
        )
        db.commit()
        row = db.execute("SELECT * FROM contracts WHERE id=?", (cur.lastrowid,)).fetchone()
        db.close()
        self.send_json(dict(row), 201)

    def update_contract(self, cid):
        body = self.read_body()
        db = get_db()
        db.execute("""UPDATE contracts SET status_id=?,currency_id=?,
              price_per_standard_post=?,price_per_top_post=?,
              monthly_standard_posts=?,monthly_top_posts=?,
              signing_at=?,end_at=? WHERE id=?""",
            (body.get('status_id'), body.get('currency_id'),
             body.get('price_per_standard_post'), body.get('price_per_top_post'),
             body.get('monthly_standard_posts',0), body.get('monthly_top_posts',0),
             body.get('signing_at'), body.get('end_at'), cid))
        db.commit()
        row = db.execute("SELECT * FROM contracts WHERE id=?", (cid,)).fetchone()
        db.close()
        self.send_json(dict(row))

    def delete_contract(self, cid):
        db = get_db()
        db.execute("DELETE FROM contracts WHERE id=?", (cid,))
        db.commit()
        db.close()
        self.send_json({'deleted': cid})

    # ── POST ENDPOINTS ───────────────────────────────────────
    def get_posts(self, qs={}):
        db = get_db()
        sql = """
            SELECT po.*,
              lv_mt.value AS mention_type, lv_mt.code AS mention_type_code,
              p.handle, p.ambassador_id,
              a.first_name || ' ' || COALESCE(a.last_name,'') AS ambassador_name,
              lv_plat.value AS platform, lv_plat.code AS platform_code,
              COALESCE(SUM(pvh.new_views),0) AS total_views
            FROM posts po
            JOIN profiles p ON p.id = po.profile_id
            JOIN ambassadors a ON a.id = p.ambassador_id
            LEFT JOIN list_values lv_mt   ON lv_mt.id   = po.mention_type_id
            LEFT JOIN list_values lv_plat ON lv_plat.id = p.platform_id
            LEFT JOIN post_views_history pvh ON pvh.post_id = po.id
        """
        params = []
        where = []
        if qs.get('profile_id'):
            where.append('po.profile_id=?'); params.append(qs['profile_id'][0])
        if qs.get('ambassador_id'):
            where.append('p.ambassador_id=?'); params.append(qs['ambassador_id'][0])
        if qs.get('platform_code'):
            where.append('lv_plat.code=?'); params.append(qs['platform_code'][0])
        if qs.get('mention_type_code'):
            where.append('lv_mt.code=?'); params.append(qs['mention_type_code'][0])
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' GROUP BY po.id ORDER BY po.published_at DESC'
        rows = db.execute(sql, params).fetchall()
        db.close()
        self.send_json(rows_to_list(rows))

    def get_post(self, pid):
        db = get_db()
        row = db.execute("""
            SELECT po.*, lv_mt.value AS mention_type, lv_mt.code AS mention_type_code,
              p.handle, p.ambassador_id,
              COALESCE(SUM(pvh.new_views),0) AS total_views
            FROM posts po
            JOIN profiles p ON p.id=po.profile_id
            LEFT JOIN list_values lv_mt ON lv_mt.id=po.mention_type_id
            LEFT JOIN post_views_history pvh ON pvh.post_id=po.id
            WHERE po.id=? GROUP BY po.id
        """, (pid,)).fetchone()
        db.close()
        if not row: return self.send_err('Not found', 404)
        self.send_json(dict(row))

    def create_post(self):
        body = self.read_body()
        db = get_db()
        cur = db.execute("""
            INSERT INTO posts(profile_id,url,mention_type_id,mention_offset,content_score,published_at)
            VALUES(?,?,?,?,?,?)""",
            (body.get('profile_id'), body.get('url'), body.get('mention_type_id'),
             body.get('mention_offset',0), body.get('content_score'), body.get('published_at'))
        )
        db.commit()
        row = db.execute("SELECT * FROM posts WHERE id=?", (cur.lastrowid,)).fetchone()
        db.close()
        self.send_json(dict(row), 201)

    def update_post(self, pid):
        body = self.read_body()
        db = get_db()
        db.execute("""UPDATE posts SET url=?,mention_type_id=?,mention_offset=?,
              content_score=?,published_at=? WHERE id=?""",
            (body.get('url'), body.get('mention_type_id'), body.get('mention_offset',0),
             body.get('content_score'), body.get('published_at'), pid))
        db.commit()
        row = db.execute("SELECT * FROM posts WHERE id=?", (pid,)).fetchone()
        db.close()
        self.send_json(dict(row))

    def delete_post(self, pid):
        db = get_db()
        db.execute("DELETE FROM posts WHERE id=?", (pid,))
        db.commit()
        db.close()
        self.send_json({'deleted': pid})

    # ── POST VIEWS ───────────────────────────────────────────
    def get_post_views(self, qs={}):
        db = get_db()
        sql = "SELECT * FROM post_views_history"
        params = []
        if qs.get('post_id'):
            sql += ' WHERE post_id=?'; params.append(qs['post_id'][0])
        sql += ' ORDER BY views_date'
        rows = db.execute(sql, params).fetchall()
        db.close()
        self.send_json(rows_to_list(rows))

    def create_post_views(self):
        body = self.read_body()
        db = get_db()
        cur = db.execute(
            "INSERT OR REPLACE INTO post_views_history(post_id,views_date,new_views) VALUES(?,?,?)",
            (body.get('post_id'), body.get('views_date'), body.get('new_views',0))
        )
        db.commit()
        row = db.execute("SELECT * FROM post_views_history WHERE id=?", (cur.lastrowid,)).fetchone()
        db.close()
        self.send_json(dict(row), 201)

    # ── REVENUES ─────────────────────────────────────────────
    def get_revenues(self, qs={}):
        db = get_db()
        rows = db.execute("""
            SELECT r.*, lv_c.value AS country, lv_c.code AS country_code,
              lv_cur.value AS currency, lv_cur.code AS currency_code
            FROM revenues r
            LEFT JOIN list_values lv_c   ON lv_c.id   = r.country_id
            LEFT JOIN list_values lv_cur ON lv_cur.id = r.currency_id
            ORDER BY r.views_date DESC
        """).fetchall()
        db.close()
        self.send_json(rows_to_list(rows))

    def create_revenue(self):
        body = self.read_body()
        db = get_db()
        cur = db.execute(
            "INSERT INTO revenues(views_date,country_id,currency_id,amount) VALUES(?,?,?,?)",
            (body.get('views_date'), body.get('country_id'), body.get('currency_id'), body.get('amount',0))
        )
        db.commit()
        row = db.execute("SELECT * FROM revenues WHERE id=?", (cur.lastrowid,)).fetchone()
        db.close()
        self.send_json(dict(row), 201)

    def delete_revenue(self, rid):
        db = get_db()
        db.execute("DELETE FROM revenues WHERE id=?", (rid,))
        db.commit()
        db.close()
        self.send_json({'deleted': rid})

    # ── RPUS ─────────────────────────────────────────────────
    def get_rpus(self, qs={}):
        db = get_db()
        rows = db.execute("""
            SELECT r.*, lv_c.value AS country, lv_c.code AS country_code,
              lv_n.value AS niche, lv_n.code AS niche_code
            FROM rpus r
            LEFT JOIN list_values lv_c ON lv_c.id = r.country_id
            LEFT JOIN list_values lv_n ON lv_n.id = r.niche_id
            ORDER BY r.views_date DESC
        """).fetchall()
        db.close()
        self.send_json(rows_to_list(rows))

    def create_rpu(self):
        body = self.read_body()
        db = get_db()
        cur = db.execute(
            "INSERT OR REPLACE INTO rpus(views_date,country_id,niche_id,rpu) VALUES(?,?,?,?)",
            (body.get('views_date'), body.get('country_id'), body.get('niche_id'), body.get('rpu',0))
        )
        db.commit()
        row = db.execute("SELECT * FROM rpus WHERE id=?", (cur.lastrowid,)).fetchone()
        db.close()
        self.send_json(dict(row), 201)

    def delete_rpu(self, rid):
        db = get_db()
        db.execute("DELETE FROM rpus WHERE id=?", (rid,))
        db.commit()
        db.close()
        self.send_json({'deleted': rid})

    # ── DASHBOARD (agregado) ─────────────────────────────────
    def get_dashboard(self):
        db = get_db()
        total_ambassadors = db.execute("SELECT COUNT(*) FROM ambassadors").fetchone()[0]
        total_profiles    = db.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
        signed_contracts  = db.execute("""
            SELECT COUNT(*) FROM contracts c
            JOIN list_values lv ON lv.id=c.status_id WHERE lv.code='signed'
        """).fetchone()[0]
        total_views = db.execute("SELECT COALESCE(SUM(new_views),0) FROM post_views_history").fetchone()[0]
        expected_revenue = db.execute("""
            SELECT COALESCE(SUM(
              COALESCE(price_per_standard_post,0)*COALESCE(monthly_standard_posts,0) +
              COALESCE(price_per_top_post,0)*COALESCE(monthly_top_posts,0)
            ),0) FROM contracts c
            JOIN list_values lv ON lv.id=c.status_id WHERE lv.code='signed'
        """).fetchone()[0]
        real_revenue = db.execute("SELECT COALESCE(SUM(amount),0) FROM revenues").fetchone()[0]

        # Views trend (last 30 days from post_views_history)
        trend = db.execute("""
            SELECT views_date, SUM(new_views) AS views
            FROM post_views_history
            WHERE views_date >= date('now','-30 days')
            GROUP BY views_date ORDER BY views_date
        """).fetchall()

        # Platform split
        platform_split = db.execute("""
            SELECT lv.value AS platform, COUNT(DISTINCT p.id) AS count
            FROM profiles p
            JOIN list_values lv ON lv.id=p.platform_id
            GROUP BY p.platform_id
        """).fetchall()

        # Top ambassadors
        top = db.execute("""
            SELECT a.id, a.first_name || ' ' || COALESCE(a.last_name,'') AS name,
              lv_c.code AS country_code,
              lv_l.code AS lang_code,
              lv_plat.code AS platform_code,
              COALESCE(SUM(pvh.new_views),0) AS total_views,
              COALESCE(AVG(po.content_score),0) AS avg_score,
              (SELECT lv2.code FROM contracts c2
               JOIN profiles p2 ON p2.id=c2.profile_id
               JOIN list_values lv2 ON lv2.id=c2.status_id
               WHERE p2.ambassador_id=a.id ORDER BY c2.created_at DESC LIMIT 1) AS contract_status
            FROM ambassadors a
            LEFT JOIN list_values lv_c    ON lv_c.id    = a.country_id
            LEFT JOIN list_values lv_l    ON lv_l.id    = a.primary_language_id
            LEFT JOIN profiles p          ON p.ambassador_id = a.id
            LEFT JOIN list_values lv_plat ON lv_plat.id = p.platform_id
            LEFT JOIN posts po            ON po.profile_id = p.id
            LEFT JOIN post_views_history pvh ON pvh.post_id = po.id
            GROUP BY a.id ORDER BY total_views DESC LIMIT 6
        """).fetchall()

        db.close()
        self.send_json({
            'kpis': {
                'total_ambassadors': total_ambassadors,
                'total_profiles':    total_profiles,
                'signed_contracts':  signed_contracts,
                'total_views':       total_views,
                'expected_revenue':  round(expected_revenue, 2),
                'real_revenue':      round(real_revenue, 2),
            },
            'views_trend':    rows_to_list(trend),
            'platform_split': rows_to_list(platform_split),
            'top_ambassadors': rows_to_list(top),
        })


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    init_db()
    server = HTTPServer(('0.0.0.0', PORT), Handler)
    print(f"[SERVER] Ambassadors API running at http://0.0.0.0:{PORT}")
    print(f"[SERVER] Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[SERVER] Stopped.")
