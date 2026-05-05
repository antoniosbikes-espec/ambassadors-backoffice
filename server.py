#!/usr/bin/env python3
"""
Ambassadors Back Office — REST API Server
Python 3 + SQLite (stdlib only, no pip required)
"""

import sqlite3, json, os, sys, re, time, urllib.request
VERSION = "1.0.5 - Clean Migrations"
print(f"--- SERVER VERSION: {VERSION} ---")
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
from threading import Lock

# Usamos una carpeta dedicada para la base de datos en el raíz
if os.path.exists('/persistent_data'):
    DB_DIR = '/persistent_data'
elif os.path.exists('/data'):
    DB_DIR = '/data'
else:
    # If server.py is in root, use ./db. If in backend/, use ../db.
    base = os.path.dirname(__file__)
    if os.path.exists(os.path.join(base, 'db')) or not os.path.exists(os.path.join(os.path.dirname(base), 'db')):
        DB_DIR = os.path.join(base, 'db')
    else:
        DB_DIR = os.path.join(os.path.dirname(base), 'db')

DB_PATH = os.path.join(DB_DIR, 'ambassadors_v3.db')

if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR, exist_ok=True)

# Intentamos forzar permisos de escritura si estamos en Railway/Linux
try:
    if os.name != 'nt': # No en Windows
        os.chmod(DB_DIR, 0o777)
        if os.path.exists(DB_PATH):
            os.chmod(DB_PATH, 0o666)
except:
    pass

PORT         = int(os.environ.get('PORT', 8787))
AUTH_TOKEN   = os.environ.get('AUTH_TOKEN', 'regaliz-marketing-token-2024')
APP_USERNAME = os.environ.get('APP_USERNAME', 'marketing')
APP_PASSWORD = os.environ.get('APP_PASSWORD', 'regaliz1')
USER_CREDENTIALS = {APP_USERNAME: APP_PASSWORD}

# ── CORS: restringir al dominio de producción ─────────────────
# Permite también localhost para desarrollo local
ALLOWED_ORIGINS = ['*']

def get_cors_origin(request_origin):
    """Devuelve el origen permitido o '*' si está habilitado."""
    if '*' in ALLOWED_ORIGINS:
        return '*'
    if request_origin and request_origin in ALLOWED_ORIGINS:
        return request_origin
    return ALLOWED_ORIGINS[0]

# ─────────────────────────────────────────────────────────────
# DATABASE — SCHEMA + SEED
# ─────────────────────────────────────────────────────────────

SCHEMA = """
PRAGMA journal_mode=DELETE; -- Más compatible con discos de red que WAL
PRAGMA foreign_keys = ON;

-- ── Catálogos ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lists (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL UNIQUE   -- e.g. 'platform', 'country', 'language'...
);

CREATE TABLE IF NOT EXISTS list_values (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    list_id    INTEGER NOT NULL REFERENCES lists(id),
    value      TEXT    NOT NULL,
    is_active  INTEGER NOT NULL DEFAULT 1,
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
    content_target_score  REAL CHECK(content_target_score BETWEEN 0 AND 10),
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
    contract_file_url        TEXT,
    notes                    TEXT,
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
    published_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_post_profile ON posts(profile_id);

-- ── Historial de visualizaciones (daily_views) ─────────────
CREATE TABLE IF NOT EXISTS daily_views (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id     INTEGER NOT NULL REFERENCES posts(id),
    views_date  TEXT    NOT NULL,
    new_views   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(post_id, views_date)
);
CREATE INDEX IF NOT EXISTS idx_dv_post ON daily_views(post_id, views_date);

-- ── Revenue real ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS revenues (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    views_date    TEXT    NOT NULL,
    country_id    INTEGER NOT NULL REFERENCES list_values(id),
    currency_id   INTEGER REFERENCES list_values(id),
    new_revenue   REAL    NOT NULL DEFAULT 0
);

-- ── RPUs (Revenue per unit / view) ─────────────────────────
CREATE TABLE IF NOT EXISTS rpus (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    views_date  TEXT    NOT NULL,
    country_id  INTEGER NOT NULL REFERENCES list_values(id),
    niche_id    INTEGER NOT NULL REFERENCES list_values(id),
    rpu         REAL    NOT NULL DEFAULT 0,
    UNIQUE(views_date, country_id, niche_id)
);
"""

SEEDS = """
-- Lists
INSERT OR IGNORE INTO lists(name) VALUES
  ('platform'),('country'),('language'),('niche'),
  ('contract_status'),('mention_type'),('currency');

-- Platforms
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'YouTube'   FROM lists WHERE name='platform';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Instagram' FROM lists WHERE name='platform';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'TikTok'    FROM lists WHERE name='platform';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'LinkedIn'  FROM lists WHERE name='platform';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Twitch'    FROM lists WHERE name='platform';

-- Countries
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'España'          FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'México'          FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Argentina'       FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Estados Unidos'  FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Reino Unido'     FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Francia'         FROM lists WHERE name='country';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Alemania'        FROM lists WHERE name='country';

-- Languages
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Español'   FROM lists WHERE name='language';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Inglés'    FROM lists WHERE name='language';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Portugués' FROM lists WHERE name='language';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Francés'   FROM lists WHERE name='language';

-- Niches
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Fashion'   FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Beauty'     FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Fitness'   FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Tech'         FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Food'         FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Travel'     FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Gaming'     FROM lists WHERE name='niche';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Lifestyle' FROM lists WHERE name='niche';

-- Contract statuses
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Borrador'       FROM lists WHERE name='contract_status';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Ofertado'     FROM lists WHERE name='contract_status';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Firmado'       FROM lists WHERE name='contract_status';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Expirado'     FROM lists WHERE name='contract_status';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Cancelado'  FROM lists WHERE name='contract_status';

-- Mention types
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'M (Mention)' FROM lists WHERE name='mention_type';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'OM (Organic Mention)' FROM lists WHERE name='mention_type';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'TikTok' FROM lists WHERE name='mention_type';

-- Currencies
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Euro'              FROM lists WHERE name='currency';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Dólar USD'         FROM lists WHERE name='currency';
INSERT OR IGNORE INTO list_values(list_id,value) SELECT id,'Libra esterlina'   FROM lists WHERE name='currency';
"""

DEMO_DATA = """
-- Demo ambassadors (only if table is empty)
INSERT OR REPLACE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'carlos@ejemplo.com','Carlos','Martínez',
  (SELECT id FROM list_values WHERE value='Español' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE value='España' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-01-10 00:00:00'
);
INSERT OR REPLACE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'laura@ejemplo.com','Laura','Gómez',
  (SELECT id FROM list_values WHERE value='Español' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE value='España' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-02-14 00:00:00'
);
INSERT OR REPLACE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'miguel@ejemplo.com','Miguel','Torres',
  (SELECT id FROM list_values WHERE value='Español' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE value='México' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-01-28 00:00:00'
);
INSERT OR REPLACE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'sofia@ejemplo.com','Sofia','Ruiz',
  (SELECT id FROM list_values WHERE value='Español' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE value='Argentina' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-03-01 00:00:00'
);
INSERT OR REPLACE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'james@ejemplo.com','James','Wilson',
  (SELECT id FROM list_values WHERE value='Inglés' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE value='Estados Unidos' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-03-15 00:00:00'
);
INSERT OR REPLACE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'emma@ejemplo.com','Emma','Johnson',
  (SELECT id FROM list_values WHERE value='Inglés' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE value='Reino Unido' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-02-20 00:00:00'
);
INSERT OR REPLACE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'pablo@ejemplo.com','Pablo','Díaz',
  (SELECT id FROM list_values WHERE value='Español' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE value='España' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2024-01-05 00:00:00'
);
INSERT OR REPLACE INTO ambassadors(email,first_name,last_name,primary_language_id,country_id,created_at) VALUES(
  'ana@ejemplo.com','Ana','López',
  (SELECT id FROM list_values WHERE value='Español' AND list_id=(SELECT id FROM lists WHERE name='language')),
  (SELECT id FROM list_values WHERE value='México' AND list_id=(SELECT id FROM lists WHERE name='country')),
  '2023-12-01 00:00:00'
);

-- Demo profiles
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='carlos@ejemplo.com'),
  (SELECT id FROM list_values WHERE value='YouTube' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@carlosfitness','https://youtube.com/@carlosfitness',
  (SELECT id FROM list_values WHERE value='Fitness' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-01-10 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='carlos@ejemplo.com'),
  (SELECT id FROM list_values WHERE value='Instagram' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@carlos.fit','https://instagram.com/carlos.fit',
  (SELECT id FROM list_values WHERE value='Fitness' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-01-10 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='laura@ejemplo.com'),
  (SELECT id FROM list_values WHERE value='YouTube' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@laurabeauty','https://youtube.com/@laurabeauty',
  (SELECT id FROM list_values WHERE value='Beauty' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-02-14 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='sofia@ejemplo.com'),
  (SELECT id FROM list_values WHERE value='Instagram' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@sofia.fashion','https://instagram.com/sofia.fashion',
  (SELECT id FROM list_values WHERE value='Fashion' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-03-01 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='james@ejemplo.com'),
  (SELECT id FROM list_values WHERE value='YouTube' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@jameswilsontravel','https://youtube.com/@jamestravel',
  (SELECT id FROM list_values WHERE value='Travel' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-03-15 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='emma@ejemplo.com'),
  (SELECT id FROM list_values WHERE value='YouTube' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@emmafooduk','https://youtube.com/@emmafooduk',
  (SELECT id FROM list_values WHERE value='Food' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-02-20 00:00:00'
);
INSERT OR IGNORE INTO profiles(ambassador_id,platform_id,handle,url,niche_id,created_at) VALUES(
  (SELECT id FROM ambassadors WHERE email='pablo@ejemplo.com'),
  (SELECT id FROM list_values WHERE value='YouTube' AND list_id=(SELECT id FROM lists WHERE name='platform')),
  '@pablofit','https://youtube.com/@pablofit',
  (SELECT id FROM list_values WHERE value='Fitness' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  '2024-01-05 00:00:00'
);

-- Demo contracts
INSERT OR IGNORE INTO contracts(profile_id,status_id,currency_id,price_per_standard_post,price_per_top_post,monthly_standard_posts,monthly_top_posts,signing_at,end_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@carlosfitness'),
  (SELECT id FROM list_values WHERE value='Firmado' AND list_id=(SELECT id FROM lists WHERE name='contract_status')),
  (SELECT id FROM list_values WHERE value='Euro' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  1000.00,2000.00,4,1,'2024-01-10 00:00:00','2024-12-31 00:00:00'
);
INSERT OR IGNORE INTO contracts(profile_id,status_id,currency_id,price_per_standard_post,price_per_top_post,monthly_standard_posts,monthly_top_posts,signing_at,end_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@laurabeauty'),
  (SELECT id FROM list_values WHERE value='Firmado' AND list_id=(SELECT id FROM lists WHERE name='contract_status')),
  (SELECT id FROM list_values WHERE value='Euro' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  2000.00,4000.00,5,1,'2024-02-14 00:00:00','2024-12-31 00:00:00'
);
INSERT OR IGNORE INTO contracts(profile_id,status_id,currency_id,price_per_standard_post,monthly_standard_posts,signing_at,end_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@jameswilsontravel'),
  (SELECT id FROM list_values WHERE value='Ofertado' AND list_id=(SELECT id FROM lists WHERE name='contract_status')),
  (SELECT id FROM list_values WHERE value='Dólar USD' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  3000.00,3,'2024-03-15 00:00:00','2025-03-15 00:00:00'
);
INSERT OR IGNORE INTO contracts(profile_id,status_id,currency_id,price_per_standard_post,monthly_standard_posts,signing_at,end_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@emmafooduk'),
  (SELECT id FROM list_values WHERE value='Firmado' AND list_id=(SELECT id FROM lists WHERE name='contract_status')),
  (SELECT id FROM list_values WHERE value='Libra esterlina' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  1600.00,3,'2024-02-20 00:00:00','2024-12-31 00:00:00'
);

-- Demo posts
INSERT OR IGNORE INTO posts(profile_id,url,mention_type_id,mention_offset,content_score,published_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@carlosfitness'),
  'https://youtu.be/abc001',
  (SELECT id FROM list_values WHERE value='M (Mention)' AND list_id=(SELECT id FROM lists WHERE name='mention_type')),
  30, 0.92,'2024-03-15 00:00:00'
);
INSERT OR IGNORE INTO posts(profile_id,url,mention_type_id,mention_offset,content_score,published_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@laurabeauty'),
  'https://youtu.be/abc002',
  (SELECT id FROM list_values WHERE value='OM (Organic Mention)' AND list_id=(SELECT id FROM lists WHERE name='mention_type')),
  120,0.88,'2024-03-20 00:00:00'
);
INSERT OR IGNORE INTO posts(profile_id,url,mention_type_id,mention_offset,content_score,published_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@jameswilsontravel'),
  'https://youtu.be/abc003',
  (SELECT id FROM list_values WHERE value='M (Mention)' AND list_id=(SELECT id FROM lists WHERE name='mention_type')),
  0,0.91,'2024-03-18 00:00:00'
);
INSERT OR IGNORE INTO posts(profile_id,url,mention_type_id,mention_offset,content_score,published_at) VALUES(
  (SELECT id FROM profiles WHERE handle='@emmafooduk'),
  'https://youtu.be/abc004',
  (SELECT id FROM list_values WHERE value='OM (Organic Mention)' AND list_id=(SELECT id FROM lists WHERE name='mention_type')),
  60,0.75,'2024-03-22 00:00:00'
);

-- Demo daily_views
INSERT OR IGNORE INTO daily_views(post_id,views_date,new_views) VALUES(
  (SELECT id FROM posts WHERE url='https://youtu.be/abc001'), '2024-03-15', 185000
);
INSERT OR IGNORE INTO daily_views(post_id,views_date,new_views) VALUES(
  (SELECT id FROM posts WHERE url='https://youtu.be/abc002'), '2024-03-20', 420000
);
INSERT OR IGNORE INTO daily_views(post_id,views_date,new_views) VALUES(
  (SELECT id FROM posts WHERE url='https://youtu.be/abc003'), '2024-03-18', 540000
);
INSERT OR IGNORE INTO daily_views(post_id,views_date,new_views) VALUES(
  (SELECT id FROM posts WHERE url='https://youtu.be/abc004'), '2024-03-22', 72000
);

-- Demo revenues
INSERT OR IGNORE INTO revenues(views_date,country_id,currency_id,new_revenue) VALUES(
  '2024-03-01',
  (SELECT id FROM list_values WHERE value='España' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE value='Euro' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  12300.00
);
INSERT OR IGNORE INTO revenues(views_date,country_id,currency_id,new_revenue) VALUES(
  '2024-03-01',
  (SELECT id FROM list_values WHERE value='México' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE value='Dólar USD' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  4800.00
);
INSERT OR IGNORE INTO revenues(views_date,country_id,currency_id,new_revenue) VALUES(
  '2024-03-01',
  (SELECT id FROM list_values WHERE value='Estados Unidos' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE value='Dólar USD' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  18600.00
);
INSERT OR IGNORE INTO revenues(views_date,country_id,currency_id,new_revenue) VALUES(
  '2024-03-01',
  (SELECT id FROM list_values WHERE value='Reino Unido' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE value='Libra esterlina' AND list_id=(SELECT id FROM lists WHERE name='currency')),
  6840.00
);

-- Demo RPUs
INSERT OR IGNORE INTO rpus(views_date,country_id,niche_id,rpu)
SELECT '2024-03-01',
  (SELECT id FROM list_values WHERE value='España' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE value='Fitness' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  0.0220
WHERE NOT EXISTS(SELECT 1 FROM rpus LIMIT 1);

INSERT OR IGNORE INTO rpus(views_date,country_id,niche_id,rpu)
SELECT '2024-03-01',
  (SELECT id FROM list_values WHERE value='Estados Unidos' AND list_id=(SELECT id FROM lists WHERE name='country')),
  (SELECT id FROM list_values WHERE value='Travel' AND list_id=(SELECT id FROM lists WHERE name='niche')),
  0.0460
WHERE (SELECT COUNT(*) FROM rpus) < 2;
"""


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""
    daemon_threads = True

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000") # 30s timeout
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except:
        pass 
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = get_db()
    
def init_db():
    conn = get_db()
    
    def table_has_col(table, col):
        cursor = conn.execute(f"PRAGMA table_info({table})")
        cols = [r[1].lower() for r in cursor.fetchall()]
        return col.lower() in cols

    def safe_add_col(table, col, type_def):
        if not table_has_col(table, col):
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {type_def}")
                print(f"[DB] Columna {col} añadida a {table}")
            except Exception as e: print(f"[DB] Error añadiendo {col}: {e}")

    def safe_drop_col(table, col):
        if table_has_col(table, col):
            try:
                conn.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
                print(f"[DB] Columna {col} eliminada de {table}")
            except Exception as e: print(f"[DB] Error eliminando {col}: {e}")

    # DEBUG: Listar todo en la DB
    try:
        items = conn.execute("SELECT type, name FROM sqlite_master").fetchall()
        print(f"[DB DEBUG] Objetos en DB: {[dict(i) for i in items]}")
    except: pass

    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        # Limpieza de posibles tablas temporales de migraciones fallidas anteriores
        conn.execute("DROP TABLE IF EXISTS ambassadors_old")
        conn.execute("DROP TABLE IF EXISTS lists_old")
        conn.execute("DROP TABLE IF EXISTS list_values_old")
        
        # Asegurar tablas básicas
        conn.executescript(SCHEMA)
        
        # MIGRACIONES ESPECÍFICAS
        # 1. Ambassadors
        safe_add_col('ambassadors', 'created_at', "TEXT NOT NULL DEFAULT (datetime('now'))")
        safe_drop_col('ambassadors', 'notes')
        
        # 2. Lists & Values
        safe_drop_col('lists', 'created_at')
        safe_drop_col('list_values', 'created_at')
        safe_drop_col('list_values', 'code')
        
        # 3. Revenues
        safe_add_col('revenues', 'new_revenue', "REAL NOT NULL DEFAULT 0")
        if table_has_col('revenues', 'amount'):
            conn.execute("UPDATE revenues SET new_revenue = amount WHERE new_revenue = 0")
            safe_drop_col('revenues', 'amount')
        safe_drop_col('revenues', 'created_at')
        safe_drop_col('revenues', 'niche_id')
        
        # 4. Posts
        safe_drop_col('posts', 'created_at')
        
        # 5. RPUs
        safe_drop_col('rpus', 'created_at')

        # 6. Contracts — asegurar todas las columnas requeridas
        safe_add_col('contracts', 'signing_at', 'TEXT')
        safe_add_col('contracts', 'end_at', 'TEXT')
        safe_add_col('contracts', 'contract_file_url', 'TEXT')
        safe_add_col('contracts', 'notes', 'TEXT')

        # SEEDS
        conn.executescript(SEEDS)
        conn.executescript(DEMO_DATA)
        
        conn.execute("PRAGMA foreign_keys = ON")
        conn.commit()
    except Exception as e:
        print(f"[DB] Error crítico init_db: {e}")
        conn.execute("PRAGMA foreign_keys = ON")

    conn.close()
    
    # DIAGNÓSTICO DE FILAS
    conn = get_db()
    try:
        tables = ['ambassadors', 'profiles', 'contracts', 'posts', 'revenues', 'list_values']
        counts = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}
        print(f"[DB] Diagnóstico de datos: {counts}")
        if counts['ambassadors'] == 0:
            print("[DB] ADVERTENCIA: La tabla de embajadores está VACÍA.")
    except Exception as e:
        print(f"[DB] Error en diagnóstico: {e}")
    finally:
        conn.close()

    print(f"[DB] Initialised at {DB_PATH}")

def rows_to_list(rows):
    return [dict(r) for r in rows]

# ─────────────────────────────────────────────────────────────
# RATE LIMITING — Protección contra fuerza bruta en login
# ─────────────────────────────────────────────────────────────
MAX_LOGIN_ATTEMPTS = 10         # Intentos antes de bloquear
LOCKOUT_SECONDS    = 60        # 1 minuto de bloqueo

_login_attempts = {}   # { ip: {'count': int, 'locked_until': float} }
_login_lock     = Lock()

def check_rate_limit(ip):
    """Devuelve (bloqueado, segundos_restantes)."""
    with _login_lock:
        now  = time.time()
        data = _login_attempts.get(ip, {'count': 0, 'locked_until': 0})
        if data['locked_until'] > now:
            return True, int(data['locked_until'] - now)
        if data['locked_until'] and data['locked_until'] <= now:
            _login_attempts[ip] = {'count': 0, 'locked_until': 0}
        return False, 0

def record_failed_login(ip):
    with _login_lock:
        now  = time.time()
        data = _login_attempts.get(ip, {'count': 0, 'locked_until': 0})
        data['count'] += 1
        if data['count'] >= MAX_LOGIN_ATTEMPTS:
            data['locked_until'] = now + LOCKOUT_SECONDS
            print(f"[SECURITY] IP {ip} bloqueada 15 min tras {MAX_LOGIN_ATTEMPTS} intentos fallidos")
        _login_attempts[ip] = data

def reset_login_attempts(ip):
    with _login_lock:
        _login_attempts.pop(ip, None)

# ─────────────────────────────────────────────────────────────
# HTTP HANDLER
# ─────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.command}] {self.path} → {args[1] if len(args)>1 else ''}")

    def send_json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
        origin = self.headers.get('Origin', '')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', get_cors_origin(origin))
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Vary', 'Origin')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('X-Server-Version', '2.0.5_secure')
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
        origin = self.headers.get('Origin', '')
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', get_cors_origin(origin))
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Vary', 'Origin')
        self.end_headers()

    def do_HEAD(self):
        # Railway CDN hace HEAD requests como health check.
        # Responder 200 para que no marque el servidor como caído.
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def do_GET(self):  self.handle_method('GET')
    # ── Utilidades de scraping para vistas reales ──────
    def fetch_real_views(self, platform_value, url):
        if not url: return None
        try:
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
            if 'youtube.com' in url or 'youtu.be' in url:
                req = urllib.request.Request(url, headers={'User-Agent': user_agent})
                with urllib.request.urlopen(req, timeout=10) as response:
                    html = response.read().decode('utf-8', errors='ignore')
                    # Intentamos varios patrones comunes en el JSON de YouTube
                    match = re.search(r'"viewCount":"(\d+)"', html)
                    if not match:
                        match = re.search(r'\\\"viewCount\\\":\\\"(\d+)\\\"', html)
                    if not match:
                        # Patrón para shorts o versiones alternativas
                        match = re.search(r'"videoViewCountRenderer":\s*{"viewCount":\s*{"simpleText":"([\d,.]+)', html)
                        if match:
                            views_str = re.sub(r'[^\d]', '', match.group(1))
                            return int(views_str)
                    
                    if match:
                        print(f"[Scraper] OK: {url} -> {match.group(1)} views")
                        return int(match.group(1))
                    else:
                        print(f"[Scraper] No se encontró viewCount en {url}")
            # TikTok/Instagram siguen siendo difíciles sin API
        except Exception as e:
            print(f"[Scraper] Error fetching {url}: {e}")
        return None

    def sync_all_views(self):
        try:
            print("[Sync] Iniciando sincronización de visualizaciones reales...")
            posts = self.db.execute("""
                SELECT po.id, po.url, lv.value as platform_value 
                FROM posts po
                JOIN profiles p ON p.id = po.profile_id
                JOIN list_values lv ON lv.id = p.platform_id
            """).fetchall()
            
            updated = 0
            for p in posts:
                real_v = self.fetch_real_views(p['platform_value'], p['url'])
                if real_v is not None:
                    last_v = self.db.execute("SELECT SUM(new_views) FROM daily_views WHERE post_id=?", (p['id'],)).fetchone()[0] or 0
                    if real_v > last_v:
                        diff = real_v - last_v
                        self.db.execute("INSERT OR IGNORE INTO daily_views (post_id, new_views, views_date) VALUES (?, 0, date('now'))", (p['id'],))
                        self.db.execute("UPDATE daily_views SET new_views = new_views + ? WHERE post_id=? AND views_date=date('now')", (diff, p['id']))
                        updated += 1
            
            self.db.commit()
            self.send_json({"status": "success", "updated_posts": updated})
        except Exception as e:
            print(f"[Sync] Error: {e}")
            self.send_err(f"Error al sincronizar: {e}", 500)

    def do_POST(self): self.handle_method('POST')
    def do_PUT(self):  self.handle_method('PUT')
    def do_DELETE(self): self.handle_method('DELETE')

    def handle_method(self, method):
        # 1. Rutas públicas (Login)
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')
        
        if path == '/api/login' and method == 'POST':
            return self.handle_login()
        if path == '/api/ping':
            return self.send_json({'status': 'ok', 'user_defined': APP_USERNAME != 'marketing'})
        if path == '/api/health':
            return self.send_json({'status': 'ok', 'timestamp': '2026-05-02T20:12:00', 'version': '2.0.7'})
        if path == '/api/debug-schema':
            db = get_db()
            rows = db.execute("SELECT type, name, tbl_name, sql FROM sqlite_master").fetchall()
            db.close()
            return self.send_json(rows_to_list(rows))
        if path == '/api/ping':
            return self.send_json({'status': 'ok'})

        # 2. Verificar Autenticación para el resto
        auth_header = self.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer ') or auth_header[7:] != AUTH_TOKEN:
            return self.send_err('Unauthorized', 401)

        # 3. Abrir DB y enrutar
        self.db = get_db()
        try:
            # ── /api/tables ────────────────────────────
            if path == '/api/tables' and method == 'GET':
                # Listar todas las tablas de usuario
                tables = self.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_old' AND name NOT LIKE '%_repair' AND name NOT IN ('users', 'sessions')").fetchall()
                return self.send_json([t['name'] for t in tables])

            m_table = re.match(r'^/api/tables/([^/]+)$', path)
            if m_table and method == 'GET':
                table_name = m_table.group(1)
                # Seguridad básica: validar que el nombre de la tabla existe en sqlite_master
                exists = self.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone()
                if not exists:
                    return self.send_err("Tabla no encontrada", 404)
                
                # Obtener columnas
                columns_info = self.db.execute(f"PRAGMA table_info({table_name})").fetchall()
                columns = [c['name'] for c in columns_info]
                
                # Obtener filas
                rows = self.db.execute(f"SELECT * FROM {table_name} LIMIT 500").fetchall()
                return self.send_json({
                    'table': table_name,
                    'columns': columns,
                    'rows': rows_to_list(rows)
                })

            self.route(method)
        finally:
            self.db.close()

    def handle_login(self):
        ip = self.client_address[0]

        # Comprobar rate limit antes de validar credenciales
        blocked, remaining = check_rate_limit(ip)
        if blocked:
            mins = max(remaining // 60, 1)
            return self.send_err(
                f'Demasiados intentos fallidos. Espera {mins} minuto{"s" if mins != 1 else ""} e inténtalo de nuevo.',
                429
            )

        body     = self.read_body()
        username = body.get('username')
        password = body.get('password')

        if USER_CREDENTIALS.get(username) == password:
            reset_login_attempts(ip)  # Login OK: resetear contador
            self.send_json({
                'token': AUTH_TOKEN,
                'user': {'username': username, 'role': 'marketing'}
            })
        else:
            record_failed_login(ip)   # Login fallido: sumar intento
            self.send_err('Usuario o contraseña incorrectos', 401)

    def route(self, method):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        qs     = parse_qs(parsed.query)
        print(f"🔍 API Request: {method} {path}")

        def path_id(base):
            m = re.match(rf'^{base}/(\d+)$', path)
            return int(m.group(1)) if m else None

        try:
            if path == '/api/version': return self.send_json({'version': '2.0.3_fixed_delete'})
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
            if path == '/api/posts/sync-views' and method == 'POST':
                return self.sync_all_views()
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
            if pid:
                if method == 'PUT':    return self.update_rpu(pid)
                if method == 'DELETE': return self.delete_rpu(pid)

            # ── /api/dashboard ──────────────────────────────
            if path == '/api/dashboard' and method == 'GET':
                return self.get_dashboard(qs)

            # ── DEBUG ───────────────────────────────────────
            if path == '/api/debug/fk' and method == 'GET':
                rows = self.db.execute("SELECT name, sql FROM sqlite_master WHERE type='table'").fetchall()
                return self.send_json(rows_to_list(rows))

            self.send_err(f'Not found: {method} {path}', 404)

        except Exception as e:
            import traceback; traceback.print_exc()
            self.send_err(str(e), 500)

    # ── LIST ENDPOINTS ───────────────────────────────────────
    def get_lists(self):
        rows = self.db.execute("""
            SELECT lv.*, l.name as list_name FROM list_values lv
            JOIN lists l ON l.id = lv.list_id
            WHERE lv.is_active = 1
            ORDER BY l.name, lv.value
        """).fetchall()
        self.send_json(rows_to_list(rows))

    def get_list_values(self, list_name=None):
        if list_name:
            rows = self.db.execute("""
                SELECT lv.*, l.name as list_name FROM list_values lv
                JOIN lists l ON l.id = lv.list_id
                WHERE l.name = ? AND lv.is_active = 1
                ORDER BY lv.value
            """, (list_name,)).fetchall()
        else:
            rows = self.db.execute("""
                SELECT lv.*, l.name as list_name FROM list_values lv
                JOIN lists l ON l.id = lv.list_id
                WHERE lv.is_active = 1
                ORDER BY l.name, lv.value
            """).fetchall()
        self.send_json(rows_to_list(rows))

    def create_list_value(self):
        body = self.read_body()
        cur = self.db.execute(
            "INSERT INTO list_values(list_id,value) VALUES(?,?)",
            (body.get('list_id'), body.get('value'))
        )
        self.db.commit()
        row = self.db.execute("SELECT lv.*,l.name as list_name FROM list_values lv JOIN lists l ON l.id=lv.list_id WHERE lv.id=?", (cur.lastrowid,)).fetchone()
        self.send_json(dict(row), 201)

    def update_list_value(self, lv_id):
        body = self.read_body()
        self.db.execute("UPDATE list_values SET value=?,is_active=? WHERE id=?",
                   (body.get('value'), body.get('is_active', 1), lv_id))
        self.db.commit()
        row = self.db.execute("SELECT lv.*,l.name as list_name FROM list_values lv JOIN lists l ON l.id=lv.list_id WHERE lv.id=?", (lv_id,)).fetchone()
        self.send_json(dict(row))

    def delete_list_value(self, lv_id):
        self.db.execute("UPDATE list_values SET is_active=0 WHERE id=?", (lv_id,))
        self.db.commit()
        self.send_json({'deleted': lv_id})

    # ── AMBASSADOR ENDPOINTS ─────────────────────────────────
    def get_ambassadors(self, qs={}):
        sql = """
            SELECT a.id, a.email, a.first_name, a.last_name, a.primary_language_id, a.country_id, a.created_at,
              lv_lang.value  AS language,
              lv_lang.value   AS language_code,
              lv_country.value AS country,
              lv_country.value  AS country_value,
              (SELECT COUNT(*) FROM profiles p WHERE p.ambassador_id = a.id) AS profile_count,
              (SELECT status_id FROM contracts c JOIN profiles p2 ON p2.id=c.profile_id
               WHERE p2.ambassador_id=a.id ORDER BY c.created_at DESC LIMIT 1) AS latest_contract_status_id,
              (SELECT lv.value FROM contracts c
               JOIN profiles p2 ON p2.id=c.profile_id
               JOIN list_values lv ON lv.id=c.status_id
               WHERE p2.ambassador_id=a.id ORDER BY c.created_at DESC LIMIT 1) AS latest_contract_status
            FROM ambassadors a
            LEFT JOIN list_values lv_lang    ON lv_lang.id    = a.primary_language_id
            LEFT JOIN list_values lv_country ON lv_country.id = a.country_id
        """
        params = []
        where  = []
        if qs.get('country_value'):
            where.append('lv_country.value = ?'); params.append(qs['country_value'][0])
        if qs.get('platform_value'):
            where.append('EXISTS (SELECT 1 FROM profiles pf JOIN list_values lv_p ON lv_p.id=pf.platform_id WHERE pf.ambassador_id=a.id AND lv_p.value=?)')
            params.append(qs['platform_value'][0])
        if qs.get('niche_value'):
            where.append('EXISTS (SELECT 1 FROM profiles pf JOIN list_values lv_n ON lv_n.id=pf.niche_id WHERE pf.ambassador_id=a.id AND lv_n.value=?)')
            params.append(qs['niche_value'][0])
        if qs.get('status_value'):
            where.append("""
                (SELECT lv_s.value FROM contracts c 
                 JOIN profiles p2 ON p2.id=c.profile_id 
                 JOIN list_values lv_s ON lv_s.id=c.status_id 
                 WHERE p2.ambassador_id=a.id ORDER BY c.created_at DESC LIMIT 1) = ?
            """)
            params.append(qs['status_value'][0])
        if qs.get('search'):
            where.append("(a.first_name || ' ' || COALESCE(a.last_name,'') || ' ' || a.email LIKE ?)")
            params.append(f'%{qs["search"][0]}%')
        
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' ORDER BY a.first_name'
        rows = self.db.execute(sql, params).fetchall()
        data = rows_to_list(rows)
        print(f"✅ Ambassadors found: {len(data)}")
        self.send_json(data)

    def get_ambassador(self, aid):
        row = self.db.execute("""
            SELECT a.id, a.email, a.first_name, a.last_name, a.primary_language_id, a.country_id, a.created_at,
              lv_lang.value  AS language, lv_lang.value AS language_code,
              lv_country.value AS country, lv_country.value AS country_value
            FROM ambassadors a
            LEFT JOIN list_values lv_lang    ON lv_lang.id    = a.primary_language_id
            LEFT JOIN list_values lv_country ON lv_country.id = a.country_id
            WHERE a.id = ?
        """, (aid,)).fetchone()
        if not row: return self.send_err('Not found', 404)
        self.send_json(dict(row))

    def create_ambassador(self):
        body = self.read_body()
        cur = self.db.execute(
            "INSERT INTO ambassadors(email,first_name,last_name,primary_language_id,country_id) VALUES(?,?,?,?,?)",
            (body.get('email'), body.get('first_name'), body.get('last_name'),
             body.get('primary_language_id'), body.get('country_id'))
        )
        self.db.commit()
        row = self.db.execute("SELECT id, email, first_name, last_name, primary_language_id, country_id, created_at FROM ambassadors WHERE id=?", (cur.lastrowid,)).fetchone()
        self.send_json(dict(row), 201)

    def update_ambassador(self, aid):
        body = self.read_body()
        self.db.execute("""UPDATE ambassadors SET email=?,first_name=?,last_name=?,
                      primary_language_id=?,country_id=? WHERE id=?""",
                   (body.get('email'), body.get('first_name'), body.get('last_name'),
                    body.get('primary_language_id'), body.get('country_id'), aid))
        self.db.commit()
        row = self.db.execute("SELECT id, email, first_name, last_name, primary_language_id, country_id, created_at FROM ambassadors WHERE id=?", (aid,)).fetchone()
        self.send_json(dict(row))

    def delete_ambassador(self, aid):
        try:
            self.db.execute("PRAGMA foreign_keys = OFF")
            self.db.execute("""DELETE FROM daily_views WHERE post_id IN 
                          (SELECT id FROM posts WHERE profile_id IN 
                          (SELECT id FROM profiles WHERE ambassador_id=?))""", (aid,))
            self.db.execute("""DELETE FROM posts WHERE profile_id IN 
                          (SELECT id FROM profiles WHERE ambassador_id=?)""", (aid,))
            self.db.execute("""DELETE FROM contracts WHERE profile_id IN 
                          (SELECT id FROM profiles WHERE ambassador_id=?)""", (aid,))
            self.db.execute("""DELETE FROM profile_analyses WHERE profile_id IN 
                          (SELECT id FROM profiles WHERE ambassador_id=?)""", (aid,))
            self.db.execute("DELETE FROM profiles WHERE ambassador_id=?", (aid,))
            self.db.execute("DELETE FROM ambassadors WHERE id=?", (aid,))
            self.db.commit()
            self.db.execute("PRAGMA foreign_keys = ON")
            self.send_json({'deleted': aid})
        except Exception as e:
            self.db.rollback()
            self.send_err(str(e), 500)

    # ── PROFILE ENDPOINTS ────────────────────────────────────
    def get_profiles(self, qs={}):
        sql = """
            SELECT p.*,
              a.first_name || ' ' || COALESCE(a.last_name,'') AS ambassador_name,
              lv_plat.value AS platform, lv_plat.value AS platform_value,
              lv_niche.value AS niche,   lv_niche.value AS niche_value,
              (SELECT pa.expected_views FROM profile_analyses pa WHERE pa.profile_id=p.id
               ORDER BY pa.created_at DESC LIMIT 1) AS expected_views,
              (SELECT pa.content_target_score FROM profile_analyses pa WHERE pa.profile_id=p.id
               ORDER BY pa.created_at DESC LIMIT 1) AS content_score,
              (SELECT SUM(dv.new_views) FROM daily_views dv
               JOIN posts po ON po.id=dv.post_id WHERE po.profile_id=p.id) AS total_views
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
        if qs.get('platform_value'):
            where.append('lv_plat.value=?'); params.append(qs['platform_value'][0])
        if qs.get('niche_id'):
            where.append('p.niche_id=?'); params.append(qs['niche_id'][0])
        if qs.get('niche_value'):
            where.append('lv_niche.value=?'); params.append(qs['niche_value'][0])
        if qs.get('country_value'):
            where.append('a.country_id = (SELECT id FROM list_values WHERE UPPER(value)=UPPER(?))')
            params.append(qs['country_value'][0])
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' ORDER BY a.first_name, p.id'
        rows = self.db.execute(sql, params).fetchall()
        self.send_json(rows_to_list(rows))

    def get_profile(self, pid):
        row = self.db.execute("""
            SELECT p.*,
              a.first_name || ' ' || COALESCE(a.last_name,'') AS ambassador_name,
              lv_plat.value AS platform, lv_plat.value AS platform_value,
              lv_niche.value AS niche, lv_niche.value AS niche_value
            FROM profiles p
            JOIN ambassadors a ON a.id=p.ambassador_id
            LEFT JOIN list_values lv_plat  ON lv_plat.id  = p.platform_id
            LEFT JOIN list_values lv_niche ON lv_niche.id = p.niche_id
            WHERE p.id=?
        """, (pid,)).fetchone()
        if not row: return self.send_err('Not found', 404)
        self.send_json(dict(row))

    def create_profile(self):
        body = self.read_body()
        url = body.get('url')
        if url:
            exists = self.db.execute("SELECT id FROM profiles WHERE url=?", (url,)).fetchone()
            if exists:
                return self.send_err('Ya existe un canal con esta URL', 400)
                
        cur = self.db.execute(
            "INSERT INTO profiles(ambassador_id,platform_id,handle,url,niche_id) VALUES(?,?,?,?,?)",
            (body.get('ambassador_id'), body.get('platform_id'),
             body.get('handle'), url, body.get('niche_id'))
        )
        self.db.commit()
        row = self.db.execute("SELECT * FROM profiles WHERE id=?", (cur.lastrowid,)).fetchone()
        self.send_json(dict(row), 201)

    def update_profile(self, pid):
        body = self.read_body()
        self.db.execute("UPDATE profiles SET platform_id=?,handle=?,url=?,niche_id=? WHERE id=?",
                   (body.get('platform_id'), body.get('handle'), body.get('url'),
                    body.get('niche_id'), pid))
        self.db.commit()
        row = self.db.execute("SELECT * FROM profiles WHERE id=?", (pid,)).fetchone()
        self.send_json(dict(row))

    def delete_profile(self, pid):
        try:
            self.db.execute("PRAGMA foreign_keys = OFF")
            self.db.execute("DELETE FROM daily_views WHERE post_id IN (SELECT id FROM posts WHERE profile_id=?)", (pid,))
            self.db.execute("DELETE FROM posts WHERE profile_id=?", (pid,))
            self.db.execute("DELETE FROM contracts WHERE profile_id=?", (pid,))
            self.db.execute("DELETE FROM profile_analyses WHERE profile_id=?", (pid,))
            self.db.execute("DELETE FROM profiles WHERE id=?", (pid,))
            self.db.commit()
            self.db.execute("PRAGMA foreign_keys = ON")
            self.send_json({'deleted': pid})
        except Exception as e:
            self.db.rollback()
            self.send_err(str(e), 500)

    # ── PROFILE ANALYSES ─────────────────────────────────────
    def get_profile_analyses(self, qs={}):
        sql = "SELECT * FROM profile_analyses"
        params = []
        if qs.get('profile_id'):
            sql += ' WHERE profile_id=?'; params.append(qs['profile_id'][0])
        sql += ' ORDER BY created_at DESC'
        rows = self.db.execute(sql, params).fetchall()
        self.send_json(rows_to_list(rows))

    def create_profile_analysis(self):
        body = self.read_body()
        cur = self.db.execute("""
            INSERT INTO profile_analyses(profile_id,expected_views,total_30d_posts,
              cache_score,content_target_score,country_target_score)
            VALUES(?,?,?,?,?,?)""",
            (body.get('profile_id'), body.get('expected_views',0),
             body.get('total_30d_posts',0), body.get('cache_score'),
             body.get('content_target_score'), body.get('country_target_score'))
        )
        self.db.commit()
        row = self.db.execute("SELECT * FROM profile_analyses WHERE id=?", (cur.lastrowid,)).fetchone()
        self.send_json(dict(row), 201)

    # ── CONTRACT ENDPOINTS ───────────────────────────────────
    def get_contracts(self, qs={}):
        sql = """
            SELECT c.*,
              lv_st.value AS status, lv_st.value AS status_value,
              lv_cur.value AS currency, lv_cur.value AS currency_value,
              p.handle, p.ambassador_id,
              a.first_name || ' ' || COALESCE(a.last_name,'') AS ambassador_name,
              lv_plat.value AS platform, lv_plat.value AS platform_value,
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
        if qs.get('status_value'):
            where.append('lv_st.value=?'); params.append(qs['status_value'][0])
        if qs.get('country_value'):
            where.append('a.country_id = (SELECT id FROM list_values WHERE UPPER(value)=UPPER(?))')
            params.append(qs['country_value'][0])
        if qs.get('niche_value'):
            where.append('p.niche_id = (SELECT id FROM list_values WHERE UPPER(value)=UPPER(?))')
            params.append(qs['niche_value'][0])
        if qs.get('platform_value'):
            where.append('lv_plat.value=?'); params.append(qs['platform_value'][0])
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' ORDER BY c.created_at DESC'
        rows = self.db.execute(sql, params).fetchall()
        self.send_json(rows_to_list(rows))

    def get_contract(self, cid):
        row = self.db.execute("""
            SELECT c.*, lv_st.value AS status, lv_st.value AS status_value,
              lv_cur.value AS currency, lv_cur.value AS currency_value,
              p.handle, p.ambassador_id
            FROM contracts c
            JOIN profiles p ON p.id=c.profile_id
            LEFT JOIN list_values lv_st  ON lv_st.id  = c.status_id
            LEFT JOIN list_values lv_cur ON lv_cur.id = c.currency_id
            WHERE c.id=?
        """, (cid,)).fetchone()
        if not row: return self.send_err('Not found', 404)
        self.send_json(dict(row))

    def create_contract(self):
        body = self.read_body()
        cur = self.db.execute("""
            INSERT INTO contracts(profile_id,status_id,currency_id,
              price_per_standard_post,price_per_top_post,
              monthly_standard_posts,monthly_top_posts,signing_at,end_at,contract_file_url,notes)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (body.get('profile_id'), body.get('status_id'), body.get('currency_id'),
             body.get('price_per_standard_post'), body.get('price_per_top_post'),
             body.get('monthly_standard_posts',0), body.get('monthly_top_posts',0),
             body.get('signing_at'), body.get('end_at'), body.get('contract_file_url'),
             body.get('notes'))
        )
        self.db.commit()
        row = self.db.execute("SELECT * FROM contracts WHERE id=?", (cur.lastrowid,)).fetchone()
        self.send_json(dict(row), 201)

    def update_contract(self, cid):
        body = self.read_body()
        print(f"[CONTRACT UPDATE] id={cid} notes={repr(body.get('notes'))} body_keys={list(body.keys())}", flush=True)
        self.db.execute("""UPDATE contracts SET status_id=?,currency_id=?,
              price_per_standard_post=?,price_per_top_post=?,
              monthly_standard_posts=?,monthly_top_posts=?,
              signing_at=?,end_at=?,contract_file_url=?,notes=? WHERE id=?""",
            (body.get('status_id'), body.get('currency_id'),
             body.get('price_per_standard_post'), body.get('price_per_top_post'),
             body.get('monthly_standard_posts',0), body.get('monthly_top_posts',0),
             body.get('signing_at'), body.get('end_at'), body.get('contract_file_url'),
             body.get('notes'), cid))
        self.db.commit()
        row = self.db.execute("SELECT * FROM contracts WHERE id=?", (cid,)).fetchone()
        result = dict(row)
        print(f"[CONTRACT UPDATE] saved notes={repr(result.get('notes'))}", flush=True)
        self.send_json(result)

    def delete_contract(self, cid):
        try:
            self.db.execute("DELETE FROM contracts WHERE id=?", (cid,))
            self.db.commit()
            self.send_json({'deleted': cid})
        except Exception as e:
            self.db.rollback()
            self.send_err(str(e), 500)

    # ── POST ENDPOINTS ───────────────────────────────────────
    def get_posts(self, qs={}):
        sql = """
            SELECT po.*,
              lv_mt.value AS mention_type, lv_mt.value AS mention_type_value,
              p.handle, p.ambassador_id,
              a.first_name || ' ' || COALESCE(a.last_name,'') AS ambassador_name,
              lv_plat.value AS platform, lv_plat.value AS platform_value,
              COALESCE(SUM(dv.new_views),0) AS total_views
            FROM posts po
            JOIN profiles p ON p.id = po.profile_id
            JOIN ambassadors a ON a.id = p.ambassador_id
            LEFT JOIN list_values lv_mt   ON lv_mt.id   = po.mention_type_id
            LEFT JOIN list_values lv_plat ON lv_plat.id = p.platform_id
            LEFT JOIN daily_views dv ON dv.post_id = po.id
        """
        params = []
        where = []
        if qs.get('profile_id'):
            where.append('po.profile_id=?'); params.append(qs['profile_id'][0])
        if qs.get('ambassador_id'):
            where.append('p.ambassador_id=?'); params.append(qs['ambassador_id'][0])
        if qs.get('platform_value'):
            where.append('lv_plat.value=?'); params.append(qs['platform_value'][0])
        if qs.get('mention_type_value'):
            where.append('lv_mt.value=?'); params.append(qs['mention_type_value'][0])
        if qs.get('country_value'):
            where.append('a.country_id = (SELECT id FROM list_values WHERE UPPER(value)=UPPER(?))')
            params.append(qs['country_value'][0])
        if qs.get('niche_value'):
            where.append('p.niche_id = (SELECT id FROM list_values WHERE UPPER(value)=UPPER(?))')
            params.append(qs['niche_value'][0])
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' GROUP BY po.id ORDER BY po.published_at DESC'
        rows = self.db.execute(sql, params).fetchall()
        self.send_json(rows_to_list(rows))

    def get_post(self, pid):
        row = self.db.execute("""
            SELECT po.*, lv_mt.value AS mention_type, lv_mt.value AS mention_type_value,
              p.handle, p.ambassador_id,
              COALESCE(SUM(dv.new_views),0) AS total_views
            FROM posts po
            JOIN profiles p ON p.id=po.profile_id
            LEFT JOIN list_values lv_mt ON lv_mt.id=po.mention_type_id
            LEFT JOIN daily_views dv ON dv.post_id=po.id
            WHERE po.id=? GROUP BY po.id
        """, (pid,)).fetchone()
        if not row: return self.send_err('Not found', 404)
        self.send_json(dict(row))

    def create_post(self):
        body = self.read_body()
        # Usamos ON CONFLICT para actualizar si ya existe, manteniendo el mismo ID y las estadísticas
        cur = self.db.execute("""
            INSERT INTO posts(profile_id,url,mention_type_id,mention_offset,content_score,published_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(url) DO UPDATE SET
                mention_type_id=excluded.mention_type_id,
                mention_offset=excluded.mention_offset,
                content_score=excluded.content_score,
                published_at=excluded.published_at
        """, (body.get('profile_id'), body.get('url'), body.get('mention_type_id'),
              body.get('mention_offset',0), body.get('content_score'), body.get('published_at')))
        self.db.commit()
        # Buscamos el post por URL (única) para evitar errores con UPSERT + lastrowid
        row = self.db.execute("SELECT * FROM posts WHERE url=?", (body.get('url'),)).fetchone()
        if not row: return self.send_err('Error al recuperar el post guardado', 500)
        self.send_json(dict(row), 201)

    def update_post(self, pid):
        body = self.read_body()
        self.db.execute("""UPDATE posts SET url=?,mention_type_id=?,mention_offset=?,
              content_score=?,published_at=? WHERE id=?""",
            (body.get('url'), body.get('mention_type_id'), body.get('mention_offset',0),
             body.get('content_score'), body.get('published_at'), pid))
        self.db.commit()
        row = self.db.execute("SELECT * FROM posts WHERE id=?", (pid,)).fetchone()
        self.send_json(dict(row))

    def delete_post(self, pid):
        try:
            # Forzar limpieza de cualquier cosa que apunte al post
            self.db.execute("PRAGMA foreign_keys = OFF")
            self.db.execute("DELETE FROM daily_views WHERE post_id=?", (pid,))
            self.db.execute("DELETE FROM posts WHERE id=?", (pid,))
            self.db.commit()
            self.db.execute("PRAGMA foreign_keys = ON")
            self.send_json({'deleted': pid})
        except Exception as e:
            self.db.rollback()
            self.send_err(str(e), 500)
    # ── POST VIEWS ───────────────────────────────────────────
    def get_post_views(self, qs={}):
        sql = "SELECT * FROM daily_views"
        params = []
        if qs.get('post_id'):
            sql += ' WHERE post_id=?'; params.append(qs['post_id'][0])
        sql += ' ORDER BY views_date'
        rows = self.db.execute(sql, params).fetchall()
        self.send_json(rows_to_list(rows))

    def create_post_views(self):
        body = self.read_body()
        cur = self.db.execute(
            "INSERT OR REPLACE INTO daily_views(post_id,views_date,new_views) VALUES(?,?,?)",
            (body.get('post_id'), body.get('views_date'), body.get('new_views',0))
        )
        self.db.commit()
        row = self.db.execute("SELECT * FROM daily_views WHERE id=?", (cur.lastrowid,)).fetchone()
        self.send_json(dict(row), 201)

    # ── REVENUES ─────────────────────────────────────────────
    def get_revenues(self, qs={}):
        rows = self.db.execute("""
            SELECT r.*, lv_c.value AS country, lv_c.value AS country_value,
              lv_n.value AS niche, lv_n.value AS niche_value,
              lv_cur.value AS currency, lv_cur.value AS currency_value
            FROM revenues r
            LEFT JOIN list_values lv_c   ON lv_c.id   = r.country_id
            LEFT JOIN list_values lv_n   ON lv_n.id   = r.niche_id
            LEFT JOIN list_values lv_cur ON lv_cur.id = r.currency_id
            ORDER BY r.views_date DESC
        """).fetchall()
        self.send_json(rows_to_list(rows))

    def create_revenue(self):
        body = self.read_body()
        cur = self.db.execute(
            "INSERT INTO revenues(views_date,country_id,currency_id,new_revenue) VALUES(?,?,?,?)",
            (body.get('views_date'), body.get('country_id'), body.get('currency_id'), body.get('new_revenue', body.get('amount', 0)))
        )
        self.db.commit()
        self.send_json({'id': cur.lastrowid}, 201)

    def delete_revenue(self, rid):
        self.db.execute("DELETE FROM revenues WHERE id=?", (rid,))
        self.db.commit()
        self.send_json({'deleted': rid})

    # ── RPUS ─────────────────────────────────────────────────
    def get_rpus(self, qs={}):
        rows = self.db.execute("""
            SELECT r.*, lv_c.value AS country, lv_c.value AS country_value,
              lv_n.value AS niche, lv_n.value AS niche_value
            FROM rpus r
            LEFT JOIN list_values lv_c ON lv_c.id = r.country_id
            LEFT JOIN list_values lv_n ON lv_n.id = r.niche_id
            ORDER BY r.views_date DESC
        """).fetchall()
        self.send_json(rows_to_list(rows))

    def create_rpu(self):
        body = self.read_body()
        cur = self.db.execute(
            "INSERT OR REPLACE INTO rpus(views_date,country_id,niche_id,rpu) VALUES(?,?,?,?)",
            (body.get('views_date'), body.get('country_id'), body.get('niche_id'), body.get('rpu',0))
        )
        self.db.commit()
        row = self.db.execute("SELECT * FROM rpus WHERE id=?", (cur.lastrowid,)).fetchone()
        self.send_json(dict(row), 201)

    def delete_rpu(self, rid):
        self.db.execute("DELETE FROM rpus WHERE id=?", (rid,))
        self.db.commit()
        self.send_json({'deleted': rid})

    def update_rpu(self, rid):
        body = self.read_body()
        self.db.execute("""
            UPDATE rpus SET views_date=?, country_id=?, niche_id=?, rpu=?
            WHERE id=?
        """, (body.get('views_date'), body.get('country_id'), body.get('niche_id'), body.get('rpu',0), rid))
        self.db.commit()
        self.send_json({'updated': rid})

    # ── DASHBOARD (agregado) ─────────────────────────────────
    def get_dashboard(self, qs=None):
        where_parts = []
        params = []
        days = '30'
        
        # Recogemos filtros básicos
        if qs:
            print(f"📊 Dashboard Request Params: {qs}", flush=True)
            if qs.get('country_value') and qs['country_value'][0]:
                where_parts.append('a.country_id = (SELECT id FROM list_values WHERE UPPER(value)=UPPER(?))')
                params.append(qs['country_value'][0])
            if qs.get('niche_value') and qs['niche_value'][0]:
                where_parts.append('p.niche_id = (SELECT id FROM list_values WHERE UPPER(value)=UPPER(?))')
                params.append(qs['niche_value'][0])
            if qs.get('platform_value') and qs['platform_value'][0]:
                where_parts.append('p.platform_id = (SELECT id FROM list_values WHERE UPPER(value)=UPPER(?))')
                params.append(qs['platform_value'][0])
            if qs.get('ambassador_id'):
                where_parts.append('a.id = ?')
                params.append(qs['ambassador_id'][0])
            if qs.get('days'):
                days = qs['days'][0]

        # Base join robusta
        needs_p = any(k in qs for k in ['niche_value', 'platform_value']) if qs else False
        join_type = "JOIN" if needs_p else "LEFT JOIN"
        base_from = f"FROM ambassadors a {join_type} profiles p ON p.ambassador_id = a.id"
        
        # Helper para construir la cláusula WHERE de forma limpia
        def build_where(extra_conditions=[]):
            all_conds = where_parts + extra_conditions
            if not all_conds: return "", []
            return " WHERE " + " AND ".join(all_conds), params + [c[1] for c in extra_conditions if isinstance(c, tuple)]
        
        # 1. KPIs principales
        w_sql, w_params = build_where()
        res = self.db.execute(f"SELECT COUNT(DISTINCT a.id), COUNT(DISTINCT p.id) {base_from} {w_sql}", params).fetchone()
        total_ambassadors = res[0]
        total_profiles    = res[1]
        
        # Contratos firmados
        w_sql_s, _ = build_where(["lv_s.value='Firmado'"])
        signed_contracts = self.db.execute(f"""
            SELECT COUNT(DISTINCT c.id) {base_from}
            JOIN contracts c ON c.profile_id = p.id
            JOIN list_values lv_s ON lv_s.id = c.status_id
            {w_sql_s}
        """, params).fetchone()[0] or 0

        # Revenue Esperado (NUEVA FÓRMULA PERFORMANCE-BASED)
        w_sql_s, _ = build_where(["lv_s.value='Firmado'"])
        rows_perf = self.db.execute(f"""
            SELECT 
                c.monthly_standard_posts, c.monthly_top_posts,
                pa.expected_views, pa.cache_score, pa.content_target_score, pa.country_target_score,
                lv_plat.value AS platform_value,
                lv_country.value AS country_value
            {base_from}
            JOIN contracts c ON c.profile_id = p.id
            JOIN list_values lv_s ON lv_s.id = c.status_id
            LEFT JOIN list_values lv_plat ON lv_plat.id = p.platform_id
            LEFT JOIN list_values lv_country ON lv_country.id = a.country_id
            LEFT JOIN profile_analyses pa ON pa.id = COALESCE(c.last_analysis_id, 
                (SELECT id FROM profile_analyses WHERE profile_id=p.id ORDER BY created_at DESC LIMIT 1))
            {w_sql_s}
        """, params).fetchall()
        print(f"[Dashboard] Contratos firmados encontrados para revenue: {len(rows_perf)}")
        
        # Multiplicadores oficiales por nombre de país (como se almacena en la BD en español)
        COUNTRY_RPM_MULT = {
            'MEXICO': 1.00, 'MÉXICO': 1.00,
            'ESPAÑA': 1.33, 'SPAIN': 1.33,
            'COLOMBIA': 0.53, 'CHILE': 0.53,
            'BRASIL': 0.33, 'BRAZIL': 0.33,
            'ALEMANIA': 3.00, 'GERMANY': 3.00,
            'FRANCIA': 1.47, 'FRANCE': 1.47,
            'ITALIA': 2.67, 'ITALY': 2.67,
            'PORTUGAL': 0.72, 'PORTUGUÉS': 0.72,
            'REINO UNIDO': 3.00, 'UNITED KINGDOM': 3.00, 'UK': 3.00,
            'ESTADOS UNIDOS': 5.00, 'UNITED STATES': 5.00, 'US': 5.00,
            'AUSTRALIA': 3.67, 'CANADA': 3.67, 'CANADÁ': 3.67,
            'ARGENTINA': 0.40, 'IRLANDA': 3.00, 'IRELAND': 3.00,
            # Códigos ISO cortos (fallback por compatibilidad)
            'MX': 1.00, 'ES': 1.33, 'CO': 0.53, 'CL': 0.53, 'BR': 0.33,
            'DE': 3.00, 'FR': 1.47, 'IT': 2.67, 'PT': 0.72,
            'US2': 5.00, 'AU': 3.67, 'CA': 3.67, 'AR': 0.40, 'IE': 3.00
        }
        # Nombres/códigos LATAM para el multiplicador por defecto (0.40)
        LATAM_CODES_SET = {
            'AR', 'CO', 'CL', 'PE', 'EC', 'VE', 'UY', 'PY', 'BO', 'GT', 'HN', 'SV', 'NI', 'CR', 'PA', 'DO', 'PR',
            'ARGENTINA', 'COLOMBIA', 'CHILE', 'PERÚ', 'PERU', 'ECUADOR', 'VENEZUELA', 'URUGUAY',
            'PARAGUAY', 'BOLIVIA', 'GUATEMALA', 'HONDURAS', 'EL SALVADOR', 'NICARAGUA',
            'COSTA RICA', 'PANAMÁ', 'PANAMA', 'REPÚBLICA DOMINICANA', 'PUERTO RICO'
        }
        
        expected_revenue = 0
        for r in rows_perf:
            ev = r['expected_views'] or 0
            if ev == 0: continue
            
            # 1. Country Multiplier (usando código corto igual que el frontend)
            c_code = (r['country_value'] or '').upper()
            country_mult = COUNTRY_RPM_MULT.get(c_code)
            if country_mult is None:
                if c_code in LATAM_CODES_SET: country_mult = 0.40
                else: country_mult = 0.12  # Developing / Default
            
            # 2. Cache Multiplier — ENUM: LOW=0.8, MID=1.0, HIGH=1.2 o número
            c_val = r['cache_score']
            if c_val is None:
                cache_mult = 1.0
            elif isinstance(c_val, str):
                cache_mult = {'LOW': 0.8, 'MID': 1.0, 'HIGH': 1.2}.get(c_val.upper(), 1.0)
            else:
                cache_mult = float(c_val) if float(c_val) > 0 else 1.0
            
            # 3. Target Scores (Content & Country)
            cts = r['content_target_score'] or 1.0
            cots = r['country_target_score'] or 0.6
            cots_adj = min(cots / 0.6, 1.0)
            
            # Base logic: Views/1000 * 42 (Reference RPM)
            base_val_post = (ev / 1000.0) * 42.0 * country_mult * cache_mult * cts * cots_adj
            
            # 4. Platform/Type Multipliers
            p_code = (r['platform_value'] or '').lower()
            m_std = r['monthly_standard_posts'] or 0
            m_top = r['monthly_top_posts'] or 0
            
            if p_code == 'youtube':
                expected_revenue += (base_val_post * 2.5 * m_std) + (base_val_post * 4.0 * m_top)
            elif p_code == 'tiktok':
                expected_revenue += (base_val_post * 1.0 * (m_std + m_top))
            else:
                # Other post types -> 0 según especificación
                expected_revenue += 0

        # Revenue Real (Automático basado en visualizaciones reales y fórmula oficial)
        # Se calcula sobre TODOS los posts (igual que el frontend Analytics), no solo contratos Firmados
        real_conds = ["dv.views_date >= date('now', ?)"]
        w_sql_real, _ = build_where(real_conds)
        rows_real = self.db.execute(f"""
            SELECT 
                po.id AS post_id,
                SUM(dv.new_views) AS real_views,
                pa.cache_score, pa.content_target_score, pa.country_target_score,
                lv_plat.value AS platform_value,
                lv_country.value AS country_value,
                lv_mt.value AS mention_type_value
            {base_from}
            JOIN posts po ON po.profile_id = p.id
            JOIN daily_views dv ON dv.post_id = po.id
            LEFT JOIN list_values lv_plat ON lv_plat.id = p.platform_id
            LEFT JOIN list_values lv_country ON lv_country.id = a.country_id
            LEFT JOIN list_values lv_mt ON lv_mt.id = po.mention_type_id
            LEFT JOIN profile_analyses pa ON pa.id = 
                (SELECT id FROM profile_analyses WHERE profile_id=p.id ORDER BY created_at DESC LIMIT 1)
            {w_sql_real}
            GROUP BY po.id
        """, params + [f'-{days} days']).fetchall()

        real_revenue = 0
        for r in rows_real:
            rv = r['real_views'] or 0
            if rv <= 0: continue
            
            c_code = (r['country_value'] or '').upper()
            country_mult = COUNTRY_RPM_MULT.get(c_code)
            if country_mult is None:
                if c_code in LATAM_CODES_SET: country_mult = 0.40
                else: country_mult = 0.12
            
            c_val = r['cache_score']
            if c_val is None:
                cache_mult = 1.0
            elif isinstance(c_val, str):
                cache_mult = {'LOW': 0.8, 'MID': 1.0, 'HIGH': 1.2}.get(c_val.upper(), 1.0)
            else:
                cache_mult = float(c_val) if float(c_val) > 0 else 1.0
                
            cts = r['content_target_score'] or 1.0
            cots = r['country_target_score'] or 0.6
            cots_adj = min(cots / 0.6, 1.0)
            
            base_val_post = (rv / 1000.0) * 42.0 * country_mult * cache_mult * cts * cots_adj
            
            p_code = (r['platform_value'] or '').lower()
            mt_code = (r['mention_type_value'] or '').lower()
            
            if p_code == 'youtube':
                if 'organic' in mt_code or 'om' in mt_code:
                    real_revenue += base_val_post * 4.0
                else:
                    real_revenue += base_val_post * 2.5
            elif p_code == 'tiktok':
                real_revenue += base_val_post * 1.0
            else:
                real_revenue += 0
        
        # Views totales
        w_sql_v, _ = build_where()
        total_views = self.db.execute(f"""
            SELECT COALESCE(SUM(dv.new_views),0) {base_from}
            JOIN posts po ON po.profile_id = p.id
            JOIN daily_views dv ON dv.post_id = po.id
            {w_sql_v}
        """, params).fetchone()[0] or 0

        # 2. Tendencia de Views
        trend_conds = ["dv.views_date >= date('now', ?)"]
        w_sql_t, _ = build_where(trend_conds)
        # El parámetro de la fecha va al FINAL porque trend_conds se añade al final de where_parts
        trend_rows = self.db.execute(f"""
            SELECT dv.views_date, SUM(dv.new_views) AS views {base_from}
            JOIN posts po ON po.profile_id = p.id
            JOIN daily_views dv ON dv.post_id = po.id
            {w_sql_t}
            GROUP BY dv.views_date ORDER BY dv.views_date
        """, params + [f'-{days} days']).fetchall()
        trend = [dict(r) for r in trend_rows]

        # 3. Distribución por plataforma
        w_sql_p, _ = build_where()
        plat_rows = self.db.execute(f"""
            SELECT lv.value AS platform, COUNT(DISTINCT p.id) AS count {base_from}
            JOIN list_values lv ON lv.id = p.platform_id
            {w_sql_p}
            GROUP BY p.platform_id
        """, params).fetchall()
        platform_split = [dict(r) for r in plat_rows]

        # 4. Top Ambassadors (respetando filtros)
        w_sql_top, _ = build_where()
        top_rows = self.db.execute(f"""
            SELECT a.id, a.first_name || ' ' || COALESCE(a.last_name,'') AS name,
                   lv_c.value AS country_value,
                   (SELECT lv_p.value FROM profiles p2 JOIN list_values lv_p ON lv_p.id = p2.platform_id WHERE p2.ambassador_id = a.id LIMIT 1) AS platform_value,
                   (SELECT lv_s.value FROM contracts c2 JOIN list_values lv_s ON lv_s.id = c2.status_id WHERE c2.profile_id IN (SELECT id FROM profiles WHERE ambassador_id = a.id) ORDER BY c2.id DESC LIMIT 1) AS contract_status,
                   COALESCE(SUM(dv.new_views),0) AS total_views,
                   AVG(po.content_score) as avg_score
            {base_from}
            LEFT JOIN list_values lv_c ON lv_c.id = a.country_id
            LEFT JOIN posts po ON po.profile_id = p.id
            LEFT JOIN daily_views dv ON dv.post_id = po.id
            {w_sql_top}
            GROUP BY a.id ORDER BY total_views DESC LIMIT 5
        """, params).fetchall()
        top = [dict(r) for r in top_rows]
        self.send_json({
            'kpis': {
                'total_ambassadors': total_ambassadors,
                'total_profiles': total_profiles,
                'signed_contracts': signed_contracts,
                'total_views': total_views,
                'expected_revenue': round(expected_revenue, 2),
                'real_revenue': real_revenue
            },
            'views_trend': trend,
            'platform_split': platform_split,
            'top_ambassadors': top
        })

# ─────────────────────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()

    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 Server running on port {port}")
    server = ThreadedHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()
# Force redeploy - 2026-05-02T18:56:00

