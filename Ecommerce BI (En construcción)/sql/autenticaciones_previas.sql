-- =========================================================
-- CAPA DE AUTENTICACIÓN DE APLICACIÓN (APP)
-- ---------------------------------------------------------
-- Este script crea usuarios/perfiles internos para la app BI.
-- NO crea roles nativos de PostgreSQL ni reemplaza los GRANT
-- definidos en seed_completo.sql para acceso a vistas/tablas.
-- La autorización a nivel base de datos se gestiona por
-- separado con roles como pbi_executive y pbi_analytical.
-- =========================================================


CREATE SCHEMA IF NOT EXISTS internal;

CREATE TABLE IF NOT EXISTS internal.usuarios (
    usuario_id          SERIAL PRIMARY KEY,
    username            VARCHAR(50) NOT NULL UNIQUE,
    password_hash       VARCHAR(128) NOT NULL, -- En producción usar bcrypt/argon2
    nombre_completo     VARCHAR(100),
    correo              VARCHAR(100),
    rol                 VARCHAR(20) NOT NULL CHECK (rol IN ('executive','management','operational','analytical')),
    area                VARCHAR(50),
    creado_en           TIMESTAMPTZ DEFAULT NOW(),
    ultimo_login        TIMESTAMPTZ
);

-- usuarios iniciales (Password para todos: dashboard123)
-- En un sistema real, no guardar en texto plano!!!!
INSERT INTO internal.usuarios (username, password_hash, nombre_completo, rol, area) VALUES
('pedro_exec',     'dashboard123', 'Pedro Valdivia',     'executive',    'Dirección General'),
('paula_mngr',     'dashboard123', 'Paula Jaraquemada', 'management',   'Gerencia Comercial'),
('juan_ops',       'dashboard123', 'Juan Pérez',        'operational',  'Logística y Almacén'),
('carla_data',     'dashboard123', 'Carla Troncoso',    'analytical',   'Business Intelligence')
ON CONFLICT (username) DO NOTHING;

-- Permisos para que el usuario 'ecommerce' pueda leer el schema
GRANT USAGE ON SCHEMA internal TO ecommerce;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA internal TO ecommerce;
