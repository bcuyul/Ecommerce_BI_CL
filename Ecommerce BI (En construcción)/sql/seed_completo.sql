
SET client_encoding = 'UTF8';
SET timezone = 'America/Santiago';
CREATE SCHEMA IF NOT EXISTS warehouse;
DROP TABLE IF EXISTS alertas_operativas CASCADE;
DROP TABLE IF EXISTS eventos_negocio CASCADE;
DROP TABLE IF EXISTS resenas_producto CASCADE;
DROP TABLE IF EXISTS detalle_devoluciones CASCADE;
DROP TABLE IF EXISTS devoluciones CASCADE;
DROP TABLE IF EXISTS detalle_envios CASCADE;
DROP TABLE IF EXISTS envios CASCADE;
DROP TABLE IF EXISTS detalle_ordenes_compra CASCADE;
DROP TABLE IF EXISTS ordenes_compra CASCADE;
DROP TABLE IF EXISTS proveedores CASCADE;
DROP TABLE IF EXISTS movimientos_inventario CASCADE;
DROP TABLE IF EXISTS niveles_stock CASCADE;
DROP TABLE IF EXISTS almacenes CASCADE;
DROP TABLE IF EXISTS reembolsos CASCADE;
DROP TABLE IF EXISTS eventos_pago CASCADE;
DROP TABLE IF EXISTS pagos CASCADE;
DROP TABLE IF EXISTS promociones_aplicadas_pedido CASCADE;
DROP TABLE IF EXISTS historial_estado_pedidos CASCADE;
DROP TABLE IF EXISTS detalle_pedidos CASCADE;
DROP TABLE IF EXISTS pedidos CASCADE;
DROP TABLE IF EXISTS detalle_carritos CASCADE;
DROP TABLE IF EXISTS carritos CASCADE;
DROP TABLE IF EXISTS cupones CASCADE;
DROP TABLE IF EXISTS promociones CASCADE;
DROP TABLE IF EXISTS precios_producto CASCADE;
DROP TABLE IF EXISTS variantes_producto CASCADE;
DROP TABLE IF EXISTS productos CASCADE;
DROP TABLE IF EXISTS categorias CASCADE;
DROP TABLE IF EXISTS marcas CASCADE;
DROP TABLE IF EXISTS sesiones_cliente CASCADE;
DROP TABLE IF EXISTS direcciones_cliente CASCADE;
DROP TABLE IF EXISTS clientes CASCADE;

CREATE TABLE clientes (
    cliente_id          BIGSERIAL PRIMARY KEY,
    cliente_externo_id  VARCHAR(64) UNIQUE,
    correo              VARCHAR(255) NOT NULL UNIQUE,
    nombre              VARCHAR(100) NOT NULL,
    apellido            VARCHAR(100) NOT NULL,
    telefono            VARCHAR(30),
    segmento            VARCHAR(30) NOT NULL DEFAULT 'estandar' CHECK (segmento IN ('vip','premium','estandar','nuevo','inactivo')),
    estado              VARCHAR(20) NOT NULL DEFAULT 'activo' CHECK (estado IN ('activo','inactivo','bloqueado')),
    activo              BOOLEAN NOT NULL DEFAULT TRUE,
    eliminado_en        TIMESTAMPTZ,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actualizado_en      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE direcciones_cliente (
    direccion_id        BIGSERIAL PRIMARY KEY,
    cliente_id          BIGINT NOT NULL REFERENCES clientes(cliente_id) ON DELETE CASCADE,
    tipo_direccion      VARCHAR(20) NOT NULL DEFAULT 'envio' CHECK (tipo_direccion IN ('envio','facturacion','ambas')),
    destinatario        VARCHAR(200) NOT NULL,
    linea1              VARCHAR(255) NOT NULL,
    linea2              VARCHAR(255),
    ciudad              VARCHAR(100) NOT NULL,
    provincia           VARCHAR(100),
    codigo_postal       VARCHAR(20),
    codigo_pais         CHAR(2) NOT NULL DEFAULT 'CL',
    es_predeterminada   BOOLEAN NOT NULL DEFAULT FALSE,
    activo              BOOLEAN NOT NULL DEFAULT TRUE,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE sesiones_cliente (
    sesion_id           BIGSERIAL PRIMARY KEY,
    cliente_id          BIGINT REFERENCES clientes(cliente_id),
    canal               VARCHAR(30) NOT NULL DEFAULT 'web' CHECK (canal IN ('web','mobile','marketplace','call_center','api')),
    dispositivo         VARCHAR(50),
    origen_trafico      VARCHAR(50),
    iniciado_en         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finalizado_en       TIMESTAMPTZ,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE marcas (
    marca_id            BIGSERIAL PRIMARY KEY,
    nombre              VARCHAR(100) NOT NULL UNIQUE,
    activo              BOOLEAN NOT NULL DEFAULT TRUE,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE categorias (
    categoria_id        BIGSERIAL PRIMARY KEY,
    categoria_padre_id  BIGINT REFERENCES categorias(categoria_id),
    nombre              VARCHAR(100) NOT NULL,
    slug                VARCHAR(120) NOT NULL UNIQUE,
    activo              BOOLEAN NOT NULL DEFAULT TRUE,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE productos (
    producto_id         BIGSERIAL PRIMARY KEY,
    marca_id            BIGINT NOT NULL REFERENCES marcas(marca_id),
    categoria_id        BIGINT NOT NULL REFERENCES categorias(categoria_id),
    nombre              VARCHAR(255) NOT NULL,
    slug                VARCHAR(280) NOT NULL UNIQUE,
    descripcion         TEXT,
    estado_producto     VARCHAR(20) NOT NULL DEFAULT 'activo' CHECK (estado_producto IN ('activo','inactivo','borrador','descontinuado')),
    activo              BOOLEAN NOT NULL DEFAULT TRUE,
    eliminado_en        TIMESTAMPTZ,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actualizado_en      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE variantes_producto (
    variante_id         BIGSERIAL PRIMARY KEY,
    producto_id         BIGINT NOT NULL REFERENCES productos(producto_id),
    sku                 VARCHAR(100) NOT NULL UNIQUE,
    codigo_barras       VARCHAR(100),
    color               VARCHAR(50),
    talla               VARCHAR(20),
    atributos_json      JSONB DEFAULT '{}'::jsonb,
    precio_costo        NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (precio_costo >= 0),
    activa              BOOLEAN NOT NULL DEFAULT TRUE,
    eliminado_en        TIMESTAMPTZ,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actualizado_en      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE precios_producto (
    precio_id           BIGSERIAL PRIMARY KEY,
    variante_id         BIGINT NOT NULL REFERENCES variantes_producto(variante_id),
    codigo_moneda       CHAR(3) NOT NULL DEFAULT 'CLP',
    precio_lista        NUMERIC(12,2) NOT NULL CHECK (precio_lista >= 0),
    precio_oferta       NUMERIC(12,2) CHECK (precio_oferta >= 0),
    vigente_desde       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    vigente_hasta       TIMESTAMPTZ,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE promociones (
    promocion_id        BIGSERIAL PRIMARY KEY,
    nombre              VARCHAR(150) NOT NULL,
    tipo                VARCHAR(30) NOT NULL CHECK (tipo IN ('porcentaje','monto_fijo','envio_gratis','combo')),
    valor               NUMERIC(12,2) NOT NULL CHECK (valor >= 0),
    codigo_moneda       CHAR(3) DEFAULT 'CLP',
    activa              BOOLEAN NOT NULL DEFAULT TRUE,
    vigente_desde       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    vigente_hasta       TIMESTAMPTZ,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE cupones (
    cupon_id            BIGSERIAL PRIMARY KEY,
    codigo              VARCHAR(50) NOT NULL UNIQUE,
    promocion_id        BIGINT REFERENCES promociones(promocion_id),
    usos_maximos        INT DEFAULT 1,
    usos_actuales       INT NOT NULL DEFAULT 0,
    activo              BOOLEAN NOT NULL DEFAULT TRUE,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE carritos (
    carrito_id          BIGSERIAL PRIMARY KEY,
    cliente_id          BIGINT REFERENCES clientes(cliente_id),
    sesion_id           BIGINT REFERENCES sesiones_cliente(sesion_id),
    estado              VARCHAR(20) NOT NULL DEFAULT 'activo' CHECK (estado IN ('activo','convertido','abandonado','expirado')),
    codigo_moneda       CHAR(3) NOT NULL DEFAULT 'CLP',
    cupon_id            BIGINT REFERENCES cupones(cupon_id),
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actualizado_en      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE detalle_carritos (
    detalle_carrito_id  BIGSERIAL PRIMARY KEY,
    carrito_id          BIGINT NOT NULL REFERENCES carritos(carrito_id) ON DELETE CASCADE,
    variante_id         BIGINT NOT NULL REFERENCES variantes_producto(variante_id),
    cantidad            INT NOT NULL CHECK (cantidad > 0),
    precio_unitario     NUMERIC(12,2) NOT NULL CHECK (precio_unitario >= 0),
    agregado_en         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE pedidos (
    pedido_id               BIGSERIAL PRIMARY KEY,
    numero_pedido           VARCHAR(30) NOT NULL UNIQUE,
    cliente_id              BIGINT NOT NULL REFERENCES clientes(cliente_id),
    direccion_facturacion_id BIGINT REFERENCES direcciones_cliente(direccion_id),
    direccion_envio_id      BIGINT REFERENCES direcciones_cliente(direccion_id),
    canal_venta             VARCHAR(30) NOT NULL DEFAULT 'web' CHECK (canal_venta IN ('web','mobile','marketplace','call_center','api')),
    codigo_moneda           CHAR(3) NOT NULL DEFAULT 'CLP',
    estado_actual           VARCHAR(30) NOT NULL DEFAULT 'pendiente' CHECK (estado_actual IN ('pendiente','confirmado','pagado','preparando','empaquetado','enviado','entregado','cancelado','reembolsado')),
    subtotal                NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (subtotal >= 0),
    descuento_total         NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (descuento_total >= 0),
    impuesto_total          NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (impuesto_total >= 0),
    costo_envio             NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (costo_envio >= 0),
    total_final             NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (total_final >= 0),
    estado_pago             VARCHAR(20) NOT NULL DEFAULT 'pendiente' CHECK (estado_pago IN ('pendiente','parcial','pagado','reembolsado','fallido')),
    estado_fulfillment      VARCHAR(20) NOT NULL DEFAULT 'pendiente' CHECK (estado_fulfillment IN ('pendiente','preparando','listo','enviado','entregado','devuelto')),
    promocion_id            BIGINT REFERENCES promociones(promocion_id),
    cupon_id                BIGINT REFERENCES cupones(cupon_id),
    notas                   TEXT,
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cancelado_en            TIMESTAMPTZ,
    entregado_en            TIMESTAMPTZ
);

CREATE TABLE detalle_pedidos (
    detalle_pedido_id       BIGSERIAL PRIMARY KEY,
    pedido_id               BIGINT NOT NULL REFERENCES pedidos(pedido_id) ON DELETE CASCADE,
    variante_id             BIGINT NOT NULL REFERENCES variantes_producto(variante_id),
    nombre_producto_snapshot VARCHAR(255) NOT NULL,
    sku_snapshot            VARCHAR(100) NOT NULL,
    cantidad                INT NOT NULL CHECK (cantidad > 0),
    precio_unitario         NUMERIC(12,2) NOT NULL CHECK (precio_unitario >= 0),
    descuento_linea         NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (descuento_linea >= 0),
    impuesto_linea          NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (impuesto_linea >= 0),
    total_linea             NUMERIC(12,2) NOT NULL CHECK (total_linea >= 0),
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE historial_estado_pedidos (
    historial_estado_pedido_id BIGSERIAL PRIMARY KEY,
    pedido_id               BIGINT NOT NULL REFERENCES pedidos(pedido_id) ON DELETE CASCADE,
    estado_anterior         VARCHAR(30),
    estado_nuevo            VARCHAR(30) NOT NULL,
    cambiado_por            VARCHAR(100) DEFAULT 'sistema',
    codigo_motivo           VARCHAR(50),
    notas                   TEXT,
    cambiado_en             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE pagos (
    pago_id                 BIGSERIAL PRIMARY KEY,
    pedido_id               BIGINT NOT NULL REFERENCES pedidos(pedido_id),
    proveedor_pago          VARCHAR(50) NOT NULL,
    transaccion_externa_id  VARCHAR(100),
    metodo_pago             VARCHAR(30) NOT NULL CHECK (metodo_pago IN ('tarjeta_credito','tarjeta_debito','transferencia','efectivo','billetera_virtual','cuotas','criptomoneda')),
    codigo_moneda           CHAR(3) NOT NULL DEFAULT 'CLP',
    monto                   NUMERIC(12,2) NOT NULL CHECK (monto > 0),
    estado                  VARCHAR(20) NOT NULL DEFAULT 'iniciado' CHECK (estado IN ('iniciado','autorizado','capturado','fallido','cancelado','reembolsado')),
    cuotas                  SMALLINT DEFAULT 1,
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE eventos_pago (
    evento_pago_id          BIGSERIAL PRIMARY KEY,
    pago_id                 BIGINT NOT NULL REFERENCES pagos(pago_id) ON DELETE CASCADE,
    tipo_evento             VARCHAR(30) NOT NULL,
    estado_evento           VARCHAR(20) NOT NULL,
    monto_evento            NUMERIC(12,2) CHECK (monto_evento >= 0),
    payload_crudo           JSONB DEFAULT '{}'::jsonb,
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE reembolsos (
    reembolso_id            BIGSERIAL PRIMARY KEY,
    pago_id                 BIGINT NOT NULL REFERENCES pagos(pago_id),
    pedido_id               BIGINT NOT NULL REFERENCES pedidos(pedido_id),
    monto                   NUMERIC(12,2) NOT NULL CHECK (monto > 0),
    codigo_moneda           CHAR(3) NOT NULL DEFAULT 'CLP',
    codigo_motivo           VARCHAR(50),
    estado                  VARCHAR(20) NOT NULL DEFAULT 'pendiente' CHECK (estado IN ('pendiente','aprobado','procesado','rechazado')),
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    procesado_en            TIMESTAMPTZ
);

CREATE TABLE almacenes (
    almacen_id              BIGSERIAL PRIMARY KEY,
    codigo                  VARCHAR(20) NOT NULL UNIQUE,
    nombre                  VARCHAR(100) NOT NULL,
    ciudad                  VARCHAR(100),
    codigo_pais             CHAR(2) NOT NULL DEFAULT 'CL',
    activo                  BOOLEAN NOT NULL DEFAULT TRUE,
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE niveles_stock (
    nivel_stock_id          BIGSERIAL PRIMARY KEY,
    almacen_id              BIGINT NOT NULL REFERENCES almacenes(almacen_id),
    variante_id             BIGINT NOT NULL REFERENCES variantes_producto(variante_id),
    cantidad_fisica         INT NOT NULL DEFAULT 0 CHECK (cantidad_fisica >= 0),
    cantidad_reservada      INT NOT NULL DEFAULT 0 CHECK (cantidad_reservada >= 0),
    cantidad_disponible     INT GENERATED ALWAYS AS (cantidad_fisica - cantidad_reservada) STORED,
    punto_reorden           INT NOT NULL DEFAULT 10,
    actualizado_en          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (almacen_id, variante_id)
);

CREATE TABLE movimientos_inventario (
    movimiento_inventario_id BIGSERIAL PRIMARY KEY,
    almacen_id              BIGINT NOT NULL REFERENCES almacenes(almacen_id),
    variante_id             BIGINT NOT NULL REFERENCES variantes_producto(variante_id),
    tipo_movimiento         VARCHAR(30) NOT NULL CHECK (tipo_movimiento IN ('ingreso','reserva','liberacion','venta','devolucion','ajuste','traslado_entrada','traslado_salida','reposicion')),
    cantidad                INT NOT NULL,
    tipo_referencia         VARCHAR(30),
    referencia_id           BIGINT,
    notas                   TEXT,
    fecha_movimiento        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE proveedores (
    proveedor_id            BIGSERIAL PRIMARY KEY,
    nombre_proveedor        VARCHAR(200) NOT NULL,
    correo_contacto         VARCHAR(255),
    codigo_pais             CHAR(2) NOT NULL DEFAULT 'CL',
    activo                  BOOLEAN NOT NULL DEFAULT TRUE,
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE envios (
    envio_id                BIGSERIAL PRIMARY KEY,
    pedido_id               BIGINT NOT NULL REFERENCES pedidos(pedido_id),
    almacen_id              BIGINT NOT NULL REFERENCES almacenes(almacen_id),
    estado_envio            VARCHAR(30) NOT NULL DEFAULT 'pendiente' CHECK (estado_envio IN ('pendiente','preparando','despachado','en_camino','entregado','fallido','devuelto')),
    transportista           VARCHAR(100),
    numero_seguimiento      VARCHAR(100),
    costo_envio_real        NUMERIC(12,2) DEFAULT 0 CHECK (costo_envio_real >= 0),
    enviado_en              TIMESTAMPTZ,
    entregado_en            TIMESTAMPTZ,
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE eventos_negocio (
    evento_negocio_id       BIGSERIAL PRIMARY KEY,
    tipo_entidad            VARCHAR(50) NOT NULL,
    entidad_id              BIGINT,
    tipo_evento             VARCHAR(60) NOT NULL,
    payload                 JSONB DEFAULT '{}'::jsonb,
    procesado_olap          BOOLEAN NOT NULL DEFAULT FALSE,
    ocurrido_en             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE resenas_producto (
    resena_id               BIGSERIAL PRIMARY KEY,
    producto_id             BIGINT NOT NULL REFERENCES productos(producto_id),
    cliente_id              BIGINT NOT NULL REFERENCES clientes(cliente_id),
    calificacion            SMALLINT NOT NULL CHECK (calificacion BETWEEN 1 AND 5),
    comentario              TEXT,
    aprobada                BOOLEAN NOT NULL DEFAULT FALSE,
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE alertas_operativas (
    alerta_id               BIGSERIAL PRIMARY KEY,
    tipo_alerta             VARCHAR(60) NOT NULL,
    entidad                 VARCHAR(50),
    entidad_id              BIGINT,
    mensaje                 TEXT NOT NULL,
    nivel                   VARCHAR(10) NOT NULL DEFAULT 'warning' CHECK (nivel IN ('info','warning','critical')),
    resuelta                BOOLEAN NOT NULL DEFAULT FALSE,
    creado_en               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resuelto_en             TIMESTAMPTZ
);

-- Indices
CREATE INDEX idx_clientes_correo           ON clientes(correo);
CREATE INDEX idx_clientes_estado           ON clientes(estado);
CREATE INDEX idx_pedidos_cliente_id        ON pedidos(cliente_id);
CREATE INDEX idx_pedidos_estado_actual     ON pedidos(estado_actual);
CREATE INDEX idx_eventos_negocio_procesado ON eventos_negocio(procesado_olap) WHERE procesado_olap = FALSE;

-- Trigger Notify
CREATE OR REPLACE FUNCTION fn_notificar_evento_negocio() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('eventos_negocio', json_build_object('id', NEW.evento_negocio_id, 'tipo_entidad', NEW.tipo_entidad, 'entidad_id', NEW.entidad_id, 'tipo_evento', NEW.tipo_evento, 'payload', NEW.payload, 'ocurrido_en', NEW.ocurrido_en)::text);
    RETURN NEW;
END; $$;
CREATE TRIGGER trg_notificar_evento_negocio AFTER INSERT ON eventos_negocio FOR EACH ROW EXECUTE FUNCTION fn_notificar_evento_negocio();

-- =============================================================================
-- DATOS INICIALES (SEMILLA CHILENA)
-- =============================================================================

INSERT INTO almacenes (codigo, nombre, ciudad, codigo_pais) VALUES ('ALM-SCL', 'CD Santiago', 'Santiago', 'CL'), ('ALM-VLP', 'Deposito Valpo', 'Vina del Mar', 'CL'), ('ALM-CCP', 'Deposito BioBio', 'Concepcion', 'CL');
INSERT INTO marcas (nombre) VALUES ('TechPro'), ('FashionNow'), ('HomeStyle'), ('SportMax'), ('EcoVerde');
INSERT INTO categorias (nombre, slug) VALUES ('Tecnologia', 'tecnologia'), ('Ropa y Moda', 'ropa-y-moda');
INSERT INTO categorias (categoria_padre_id, nombre, slug) VALUES (1, 'Smartphones', 'smartphones'), (2, 'Remeras', 'remeras');
INSERT INTO proveedores (nombre_proveedor, correo_contacto, codigo_pais) VALUES ('Imp Del Sur', 'ventas@importadoradelsur.cl', 'CL'), ('TechDistrib', 'contacto@techdistrib.cl', 'CL');

DO $$
DECLARE nombres TEXT[] := ARRAY['Carlos','Maria','Juan','Laura','Diego','Ana']; apellidos TEXT[] := ARRAY['Garcia','Rodriguez','Lopez','Martinez']; i INT; nom TEXT; ape TEXT; cli_id BIGINT;
BEGIN
    FOR i IN 1..500 LOOP
        nom := nombres[1 + floor(random()*6)::int]; ape := apellidos[1 + floor(random()*4)::int];
        INSERT INTO clientes (cliente_externo_id, correo, nombre, apellido, telefono, creado_en) VALUES ('EXT-'||lpad(i::text,6,'0'), lower(nom)||'.'||lower(ape)||i||'@mail.com', nom, ape, '+569'||(10000000+floor(random()*89999999)::int)::text, NOW()-(random()*730)::int*INTERVAL '1 day') RETURNING cliente_id INTO cli_id;
        INSERT INTO direcciones_cliente (cliente_id, destinatario, linea1, ciudad, provincia, codigo_pais, es_predeterminada) VALUES (cli_id, nom||' '||ape, 'Calle '||i, (ARRAY['Santiago','Vina del Mar','Concepcion'])[1+floor(random()*3)::int], (ARRAY['Metropolitana','Valparaiso','Biobio'])[1+floor(random()*3)::int], 'CL', TRUE);
    END LOOP;
END $$;

DO $$
DECLARE i INT; prod_id BIGINT; var_id BIGINT; precio_base NUMERIC;
BEGIN
    FOR i IN 1..200 LOOP
        INSERT INTO productos (marca_id, categoria_id, nombre, slug) VALUES (1+floor(random()*5)::int, 3+floor(random()*2)::int, 'Producto '||i, 'prod-'||i||'-'||(random()*100)::int) RETURNING producto_id INTO prod_id;
        precio_base := 5000 + floor(random()*50000)::int;
        INSERT INTO variantes_producto (producto_id, sku, precio_costo) VALUES (prod_id, 'SKU-'||prod_id, round(precio_base*0.45,2)) RETURNING variante_id INTO var_id;
        INSERT INTO precios_producto (variante_id, precio_lista) VALUES (var_id, round(precio_base,2));
        INSERT INTO niveles_stock (almacen_id, variante_id, cantidad_fisica) VALUES (1+floor(random()*3)::int, var_id, 100);
    END LOOP;
END $$;

DO $$
DECLARE i INT; cli_id BIGINT; ped_id BIGINT; var_id BIGINT; pr NUMERIC; dt TIMESTAMPTZ;
BEGIN
    FOR i IN 1..2000 LOOP
        SELECT cliente_id INTO cli_id FROM clientes ORDER BY random() LIMIT 1; dt := NOW()-(random()*720)::int*INTERVAL '1 day';
        SELECT vp.variante_id, pp.precio_lista INTO var_id, pr FROM variantes_producto vp JOIN precios_producto pp ON pp.variante_id=vp.variante_id ORDER BY random() LIMIT 1;
        INSERT INTO pedidos (numero_pedido, cliente_id, subtotal, impuesto_total, total_final, estado_actual, creado_en) VALUES ('PED-'||i, cli_id, pr, pr*0.19, pr*1.19, 'entregado', dt) RETURNING pedido_id INTO ped_id;
        INSERT INTO detalle_pedidos (pedido_id, variante_id, nombre_producto_snapshot, sku_snapshot, cantidad, precio_unitario, total_linea) VALUES (ped_id, var_id, 'Snap', 'SKU', 1, pr, pr*1.19);
        INSERT INTO pagos (pedido_id, proveedor_pago, metodo_pago, monto, estado, creado_en) VALUES (ped_id, 'Transbank', 'tarjeta_credito', pr*1.19, 'capturado', dt);
        INSERT INTO eventos_negocio (tipo_entidad, entidad_id, tipo_evento, payload, ocurrido_en) VALUES ('pedido', ped_id, 'pedido_creado', jsonb_build_object('total',pr*1.19), dt);
    END LOOP;
END $$;


CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE warehouse.dim_fecha (
    fecha_sk SERIAL PRIMARY KEY, fecha DATE NOT NULL UNIQUE, anio SMALLINT, trimestre SMALLINT, mes SMALLINT, semana SMALLINT, dia_mes SMALLINT, dia_semana SMALLINT, dia_nombre VARCHAR(20), mes_nombre VARCHAR(20), es_fin_semana BOOLEAN, es_feriado BOOLEAN DEFAULT FALSE
);

INSERT INTO warehouse.dim_fecha (fecha, anio, trimestre, mes, semana, dia_mes, dia_semana, dia_nombre, mes_nombre, es_fin_semana)
SELECT d::date, EXTRACT(year FROM d), EXTRACT(quarter FROM d), EXTRACT(month FROM d), EXTRACT(week FROM d), EXTRACT(day FROM d), EXTRACT(dow FROM d), to_char(d, 'Day'), to_char(d, 'Month'), EXTRACT(dow FROM d) IN (0,6)
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, '1 day') g(d) ON CONFLICT DO NOTHING;

CREATE TABLE warehouse.dim_cliente (
    cliente_sk SERIAL PRIMARY KEY, cliente_id BIGINT NOT NULL, correo VARCHAR(255), nombre_completo VARCHAR(200), segmento VARCHAR(30), estado VARCHAR(20), codigo_pais CHAR(2), ciudad VARCHAR(100), region VARCHAR(100), valido_desde TIMESTAMPTZ DEFAULT NOW(), valido_hasta TIMESTAMPTZ, es_actual BOOLEAN DEFAULT TRUE
);

CREATE TABLE warehouse.dim_producto (
    producto_sk SERIAL PRIMARY KEY, producto_id BIGINT NOT NULL, nombre_producto VARCHAR(255), nombre_marca VARCHAR(100), nombre_categoria VARCHAR(100), categoria_padre VARCHAR(100), estado_producto VARCHAR(20), valido_desde TIMESTAMPTZ DEFAULT NOW(), valido_hasta TIMESTAMPTZ, es_actual BOOLEAN DEFAULT TRUE
);

CREATE TABLE warehouse.dim_variante (
    variante_sk SERIAL PRIMARY KEY, variante_id BIGINT NOT NULL, producto_sk INT REFERENCES warehouse.dim_producto(producto_sk), sku VARCHAR(100), color VARCHAR(50), talla VARCHAR(20), precio_lista_actual NUMERIC(12,2), precio_costo NUMERIC(12,2), activa BOOLEAN, valido_desde TIMESTAMPTZ DEFAULT NOW(), valido_hasta TIMESTAMPTZ, es_actual BOOLEAN DEFAULT TRUE
);

CREATE TABLE warehouse.dim_almacen (
    almacen_sk SERIAL PRIMARY KEY, almacen_id BIGINT NOT NULL, codigo VARCHAR(20), nombre VARCHAR(100), ciudad VARCHAR(100), codigo_pais CHAR(2)
);

CREATE TABLE warehouse.dim_metodo_pago (
    metodo_pago_sk SERIAL PRIMARY KEY, metodo_pago VARCHAR(30) UNIQUE, categoria VARCHAR(30)
);
INSERT INTO warehouse.dim_metodo_pago (metodo_pago, categoria) VALUES ('tarjeta_credito','tarjeta'),('tarjeta_debito','tarjeta'),('transferencia','bancario'),('efectivo','efectivo'),('billetera_virtual','digital'),('cuotas','digital'),('criptomoneda','digital') ON CONFLICT DO NOTHING;

CREATE TABLE warehouse.dim_estado_pedido (
    estado_pedido_sk SERIAL PRIMARY KEY, estado VARCHAR(30) UNIQUE, es_terminal BOOLEAN DEFAULT FALSE, es_positivo BOOLEAN DEFAULT TRUE
);
INSERT INTO warehouse.dim_estado_pedido (estado, es_terminal, es_positivo) VALUES ('pendiente',FALSE,TRUE),('confirmado',FALSE,TRUE),('pagado',FALSE,TRUE),('preparando',FALSE,TRUE),('empaquetado',FALSE,TRUE),('enviado',FALSE,TRUE),('entregado',TRUE,TRUE),('cancelado',TRUE,FALSE),('reembolsado',TRUE,FALSE) ON CONFLICT DO NOTHING;

CREATE TABLE warehouse.dim_canal ( canal_sk SERIAL PRIMARY KEY, canal VARCHAR(30) UNIQUE );
INSERT INTO warehouse.dim_canal (canal) VALUES ('web'),('mobile'),('marketplace'),('call_center'),('api') ON CONFLICT DO NOTHING;

CREATE TABLE warehouse.dim_promocion ( promocion_sk SERIAL PRIMARY KEY, promocion_id BIGINT, nombre VARCHAR(150), tipo VARCHAR(30) );

-- Facts
CREATE TABLE warehouse.fact_pedidos (
    fact_pedido_sk BIGSERIAL PRIMARY KEY, pedido_id BIGINT NOT NULL, numero_pedido VARCHAR(30), cliente_sk INT REFERENCES warehouse.dim_cliente(cliente_sk), fecha_sk INT REFERENCES warehouse.dim_fecha(fecha_sk), canal_sk INT REFERENCES warehouse.dim_canal(canal_sk), estado_pedido_sk INT REFERENCES warehouse.dim_estado_pedido(estado_pedido_sk), promocion_sk INT REFERENCES warehouse.dim_promocion(promocion_sk), subtotal NUMERIC(12,2), descuento_total NUMERIC(12,2), impuesto_total NUMERIC(12,2), costo_envio NUMERIC(12,2), total_final NUMERIC(12,2), num_items INT, estado_pago VARCHAR(20), estado_fulfillment VARCHAR(20), creado_en TIMESTAMPTZ, cargado_en TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE warehouse.fact_detalle_pedidos (
    fact_detalle_sk BIGSERIAL PRIMARY KEY, detalle_pedido_id BIGINT NOT NULL, pedido_id BIGINT NOT NULL, variante_sk INT REFERENCES warehouse.dim_variante(variante_sk), producto_sk INT REFERENCES warehouse.dim_producto(producto_sk), cliente_sk INT REFERENCES warehouse.dim_cliente(cliente_sk), fecha_sk INT REFERENCES warehouse.dim_fecha(fecha_sk), cantidad INT, precio_unitario NUMERIC(12,2), descuento_linea NUMERIC(12,2), impuesto_linea NUMERIC(12,2), total_linea NUMERIC(12,2), creado_en TIMESTAMPTZ, cargado_en TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE warehouse.fact_pagos (
    fact_pago_sk BIGSERIAL PRIMARY KEY, pago_id BIGINT NOT NULL, pedido_id BIGINT NOT NULL, cliente_sk INT REFERENCES warehouse.dim_cliente(cliente_sk), fecha_sk INT REFERENCES warehouse.dim_fecha(fecha_sk), metodo_pago_sk INT REFERENCES warehouse.dim_metodo_pago(metodo_pago_sk), monto NUMERIC(12,2), estado VARCHAR(20), proveedor_pago VARCHAR(50), creado_en TIMESTAMPTZ, cargado_en TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE warehouse.fact_movimientos_inventario (
    fact_mov_sk BIGSERIAL PRIMARY KEY, movimiento_id BIGINT NOT NULL, almacen_sk INT REFERENCES warehouse.dim_almacen(almacen_sk), variante_sk INT REFERENCES warehouse.dim_variante(variante_sk), fecha_sk INT REFERENCES warehouse.dim_fecha(fecha_sk), tipo_movimiento VARCHAR(30), cantidad INT, tipo_referencia VARCHAR(30), referencia_id BIGINT, fecha_movimiento TIMESTAMPTZ, cargado_en TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE warehouse.fact_envios (
    fact_envio_sk BIGSERIAL PRIMARY KEY, envio_id BIGINT NOT NULL, pedido_id BIGINT NOT NULL, almacen_sk INT REFERENCES warehouse.dim_almacen(almacen_sk), fecha_envio_sk INT REFERENCES warehouse.dim_fecha(fecha_sk), fecha_entrega_sk INT REFERENCES warehouse.dim_fecha(fecha_sk), estado_envio VARCHAR(30), transportista VARCHAR(100), dias_entrega INT, cargado_en TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE warehouse.fact_carritos (
    fact_carrito_sk BIGSERIAL PRIMARY KEY, carrito_id BIGINT NOT NULL, cliente_sk INT REFERENCES warehouse.dim_cliente(cliente_sk), fecha_sk INT REFERENCES warehouse.dim_fecha(fecha_sk), estado VARCHAR(20), num_items INT, valor_total NUMERIC(12,2), convertido BOOLEAN, cargado_en TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE warehouse.marcas_agua ( tabla_fuente VARCHAR(100) PRIMARY KEY, ultima_carga TIMESTAMPTZ DEFAULT '2020-01-01', actualizado_en TIMESTAMPTZ DEFAULT NOW() );
INSERT INTO warehouse.marcas_agua (tabla_fuente) VALUES ('pedidos'),('pagos'),('clientes'),('productos'),('variantes') ON CONFLICT DO NOTHING;

-- Views
CREATE OR REPLACE VIEW warehouse.vw_ventas_geograficas AS SELECT fp.*, df.fecha, dc.nombre_completo, dc.ciudad, dc.region, dca.canal, dep.estado AS estado_nombre FROM warehouse.fact_pedidos fp JOIN warehouse.dim_fecha df ON df.fecha_sk=fp.fecha_sk LEFT JOIN warehouse.dim_cliente dc ON dc.cliente_sk=fp.cliente_sk LEFT JOIN warehouse.dim_canal dca ON dca.canal_sk=fp.canal_sk LEFT JOIN warehouse.dim_estado_pedido dep ON dep.estado_pedido_sk=fp.estado_pedido_sk;
CREATE OR REPLACE VIEW warehouse.vw_clientes_360 AS SELECT dc.*, COUNT(fp.pedido_id) AS total_pedidos, SUM(fp.total_final) AS ltv FROM warehouse.dim_cliente dc LEFT JOIN warehouse.fact_pedidos fp ON fp.cliente_sk=dc.cliente_sk GROUP BY dc.cliente_sk, dc.cliente_id, dc.nombre_completo, dc.correo, dc.segmento, dc.estado, dc.ciudad, dc.region;

-- RBAC
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='pbi_executive') THEN CREATE ROLE pbi_executive WITH LOGIN PASSWORD 'exec_pbi_123'; END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='pbi_analytical') THEN CREATE ROLE pbi_analytical WITH LOGIN PASSWORD 'data_pbi_123'; END IF;
END $$;
GRANT USAGE ON SCHEMA warehouse TO pbi_executive, pbi_analytical;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO pbi_analytical;
GRANT SELECT ON warehouse.vw_ventas_geograficas TO pbi_executive;

GRANT ALL ON ALL TABLES IN SCHEMA public TO ecommerce;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO ecommerce;
GRANT ALL ON ALL TABLES IN SCHEMA warehouse TO ecommerce;
GRANT ALL ON ALL SEQUENCES IN SCHEMA warehouse TO ecommerce;
