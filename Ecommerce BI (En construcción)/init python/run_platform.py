"""Simulador de plataforma ecommerce con motor OLTP (generador de eventos) y OLAP (stream processor).
Puedes usar cualquiera de estas opciones de ejecución para probar ambos motores o solo uno de ellos:
Uso:
  python run_platform.py                      # Modo produccion (ambos motores)
  python run_platform.py --solo-oltp          # Solo simulador OLTP
  python run_platform.py --solo-olap          # Solo procesador OLAP
  python run_platform.py --interval 3 8       # Intervalo OLTP custom
  python run_platform.py --max-pedidos 50000  # Limite de pedidos custom
"""

import asyncio
import json
import logging
import os
import random
import signal
import string
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("platform")
"Configuración de las bases de datos, puedes ajustar los parametros a una base tuya o usar las credenciales por defecto previo "
"a que hayas creado la base de datos ecommerce_cl"
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "ecommerce_cl"),
    "user":     os.getenv("DB_USER", "ecommerce"),
    "password": os.getenv("DB_PASS", "ecommerce123"),
}

# --- Parametros OLTP (simulador) ---
OLTP_INTERVAL_MIN = float(os.getenv("OLTP_INTERVAL_MIN", "5"))      # seg entre acciones
OLTP_INTERVAL_MAX = float(os.getenv("OLTP_INTERVAL_MAX", "10"))
OLTP_MAX_PEDIDOS  = int(os.getenv("OLTP_MAX_PEDIDOS", "100000"))     # limite produccion
OLTP_VACUUM_EVERY = int(os.getenv("OLTP_VACUUM_EVERY", "200"))       # ciclos entre VACUUM

# --- Parametros OLAP (stream processor) ---
OLAP_BATCH_SIZE    = int(os.getenv("OLAP_BATCH_SIZE", "100"))        # eventos por batch
OLAP_POLL_INTERVAL = float(os.getenv("OLAP_POLL_INTERVAL", "2"))     # seg entre polls
OLAP_HEALTH_EVERY  = int(os.getenv("OLAP_HEALTH_EVERY", "60"))       # seg entre health logs

# Ajuste de probabilidades de acciones OLTP (deben sumar 1.0)
PROB = {
    "nuevo_pedido":       0.30,
    "avanzar_estado":     0.25,
    "procesar_pago":      0.18,
    "cancelar_pedido":    0.05,
    "reponer_stock":      0.08,
    "devolucion":         0.04,
    "carrito_abandonado": 0.05,
    "actualizar_envio":   0.05,
}

REGIONES = ["Metropolitana", "Valparaiso", "Biobio", "Araucania", "Antofagasta",
            "Coquimbo", "Maule", "O'Higgins", "Los Lagos", "Tarapaca"]
COMUNAS  = ["Santiago", "Providencia", "Las Condes", "Nunoa", "Vitacura",
            "Vina del Mar", "Concepcion", "Temuco", "Antofagasta", "La Serena",
            "Rancagua", "Talca", "Puerto Montt", "Iquique", "Maipu"]

ESTADOS_FLUJO    = ["pendiente", "confirmado", "pagado", "preparando",
                    "empaquetado", "enviado", "entregado"]
METODOS_PAGO     = ["tarjeta_credito", "tarjeta_debito", "transferencia",
                    "billetera_virtual", "cuotas"]
PROVEEDORES_PAGO = ["Transbank", "MercadoPago", "Flow", "Khipu", "Kushki"]
TRANSPORTISTAS   = ["Chilexpress", "Starken", "BlueExpress", "Correos Chile", "Urbano"]
CANALES          = ["web", "web", "web", "mobile", "mobile", "marketplace"]

running = True
oltp_stats = {
    "pedidos": 0, "pagos": 0, "estados": 0, "cancelaciones": 0,
    "reposiciones": 0, "devoluciones": 0, "carritos": 0, "envios": 0,
    "ciclos": 0, "errores": 0,
}
olap_stats = {"procesados": 0, "errores": 0, "dim_upserts": 0, "facts_inserted": 0}


def _rand_str(length=8):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def _signal_handler(sig, frame):
    global running
    log.info("Senal de detencion recibida. Finalizando ambos motores...")
    running = False

signal.signal(signal.SIGINT, _signal_handler)



# OLTP

async def oltp_get_adaptive_interval(conn):
    """Incrementa el intervalo si las tablas crecen demasiado."""
    row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM pedidos")
    total = row["cnt"]
    if total > OLTP_MAX_PEDIDOS:
        return -1
    if total > 80000:
        return random.uniform(15, 25)
    elif total > 50000:
        return random.uniform(10, 18)
    elif total > 20000:
        return random.uniform(8, 14)
    else:
        return random.uniform(OLTP_INTERVAL_MIN, OLTP_INTERVAL_MAX)


async def oltp_emit_evento(conn, tipo_entidad, entidad_id, tipo_evento, payload):
    """Inserta evento de negocio (dispara pg_notify via trigger)."""
    await conn.execute("""
        INSERT INTO eventos_negocio (tipo_entidad, entidad_id, tipo_evento, payload)
        VALUES ($1, $2, $3, $4::jsonb)
    """, tipo_entidad, entidad_id, tipo_evento, json.dumps(payload))


# --- Acciones OLTP ---
async def accion_nuevo_pedido(conn):
    cli = await conn.fetchrow("SELECT cliente_id FROM clientes ORDER BY random() LIMIT 1")
    if not cli:
        return
    cli_id = cli["cliente_id"]

    dir_row = await conn.fetchrow(
        "SELECT direccion_id FROM direcciones_cliente WHERE cliente_id=$1 LIMIT 1", cli_id)
    if not dir_row:
        return
    dir_id = dir_row["direccion_id"]

    variantes = await conn.fetch("""
        SELECT vp.variante_id, pp.precio_lista, vp.sku,
               p.nombre || ' ' || COALESCE(vp.color,'') AS nombre_snap
        FROM variantes_producto vp
        JOIN precios_producto pp ON pp.variante_id = vp.variante_id
        JOIN productos p ON p.producto_id = vp.producto_id
        WHERE vp.activa = TRUE
        ORDER BY random() LIMIT 5
    """)
    if not variantes:
        return

    num_items = random.randint(1, min(4, len(variantes)))
    items = random.sample(list(variantes), num_items)

    subtotal = Decimal("0")
    items_data = []
    for v in items:
        cant = random.randint(1, 3)
        precio = Decimal(str(v["precio_lista"]))
        desc_l = Decimal("0")
        if random.random() > 0.8:
            desc_l = (precio * cant * Decimal("0.1")).quantize(Decimal("0.01"))
        imp_l = ((precio * cant - desc_l) * Decimal("0.19")).quantize(Decimal("0.01"))
        tot_l = (precio * cant - desc_l + imp_l).quantize(Decimal("0.01"))
        subtotal += precio * cant
        items_data.append({
            "variante_id": v["variante_id"], "nombre_snap": v["nombre_snap"],
            "sku": v["sku"], "cantidad": cant, "precio": precio,
            "descuento": desc_l, "impuesto": imp_l, "total": tot_l,
        })

    desc_tot = (subtotal * Decimal("0.05")).quantize(Decimal("0.01")) \
        if random.random() > 0.7 else Decimal("0")
    imp_tot = ((subtotal - desc_tot) * Decimal("0.19")).quantize(Decimal("0.01"))
    costo_envio = Decimal("0") if subtotal > Decimal("50000") else \
        Decimal(str(round(random.uniform(2990, 7990), 0)))
    total_final = (subtotal - desc_tot + imp_tot + costo_envio).quantize(Decimal("0.01"))

    numero = f"PED-{datetime.now().strftime('%Y%m')}-{_rand_str(6)}"
    canal = random.choice(CANALES)

    ped_id = await conn.fetchval("""
        INSERT INTO pedidos (
            numero_pedido, cliente_id, direccion_facturacion_id, direccion_envio_id,
            canal_venta, codigo_moneda, estado_actual,
            subtotal, descuento_total, impuesto_total, costo_envio, total_final,
            estado_pago, estado_fulfillment
        ) VALUES ($1,$2,$3,$4,$5,'CLP','pendiente',$6,$7,$8,$9,$10,'pendiente','pendiente')
        RETURNING pedido_id
    """, numero, cli_id, dir_id, dir_id, canal,
        subtotal, desc_tot, imp_tot, costo_envio, total_final)

    await conn.execute("""
        INSERT INTO historial_estado_pedidos (pedido_id, estado_anterior, estado_nuevo, cambiado_por)
        VALUES ($1, NULL, 'pendiente', 'simulator')
    """, ped_id)

    for it in items_data:
        await conn.execute("""
            INSERT INTO detalle_pedidos (
                pedido_id, variante_id, nombre_producto_snapshot, sku_snapshot,
                cantidad, precio_unitario, descuento_linea, impuesto_linea, total_linea
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        """, ped_id, it["variante_id"], it["nombre_snap"], it["sku"],
            it["cantidad"], it["precio"], it["descuento"], it["impuesto"], it["total"])

        alm_id = random.randint(1, 3)
        await conn.execute("""
            UPDATE niveles_stock SET
                cantidad_reservada = cantidad_reservada + $1, actualizado_en = NOW()
            WHERE almacen_id = $2 AND variante_id = $3
        """, it["cantidad"], alm_id, it["variante_id"])

    await oltp_emit_evento(conn, "pedido", ped_id, "pedido_creado", {
        "pedido_id": ped_id, "numero_pedido": numero,
        "cliente_id": cli_id, "total_final": float(total_final),
        "subtotal": float(subtotal), "impuesto_total": float(imp_tot),
        "descuento_total": float(desc_tot), "costo_envio": float(costo_envio),
        "canal": canal, "num_items": len(items_data),
        "creado_en": datetime.now(timezone.utc).isoformat(),
    })
    oltp_stats["pedidos"] += 1
    log.info(f"[OLTP][PEDIDO] #{numero} — CLP ${total_final:,.0f} ({len(items_data)} items)")


async def accion_avanzar_estado(conn):
    row = await conn.fetchrow("""
        SELECT pedido_id, estado_actual, total_final, cliente_id FROM pedidos
        WHERE estado_actual IN ('pendiente','confirmado','pagado','preparando','empaquetado','enviado')
        ORDER BY random() LIMIT 1
    """)
    if not row:
        return

    ped_id = row["pedido_id"]
    est_actual = row["estado_actual"]
    idx = ESTADOS_FLUJO.index(est_actual)
    if idx >= len(ESTADOS_FLUJO) - 1:
        return
    est_nuevo = ESTADOS_FLUJO[idx + 1]

    await conn.execute(
        "UPDATE pedidos SET estado_actual=$1, actualizado_en=NOW() WHERE pedido_id=$2",
        est_nuevo, ped_id)
    await conn.execute("""
        INSERT INTO historial_estado_pedidos (pedido_id, estado_anterior, estado_nuevo, cambiado_por)
        VALUES ($1, $2, $3, 'simulator')
    """, ped_id, est_actual, est_nuevo)

    if est_nuevo == "enviado":
        await conn.execute("""
            INSERT INTO envios (pedido_id, almacen_id, estado_envio, transportista, numero_seguimiento, enviado_en)
            VALUES ($1, $2, 'en_camino', $3, $4, NOW()) ON CONFLICT DO NOTHING
        """, ped_id, random.randint(1, 3), random.choice(TRANSPORTISTAS), f"TRACK-{_rand_str(10)}")
        await conn.execute("UPDATE pedidos SET estado_fulfillment='enviado' WHERE pedido_id=$1", ped_id)

    if est_nuevo == "entregado":
        await conn.execute("UPDATE pedidos SET entregado_en=NOW(), estado_fulfillment='entregado' WHERE pedido_id=$1", ped_id)
        await conn.execute("UPDATE envios SET estado_envio='entregado', entregado_en=NOW() WHERE pedido_id=$1", ped_id)

    await oltp_emit_evento(conn, "pedido", ped_id, "pedido_actualizado", {
        "pedido_id": ped_id, "estado_anterior": est_actual, "estado_nuevo": est_nuevo,
        "cliente_id": row["cliente_id"],
    })
    oltp_stats["estados"] += 1
    log.info(f"[OLTP][ESTADO] pedido {ped_id}: {est_actual} → {est_nuevo}")


async def accion_procesar_pago(conn):
    row = await conn.fetchrow("""
        SELECT pedido_id, total_final, cliente_id FROM pedidos
        WHERE estado_actual IN ('confirmado','pendiente') AND total_final > 0
        ORDER BY random() LIMIT 1
    """)
    if not row:
        return

    ped_id = row["pedido_id"]
    monto = float(row["total_final"])
    metodo = random.choice(METODOS_PAGO)
    proveedor = random.choice(PROVEEDORES_PAGO)
    exito = random.random() > 0.08

    estado_pago = "capturado" if exito else "fallido"
    pago_id = await conn.fetchval("""
        INSERT INTO pagos (pedido_id, proveedor_pago, transaccion_externa_id,
                           metodo_pago, codigo_moneda, monto, estado)
        VALUES ($1, $2, $3, $4, 'CLP', $5, $6)
        RETURNING pago_id
    """, ped_id, proveedor, f"TXN-{_rand_str(12)}", metodo, monto, estado_pago)

    for tipo_ev in ["iniciado", "autorizado"]:
        await conn.execute("""
            INSERT INTO eventos_pago (pago_id, tipo_evento, estado_evento, monto_evento)
            VALUES ($1, $2, 'ok', $3)
        """, pago_id, tipo_ev, monto)

    if exito:
        await conn.execute("""
            INSERT INTO eventos_pago (pago_id, tipo_evento, estado_evento, monto_evento)
            VALUES ($1, 'capturado', 'ok', $2)
        """, pago_id, monto)
        await conn.execute("""
            UPDATE pedidos SET estado_pago='pagado', estado_actual='pagado', actualizado_en=NOW()
            WHERE pedido_id=$1
        """, ped_id)
        await oltp_emit_evento(conn, "pago", pago_id, "pago_capturado", {
            "pago_id": pago_id, "pedido_id": ped_id, "monto": monto,
            "metodo": metodo, "proveedor": proveedor, "cliente_id": row["cliente_id"],
            "creado_en": datetime.now(timezone.utc).isoformat(),
        })
        log.info(f"[OLTP][PAGO] pedido {ped_id} — {metodo} CLP ${monto:,.0f} ✓")
    else:
        await conn.execute("""
            INSERT INTO eventos_pago (pago_id, tipo_evento, estado_evento, monto_evento)
            VALUES ($1, 'fallido', 'error', $2)
        """, pago_id, monto)
        await oltp_emit_evento(conn, "pago", pago_id, "pago_fallido", {
            "pago_id": pago_id, "pedido_id": ped_id, "monto": monto,
            "metodo": metodo, "proveedor": proveedor,
        })
        log.warning(f"[OLTP][PAGO] pedido {ped_id} — FALLIDO via {proveedor}")
    oltp_stats["pagos"] += 1


async def accion_cancelar_pedido(conn):
    row = await conn.fetchrow("""
        SELECT pedido_id, estado_actual FROM pedidos
        WHERE estado_actual IN ('pendiente','confirmado')
        ORDER BY random() LIMIT 1
    """)
    if not row:
        return
    ped_id = row["pedido_id"]
    await conn.execute("""
        UPDATE pedidos SET estado_actual='cancelado', cancelado_en=NOW(), actualizado_en=NOW()
        WHERE pedido_id=$1
    """, ped_id)
    await conn.execute("""
        INSERT INTO historial_estado_pedidos (pedido_id, estado_anterior, estado_nuevo, cambiado_por, codigo_motivo)
        VALUES ($1, $2, 'cancelado', 'simulator', 'cliente_solicitud')
    """, ped_id, row["estado_actual"])
    await oltp_emit_evento(conn, "pedido", ped_id, "pedido_cancelado", {
        "pedido_id": ped_id, "motivo": "cliente_solicitud",
    })
    oltp_stats["cancelaciones"] += 1
    log.info(f"[OLTP][CANCEL] pedido {ped_id} cancelado")


async def accion_reponer_stock(conn):
    row = await conn.fetchrow("""
        SELECT almacen_id, variante_id FROM niveles_stock
        WHERE cantidad_disponible <= punto_reorden
        ORDER BY cantidad_disponible ASC LIMIT 1
    """)
    if not row:
        row = await conn.fetchrow("SELECT almacen_id, variante_id FROM niveles_stock ORDER BY random() LIMIT 1")
    if not row:
        return
    cant = random.randint(20, 120)
    await conn.execute("""
        UPDATE niveles_stock SET cantidad_fisica = cantidad_fisica + $1, actualizado_en = NOW()
        WHERE almacen_id = $2 AND variante_id = $3
    """, cant, row["almacen_id"], row["variante_id"])
    mov_id = await conn.fetchval("""
        INSERT INTO movimientos_inventario (almacen_id, variante_id, tipo_movimiento, cantidad, tipo_referencia)
        VALUES ($1, $2, 'reposicion', $3, 'reposicion_auto')
        RETURNING movimiento_inventario_id
    """, row["almacen_id"], row["variante_id"], cant)
    await oltp_emit_evento(conn, "inventario", mov_id, "stock_repuesto", {
        "movimiento_id": mov_id, "almacen_id": row["almacen_id"],
        "variante_id": row["variante_id"], "cantidad": cant, "tipo": "reposicion",
        "fecha": datetime.now(timezone.utc).isoformat(),
    })
    oltp_stats["reposiciones"] += 1
    log.info(f"[OLTP][STOCK] variante {row['variante_id']} +{cant} en almacen {row['almacen_id']}")


async def accion_devolucion(conn):
    row = await conn.fetchrow("""
        SELECT p.pedido_id, dp.detalle_pedido_id, dp.variante_id, dp.cantidad, p.cliente_id
        FROM pedidos p JOIN detalle_pedidos dp ON dp.pedido_id = p.pedido_id
        WHERE p.estado_actual = 'entregado'
        ORDER BY random() LIMIT 1
    """)
    if not row:
        return
    dev_id = await conn.fetchval("""
        INSERT INTO devoluciones (pedido_id, motivo, estado) VALUES ($1, $2, 'aprobada')
        RETURNING devolucion_id
    """, row["pedido_id"],
        random.choice(["producto_defectuoso", "no_cumple_expectativa", "talla_incorrecta"]))
    cant_dev = random.randint(1, max(1, row["cantidad"]))
    alm_id = random.randint(1, 3)
    await conn.execute("""
        UPDATE niveles_stock SET cantidad_fisica = cantidad_fisica + $1, actualizado_en = NOW()
        WHERE almacen_id = $2 AND variante_id = $3
    """, cant_dev, alm_id, row["variante_id"])
    await oltp_emit_evento(conn, "devolucion", dev_id, "devolucion_creada", {
        "devolucion_id": dev_id, "pedido_id": row["pedido_id"],
        "variante_id": row["variante_id"], "cantidad": cant_dev, "cliente_id": row["cliente_id"],
    })
    oltp_stats["devoluciones"] += 1
    log.info(f"[OLTP][DEVOLUCION] pedido {row['pedido_id']} — {cant_dev} unidades")


async def accion_carrito_abandonado(conn):
    cli = await conn.fetchrow("SELECT cliente_id FROM clientes ORDER BY random() LIMIT 1")
    if not cli:
        return
    cart_id = await conn.fetchval("""
        INSERT INTO carritos (cliente_id, estado, codigo_moneda) VALUES ($1, 'abandonado', 'CLP')
        RETURNING carrito_id
    """, cli["cliente_id"])
    v = await conn.fetchrow("""
        SELECT vp.variante_id, pp.precio_lista FROM variantes_producto vp
        JOIN precios_producto pp ON pp.variante_id = vp.variante_id
        WHERE vp.activa = TRUE ORDER BY random() LIMIT 1
    """)
    if v:
        await conn.execute("""
            INSERT INTO detalle_carritos (carrito_id, variante_id, cantidad, precio_unitario)
            VALUES ($1, $2, $3, $4)
        """, cart_id, v["variante_id"], random.randint(1, 3), v["precio_lista"])
    oltp_stats["carritos"] += 1
    log.info(f"[OLTP][CARRITO] carrito {cart_id} abandonado")


async def accion_actualizar_envio(conn):
    row = await conn.fetchrow("""
        SELECT envio_id, pedido_id FROM envios WHERE estado_envio = 'en_camino'
        ORDER BY random() LIMIT 1
    """)
    if not row:
        return
    await conn.execute("UPDATE envios SET estado_envio='entregado', entregado_en=NOW() WHERE envio_id=$1", row["envio_id"])
    await conn.execute("""
        UPDATE pedidos SET estado_actual='entregado', estado_fulfillment='entregado',
                          entregado_en=NOW(), actualizado_en=NOW()
        WHERE pedido_id=$1
    """, row["pedido_id"])
    await oltp_emit_evento(conn, "envio", row["envio_id"], "envio_entregado", {
        "envio_id": row["envio_id"], "pedido_id": row["pedido_id"],
    })
    oltp_stats["envios"] += 1
    log.info(f"[OLTP][ENVIO] envio {row['envio_id']} entregado")


OLTP_ACCIONES = {
    "nuevo_pedido":       accion_nuevo_pedido,
    "avanzar_estado":     accion_avanzar_estado,
    "procesar_pago":      accion_procesar_pago,
    "cancelar_pedido":    accion_cancelar_pedido,
    "reponer_stock":      accion_reponer_stock,
    "devolucion":         accion_devolucion,
    "carrito_abandonado": accion_carrito_abandonado,
    "actualizar_envio":   accion_actualizar_envio,
}


def elegir_accion():
    return random.choices(list(PROB.keys()), weights=list(PROB.values()), k=1)[0]


# OLAP 

async def olap_ensure_dim_cliente(conn, cliente_id):
    existing = await conn.fetchval("""
        SELECT cliente_sk FROM warehouse.dim_cliente WHERE cliente_id = $1 AND es_actual = TRUE
    """, cliente_id)
    if existing:
        return existing
    row = await conn.fetchrow("""
        SELECT c.cliente_id, c.correo, c.nombre || ' ' || c.apellido AS nombre_completo,
               c.segmento, c.estado,
               COALESCE(d.codigo_pais, 'CL') AS codigo_pais,
               COALESCE(d.ciudad, 'Santiago') AS ciudad,
               COALESCE(d.provincia, 'Metropolitana') AS region
        FROM clientes c
        LEFT JOIN direcciones_cliente d ON d.cliente_id = c.cliente_id AND d.es_predeterminada = TRUE
        WHERE c.cliente_id = $1
    """, cliente_id)
    if not row:
        return None
    sk = await conn.fetchval("""
        INSERT INTO warehouse.dim_cliente
            (cliente_id, correo, nombre_completo, segmento, estado, codigo_pais, ciudad, region)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING cliente_sk
    """, row["cliente_id"], row["correo"], row["nombre_completo"],
        row["segmento"], row["estado"], row["codigo_pais"], row["ciudad"], row["region"])
    olap_stats["dim_upserts"] += 1
    return sk


async def olap_ensure_dim_producto(conn, producto_id):
    existing = await conn.fetchval("""
        SELECT producto_sk FROM warehouse.dim_producto WHERE producto_id = $1 AND es_actual = TRUE
    """, producto_id)
    if existing:
        return existing
    row = await conn.fetchrow("""
        SELECT p.producto_id, p.nombre AS nombre_producto, m.nombre AS nombre_marca,
               c.nombre AS nombre_categoria, COALESCE(cp.nombre, c.nombre) AS categoria_padre,
               p.estado_producto
        FROM productos p
        JOIN marcas m ON m.marca_id = p.marca_id
        JOIN categorias c ON c.categoria_id = p.categoria_id
        LEFT JOIN categorias cp ON cp.categoria_id = c.categoria_padre_id
        WHERE p.producto_id = $1
    """, producto_id)
    if not row:
        return None
    sk = await conn.fetchval("""
        INSERT INTO warehouse.dim_producto
            (producto_id, nombre_producto, nombre_marca, nombre_categoria, categoria_padre, estado_producto)
        VALUES ($1, $2, $3, $4, $5, $6) RETURNING producto_sk
    """, row["producto_id"], row["nombre_producto"], row["nombre_marca"],
        row["nombre_categoria"], row["categoria_padre"], row["estado_producto"])
    olap_stats["dim_upserts"] += 1
    return sk


async def olap_ensure_dim_variante(conn, variante_id):
    existing = await conn.fetchval("""
        SELECT variante_sk FROM warehouse.dim_variante WHERE variante_id = $1 AND es_actual = TRUE
    """, variante_id)
    if existing:
        return existing
    row = await conn.fetchrow("""
        SELECT vp.variante_id, vp.producto_id, vp.sku, vp.color, vp.talla,
               pp.precio_lista AS precio_lista_actual, vp.precio_costo, vp.activa
        FROM variantes_producto vp
        LEFT JOIN precios_producto pp ON pp.variante_id = vp.variante_id
        WHERE vp.variante_id = $1
        ORDER BY pp.vigente_desde DESC LIMIT 1
    """, variante_id)
    if not row:
        return None
    prod_sk = await olap_ensure_dim_producto(conn, row["producto_id"])
    sk = await conn.fetchval("""
        INSERT INTO warehouse.dim_variante
            (variante_id, producto_sk, sku, color, talla, precio_lista_actual, precio_costo, activa)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING variante_sk
    """, row["variante_id"], prod_sk, row["sku"], row["color"], row["talla"],
        row["precio_lista_actual"], row["precio_costo"], row["activa"])
    olap_stats["dim_upserts"] += 1
    return sk


async def olap_ensure_dim_almacen(conn, almacen_id):
    existing = await conn.fetchval(
        "SELECT almacen_sk FROM warehouse.dim_almacen WHERE almacen_id = $1", almacen_id)
    if existing:
        return existing
    row = await conn.fetchrow(
        "SELECT almacen_id, codigo, nombre, ciudad, codigo_pais FROM almacenes WHERE almacen_id=$1",
        almacen_id)
    if not row:
        return None
    sk = await conn.fetchval("""
        INSERT INTO warehouse.dim_almacen (almacen_id, codigo, nombre, ciudad, codigo_pais)
        VALUES ($1, $2, $3, $4, $5) RETURNING almacen_sk
    """, row["almacen_id"], row["codigo"], row["nombre"], row["ciudad"], row["codigo_pais"])
    olap_stats["dim_upserts"] += 1
    return sk


async def olap_get_fecha_sk(conn, fecha):
    if isinstance(fecha, str):
        fecha = datetime.fromisoformat(fecha.replace("Z", "+00:00"))
    if hasattr(fecha, 'date'):
        fecha = fecha.date()
    return await conn.fetchval("SELECT fecha_sk FROM warehouse.dim_fecha WHERE fecha = $1", fecha)


async def olap_process_pedido_creado(conn, data):
    pedido_id = data.get("pedido_id")
    if not pedido_id:
        return
    exists = await conn.fetchval("SELECT 1 FROM warehouse.fact_pedidos WHERE pedido_id = $1", pedido_id)
    if exists:
        return

    ped = await conn.fetchrow("SELECT * FROM pedidos WHERE pedido_id = $1", pedido_id)
    if not ped:
        return

    cliente_sk = await olap_ensure_dim_cliente(conn, ped["cliente_id"])
    fecha_sk = await olap_get_fecha_sk(conn, ped["creado_en"])
    canal_sk = await conn.fetchval(
        "SELECT canal_sk FROM warehouse.dim_canal WHERE canal = $1", ped["canal_venta"])
    estado_sk = await conn.fetchval(
        "SELECT estado_pedido_sk FROM warehouse.dim_estado_pedido WHERE estado = $1", ped["estado_actual"])
    num_items = await conn.fetchval("SELECT COUNT(*) FROM detalle_pedidos WHERE pedido_id = $1", pedido_id)

    await conn.execute("""
        INSERT INTO warehouse.fact_pedidos (
            pedido_id, numero_pedido, cliente_sk, fecha_sk, canal_sk, estado_pedido_sk,
            subtotal, descuento_total, impuesto_total, costo_envio, total_final,
            num_items, estado_pago, estado_fulfillment, creado_en
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
    """, pedido_id, ped["numero_pedido"], cliente_sk, fecha_sk, canal_sk, estado_sk,
        ped["subtotal"], ped["descuento_total"], ped["impuesto_total"],
        ped["costo_envio"], ped["total_final"], num_items,
        ped["estado_pago"], ped["estado_fulfillment"], ped["creado_en"])
    olap_stats["facts_inserted"] += 1

    detalles = await conn.fetch("SELECT * FROM detalle_pedidos WHERE pedido_id = $1", pedido_id)
    for det in detalles:
        variante_sk = await olap_ensure_dim_variante(conn, det["variante_id"])
        prod_id = await conn.fetchval(
            "SELECT producto_id FROM variantes_producto WHERE variante_id=$1", det["variante_id"])
        producto_sk = await olap_ensure_dim_producto(conn, prod_id) if prod_id else None
        await conn.execute("""
            INSERT INTO warehouse.fact_detalle_pedidos (
                detalle_pedido_id, pedido_id, variante_sk, producto_sk, cliente_sk, fecha_sk,
                cantidad, precio_unitario, descuento_linea, impuesto_linea, total_linea, creado_en
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        """, det["detalle_pedido_id"], pedido_id, variante_sk, producto_sk,
            cliente_sk, fecha_sk, det["cantidad"], det["precio_unitario"],
            det["descuento_linea"], det["impuesto_linea"], det["total_linea"], det["creado_en"])
        olap_stats["facts_inserted"] += 1

    log.info(f"  [OLAP] → fact_pedidos + {len(detalles)} detalles para pedido {pedido_id}")


async def olap_process_pago(conn, data, es_exitoso):
    pago_id = data.get("pago_id")
    pedido_id = data.get("pedido_id")
    if not pago_id:
        return
    exists = await conn.fetchval("SELECT 1 FROM warehouse.fact_pagos WHERE pago_id = $1", pago_id)
    if exists:
        return
    pago = await conn.fetchrow("SELECT * FROM pagos WHERE pago_id = $1", pago_id)
    if not pago:
        return
    cliente_id = data.get("cliente_id") or await conn.fetchval(
        "SELECT cliente_id FROM pedidos WHERE pedido_id=$1", pedido_id)
    cliente_sk = await olap_ensure_dim_cliente(conn, cliente_id) if cliente_id else None
    fecha_sk = await olap_get_fecha_sk(conn, pago["creado_en"])
    metodo_sk = await conn.fetchval(
        "SELECT metodo_pago_sk FROM warehouse.dim_metodo_pago WHERE metodo_pago=$1", pago["metodo_pago"])
    await conn.execute("""
        INSERT INTO warehouse.fact_pagos (
            pago_id, pedido_id, cliente_sk, fecha_sk, metodo_pago_sk,
            monto, estado, proveedor_pago, creado_en
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    """, pago_id, pago["pedido_id"], cliente_sk, fecha_sk, metodo_sk,
        pago["monto"], pago["estado"], pago["proveedor_pago"], pago["creado_en"])
    olap_stats["facts_inserted"] += 1
    log.info(f"  [OLAP] → fact_pagos: pago {pago_id} ({'✓' if es_exitoso else '✗'})")


async def olap_process_stock(conn, data):
    mov_id = data.get("movimiento_id")
    if not mov_id:
        return
    exists = await conn.fetchval(
        "SELECT 1 FROM warehouse.fact_movimientos_inventario WHERE movimiento_id=$1", mov_id)
    if exists:
        return
    mov = await conn.fetchrow(
        "SELECT * FROM movimientos_inventario WHERE movimiento_inventario_id=$1", mov_id)
    if not mov:
        return
    almacen_sk = await olap_ensure_dim_almacen(conn, mov["almacen_id"])
    variante_sk = await olap_ensure_dim_variante(conn, mov["variante_id"])
    fecha_sk = await olap_get_fecha_sk(conn, mov["fecha_movimiento"])
    await conn.execute("""
        INSERT INTO warehouse.fact_movimientos_inventario (
            movimiento_id, almacen_sk, variante_sk, fecha_sk,
            tipo_movimiento, cantidad, tipo_referencia, referencia_id, fecha_movimiento
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    """, mov["movimiento_inventario_id"], almacen_sk, variante_sk, fecha_sk,
        mov["tipo_movimiento"], mov["cantidad"], mov["tipo_referencia"],
        mov["referencia_id"], mov["fecha_movimiento"])
    olap_stats["facts_inserted"] += 1
    log.info(f"  [OLAP] → fact_movimientos: mov {mov_id}")


OLAP_HANDLERS = {
    "pedido_creado":  lambda conn, data: olap_process_pedido_creado(conn, data),
    "pago_capturado": lambda conn, data: olap_process_pago(conn, data, True),
    "pago_fallido":   lambda conn, data: olap_process_pago(conn, data, False),
    "stock_repuesto": lambda conn, data: olap_process_stock(conn, data),
}


async def olap_process_event(conn, evento_id, tipo_evento, payload):
    handler = OLAP_HANDLERS.get(tipo_evento)
    if not handler:
        await conn.execute(
            "UPDATE eventos_negocio SET procesado_olap=TRUE WHERE evento_negocio_id=$1", evento_id)
        return
    try:
        data = payload if isinstance(payload, dict) else json.loads(payload) if payload else {}
        await handler(conn, data)
        await conn.execute(
            "UPDATE eventos_negocio SET procesado_olap=TRUE WHERE evento_negocio_id=$1", evento_id)
        olap_stats["procesados"] += 1
    except Exception as e:
        olap_stats["errores"] += 1
        log.error(f"  [OLAP] Error procesando evento {evento_id} ({tipo_evento}): {e}")


# =============================================================================
# LOOPS PRINCIPALES
# =============================================================================

async def oltp_loop(pool):
    """Loop principal del simulador OLTP."""
    log.info("[OLTP] Motor OLTP iniciado. Generando trafico transaccional...")

    while running:
        nombre = elegir_accion()
        fn = OLTP_ACCIONES[nombre]
        interval = random.uniform(OLTP_INTERVAL_MIN, OLTP_INTERVAL_MAX)

        try:
            async with pool.acquire() as conn:
                interval = await oltp_get_adaptive_interval(conn)
                if interval == -1 and nombre == "nuevo_pedido":
                    nombre = "avanzar_estado"
                    fn = OLTP_ACCIONES[nombre]

                async with conn.transaction():
                    await fn(conn)

            oltp_stats["ciclos"] += 1

            # Auto-mantenimiento
            if oltp_stats["ciclos"] % OLTP_VACUUM_EVERY == 0:
                async with pool.acquire() as conn:
                    for tabla in ["pedidos", "pagos", "eventos_negocio"]:
                        await conn.execute(f"VACUUM ANALYZE {tabla}")
                    log.info(f"[OLTP][MAINT] VACUUM completado (ciclo {oltp_stats['ciclos']})")

        except Exception as e:
            oltp_stats["errores"] += 1
            log.error(f"[OLTP][ERROR] {nombre}: {e}")

        if interval and interval > 0:
            await asyncio.sleep(interval)
        else:
            await asyncio.sleep(random.uniform(OLTP_INTERVAL_MIN, OLTP_INTERVAL_MAX))


async def olap_batch_catchup(pool):
    """Procesa todos los eventos no procesados en batch."""
    log.info("[OLAP] Batch catchup — procesando eventos pendientes...")
    total = 0
    while True:
        async with pool.acquire() as conn:
            col = await conn.fetchval("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='eventos_negocio' AND column_name='procesado_olap'
            """)
            if not col:
                log.info("[OLAP] Agregando columna procesado_olap...")
                await conn.execute("""
                    ALTER TABLE eventos_negocio
                    ADD COLUMN IF NOT EXISTS procesado_olap BOOLEAN NOT NULL DEFAULT FALSE
                """)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_eventos_procesado_olap
                    ON eventos_negocio(procesado_olap) WHERE procesado_olap = FALSE
                """)

            eventos = await conn.fetch("""
                SELECT evento_negocio_id, tipo_evento, payload FROM eventos_negocio
                WHERE procesado_olap = FALSE ORDER BY evento_negocio_id ASC LIMIT $1
            """, OLAP_BATCH_SIZE)

            if not eventos:
                break

            for ev in eventos:
                async with conn.transaction():
                    await olap_process_event(conn, ev["evento_negocio_id"],
                                             ev["tipo_evento"], ev["payload"])
            total += len(eventos)
            log.info(f"  [OLAP] Batch: {total} eventos procesados...")

    log.info(f"[OLAP] Catchup completado: {total} eventos totales")


async def olap_realtime_listener(pool):
    """Escucha pg_notify + polling periodico de eventos pendientes."""
    conn = await asyncpg.connect(**DB_CONFIG)
    log.info("[OLAP] Listener tiempo real iniciado — escuchando 'eventos_negocio'...")

    async def on_notification(connection, pid, channel, payload_str):
        try:
            data = json.loads(payload_str)
            evento_id = data.get("id")
            tipo_evento = data.get("tipo_evento")
            payload = data.get("payload", {})
            log.info(f"[OLAP][RT] Evento {evento_id}: {tipo_evento}")
            async with pool.acquire() as proc_conn:
                async with proc_conn.transaction():
                    await olap_process_event(proc_conn, evento_id, tipo_evento, payload)
        except Exception as e:
            log.error(f"[OLAP][RT] Error: {e}")

    await conn.add_listener("eventos_negocio", on_notification)

    last_health = time.time()
    try:
        while running:
            await asyncio.sleep(OLAP_POLL_INTERVAL)

            # Polling de seguridad: procesar eventos que pg_notify pudo haber perdido
            try:
                async with pool.acquire() as poll_conn:
                    pendientes = await poll_conn.fetch("""
                        SELECT evento_negocio_id, tipo_evento, payload FROM eventos_negocio
                        WHERE procesado_olap = FALSE ORDER BY evento_negocio_id ASC LIMIT $1
                    """, OLAP_BATCH_SIZE)
                    if pendientes:
                        for ev in pendientes:
                            async with poll_conn.transaction():
                                await olap_process_event(poll_conn, ev["evento_negocio_id"],
                                                         ev["tipo_evento"], ev["payload"])
            except Exception as e:
                log.error(f"[OLAP][POLL] Error: {e}")

            # Health log
            if time.time() - last_health > OLAP_HEALTH_EVERY:
                last_health = time.time()
                try:
                    async with pool.acquire() as hconn:
                        counts = await hconn.fetch("""
                            SELECT 'fact_pedidos' as t, COUNT(*) as c FROM warehouse.fact_pedidos
                            UNION ALL SELECT 'fact_pagos', COUNT(*) FROM warehouse.fact_pagos
                            UNION ALL SELECT 'fact_detalles', COUNT(*) FROM warehouse.fact_detalle_pedidos
                            UNION ALL SELECT 'fact_movimientos', COUNT(*) FROM warehouse.fact_movimientos_inventario
                            UNION ALL SELECT 'pendientes_olap', COUNT(*) FROM eventos_negocio WHERE procesado_olap = FALSE
                        """)
                        log.info("=" * 50)
                        log.info("[HEALTH] Estado del sistema:")
                        log.info("-" * 50)
                        for r in counts:
                            log.info(f"  {r['t']:25s}: {r['c']:>8,}")
                        log.info(f"  oltp_ciclos            : {oltp_stats['ciclos']:>8,}")
                        log.info(f"  oltp_pedidos           : {oltp_stats['pedidos']:>8,}")
                        log.info(f"  olap_procesados        : {olap_stats['procesados']:>8,}")
                        log.info(f"  olap_facts_inserted    : {olap_stats['facts_inserted']:>8,}")
                        log.info(f"  errores_oltp           : {oltp_stats['errores']:>8,}")
                        log.info(f"  errores_olap           : {olap_stats['errores']:>8,}")
                        log.info("=" * 50)
                except Exception:
                    pass

    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await conn.remove_listener("eventos_negocio", on_notification)
        await conn.close()


async def olap_loop(pool):
    """Loop principal del procesador OLAP."""
    await olap_batch_catchup(pool)
    await olap_realtime_listener(pool)



# VERIFICACION Y ARRANQUE

async def verify_schema(pool):
    """Verifica que el schema OLTP y OLAP existan."""
    async with pool.acquire() as conn:
        # Verificar tablas OLTP criticas
        for table in ["clientes", "pedidos", "pagos", "eventos_negocio", "niveles_stock"]:
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = $1
                )
            """, table)
            if not exists:
                log.error(f"Tabla OLTP '{table}' no encontrada.")
                log.error("Ejecuta primero: psql -h localhost -U ecommerce -d ecommerce_cl -f init/seed_completo.sql")
                return False

        # Verificar schema warehouse
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'warehouse'
            )
        """)
        if not exists:
            log.error("Schema 'warehouse' no encontrado.")
            log.error("Ejecuta primero: psql -h localhost -U ecommerce -d ecommerce_cl -f init/seed_completo.sql")
            return False

        # Verificar datos base
        count = await conn.fetchval("SELECT COUNT(*) FROM clientes")
        if count == 0:
            log.error("No hay datos base (0 clientes).")
            log.error("Ejecuta primero: psql -h localhost -U ecommerce -d ecommerce_cl -f init/seed_completo.sql")
            return False

        log.info(f"[INIT] Schema verificado: {count} clientes encontrados ✓")

        # Verificar procesado_olap en eventos_negocio
        col = await conn.fetchval("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='eventos_negocio' AND column_name='procesado_olap'
        """)
        if not col:
            log.info("[INIT] Agregando columna procesado_olap a eventos_negocio...")
            await conn.execute("""
                ALTER TABLE eventos_negocio
                ADD COLUMN IF NOT EXISTS procesado_olap BOOLEAN NOT NULL DEFAULT FALSE
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_eventos_procesado_olap
                ON eventos_negocio(procesado_olap) WHERE procesado_olap = FALSE
            """)

    return True


async def main():
    global running

    # Parse CLI
    solo_oltp = "--solo-oltp" in sys.argv
    solo_olap = "--solo-olap" in sys.argv

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--interval" and i + 2 < len(args):
            global OLTP_INTERVAL_MIN, OLTP_INTERVAL_MAX
            OLTP_INTERVAL_MIN = float(args[i+1])
            OLTP_INTERVAL_MAX = float(args[i+2])
            i += 3
        elif args[i] == "--max-pedidos" and i + 1 < len(args):
            global OLTP_MAX_PEDIDOS
            OLTP_MAX_PEDIDOS = int(args[i+1])
            i += 2
        else:
            i += 1

    modo = "OLTP+OLAP"
    if solo_oltp:
        modo = "Solo OLTP"
    elif solo_olap:
        modo = "Solo OLAP"

    log.info("=" * 60)
    log.info("  PLATAFORMA DATOS — Ecommerce Chile")
    log.info(f"  DB: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    log.info(f"  Modo: {modo}")
    log.info(f"  OLTP: intervalo {OLTP_INTERVAL_MIN}s-{OLTP_INTERVAL_MAX}s | max {OLTP_MAX_PEDIDOS:,} pedidos")
    log.info(f"  OLAP: batch {OLAP_BATCH_SIZE} | poll cada {OLAP_POLL_INTERVAL}s | health cada {OLAP_HEALTH_EVERY}s")
    log.info("=" * 60)

    pool = await asyncpg.create_pool(**DB_CONFIG, min_size=3, max_size=10)

    # Verificar schema
    if not await verify_schema(pool):
        await pool.close()
        sys.exit(1)

    start_time = time.time()
    log.info("Plataforma iniciada. Ctrl+C para detener.\n")

    # Ejecutar motores concurrentes
    tasks = []
    if not solo_olap:
        tasks.append(asyncio.create_task(oltp_loop(pool)))
    if not solo_oltp:
        tasks.append(asyncio.create_task(olap_loop(pool)))

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        running = False

    # Resumen final
    await pool.close()
    elapsed = time.time() - start_time

    log.info("")
    log.info("=" * 60)
    log.info(f"  PLATAFORMA DETENIDA — {elapsed:.0f}s")
    log.info("=" * 60)

    if not solo_olap:
        log.info("  --- Motor OLTP ---")
        for k, v in oltp_stats.items():
            log.info(f"  {k:20s}: {v:>8,}")

    if not solo_oltp:
        log.info("  --- Motor OLAP ---")
        for k, v in olap_stats.items():
            log.info(f"  {k:20s}: {v:>8,}")

    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
