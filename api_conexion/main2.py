import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
import json
import logging
from logging.handlers import RotatingFileHandler
import multiprocessing
import os
import sys
from typing import Any, Optional
import urllib.parse

from fastapi import Depends, FastAPI, HTTPException, Request, Response, Security
from fastapi.security.api_key import APIKeyHeader
import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import uvicorn

# ============================================================
# RUTAS DE SISTEMA Y EJECUTABLE
# ============================================================
def obtener_directorio_base() -> str:
    if getattr(sys, 'frozen', False):
        return getattr(sys, '_MEIPASS', os.path.dirname(sys.executable))
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def obtener_directorio_ejecutable() -> str:
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def obtener_ruta_respaldo() -> str:
    carpeta_destino = os.path.join(obtener_directorio_ejecutable(), "output", "main")
    os.makedirs(carpeta_destino, exist_ok=True)
    return os.path.join(carpeta_destino, "respaldo_marcajes.txt")

# ============================================================
# CONFIGURACIÓN DE LOGS (CONSOLA + ARCHIVO PERSISTENTE)
# ============================================================
dir_logs = os.path.join(obtener_directorio_ejecutable(), "logs")
os.makedirs(dir_logs, exist_ok=True)
archivo_log = os.path.join(dir_logs, "api_hikvision.log")

log_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

file_handler = RotatingFileHandler(archivo_log, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
file_handler.setFormatter(log_formatter)

logger = logging.getLogger("HikvisionAPI")
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# ============================================================
# CARGA DE CONFIGURACIÓN EXTERNA (config.txt)
# ============================================================
config_path = os.path.join(obtener_directorio_ejecutable(), "config.txt")
CFG = {}

if os.path.exists(config_path):
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    try:
                        key, val = line.split("=", 1)
                        CFG[key.strip()] = val.strip()
                    except ValueError:
                        continue
        logger.info(f"Configuración cargada correctamente desde: {config_path}")
    except Exception as e:
        logger.error(f"Error leyendo {config_path}: {e}")
else:
    logger.warning(f"No se encontró 'config.txt' en {config_path}. Se usarán los valores por defecto.")

SERVER_PORT = int(CFG.get("SERVER_PORT", "8000"))
ENABLE_HARDWARE = CFG.get("ENABLE_HARDWARE", "0") == "1"

STORE_ID = CFG.get("STORE_ID") or CFG.get("ID_TIENDA") or "GENERAL"

BIOMETRIC_IP = CFG.get("BIOMETRIC_IP", "192.168.20.210")
BIOMETRIC_USER = CFG.get("BIOMETRIC_USER", "admin")
BIOMETRIC_PASS = CFG.get("BIOMETRIC_PASS", "admin123")

DB_HOST = CFG.get("DB_HOST", "192.168.50.19")
DB_PORT = CFG.get("DB_PORT", "5432")
DB_NAME = CFG.get("DB_NAME", "bihr")
DB_USER = CFG.get("DB_USER", "postgres")
DB_PASS = CFG.get("DB_PASS", "")

RAW_TABLE_NAME = CFG.get("TABLE_NAME", "public.marcaje")
TABLE_NAME = ".".join([f'"{part}"' for part in RAW_TABLE_NAME.replace('"', '').replace("[", "").replace("]", "").split(".")])

DUPLICATE_MINUTES = int(CFG.get("DUPLICATE_MINUTES", "2"))
API_SECRET_KEY = CFG.get("API_SECRET_KEY", "cambiar_este_token_por_uno_seguro")

file_lock = asyncio.Lock()
DB_ENGINE: Optional[Engine] = None

# ============================================================
# RESPALDO LOCAL TXT
# ============================================================
def _escribir_archivo(ruta: str, linea: str) -> None:
    with open(ruta, "a", encoding="utf-8") as f:
        f.write(linea)

async def guardar_respaldo_local_txt(id_empleado: str, fecha_hora: datetime, ip_dispositivo: str, origen: str, id_tienda: str = STORE_ID):
    archivo_respaldo = obtener_ruta_respaldo()
    timestamp = datetime.now().isoformat()
    linea_respaldo = f"{timestamp} | EMP: {id_empleado} | TIENDA: {id_tienda} | FECHA_HORA: {fecha_hora.isoformat()} | IP: {ip_dispositivo} | ORIGEN: {origen}\n"

    async with file_lock:
        try:
            await asyncio.to_thread(_escribir_archivo, archivo_respaldo, linea_respaldo)
            logger.info(f"Respaldo TXT guardado para {id_empleado} (Tienda: {id_tienda})")
        except Exception as e:
            logger.error(f"Error escribiendo respaldo TXT: {e}")

# ============================================================
# TAREA DE SINCRONIZACIÓN AUTOMÁTICA DE RESPALDOS
# ============================================================
async def tarea_sincronizar_respaldos_pendientes():
    archivo_respaldo = obtener_ruta_respaldo()

    while True:
        await asyncio.sleep(30)
        if not DB_ENGINE or not os.path.exists(archivo_respaldo):
            continue

        async with file_lock:
            try:
                def _leer_y_limpiar():
                    if not os.path.exists(archivo_respaldo):
                        return []
                    with open(archivo_respaldo, "r", encoding="utf-8") as f:
                        return f.readlines()

                lineas = await asyncio.to_thread(_leer_y_limpiar)
                if not lineas:
                    continue

                lineas_pendientes = []
                sincronizados_count = 0

                for linea in lineas:
                    if "EMP:" not in linea or "FECHA_HORA:" not in linea:
                        continue

                    try:
                        partes = [p.strip() for p in linea.split("|")]
                        emp_id, fecha_hora_str = None, None
                        ip_disp = BIOMETRIC_IP
                        origen_evt = "RESYNC"
                        tienda_evt = STORE_ID

                        for p in partes:
                            if p.startswith("EMP:"):
                                emp_id = p.replace("EMP:", "").strip()
                            elif p.startswith("TIENDA:"):
                                tienda_evt = p.replace("TIENDA:", "").strip() or STORE_ID
                            elif p.startswith("FECHA_HORA:"):
                                fecha_hora_str = p.replace("FECHA_HORA:", "").strip()
                            elif p.startswith("IP:"):
                                ip_disp = p.replace("IP:", "").strip()
                            elif p.startswith("ORIGEN:"):
                                origen_evt = p.replace("ORIGEN:", "").strip()

                        if emp_id and fecha_hora_str:
                            fh = datetime.fromisoformat(fecha_hora_str)
                            exito = await asyncio.to_thread(_operacion_db_sync, emp_id, fh, ip_disp, f"SYNC_{origen_evt}", tienda_evt)
                            if exito:
                                sincronizados_count += 1
                            else:
                                lineas_pendientes.append(linea)
                        else:
                            lineas_pendientes.append(linea)
                    except Exception as parse_err:
                        logger.error(f"Error parseando línea de respaldo para sincronizar: {parse_err}")
                        lineas_pendientes.append(linea)

                def _reescribir(pendientes):
                    with open(archivo_respaldo, "w", encoding="utf-8") as f:
                        f.writelines(pendientes)

                await asyncio.to_thread(_reescribir, lineas_pendientes)

                if sincronizados_count > 0:
                    logger.info(f"Sincronización exitosa: {sincronizados_count} marcajes pendientes subidos a PostgreSQL desde el respaldo.")

            except Exception as e:
                logger.error(f"Error en la tarea de sincronización de respaldos: {e}")

# ============================================================
# UTILIDADES DE BÚSQUEDA Y PARSEO
# ============================================================
def buscar_en_datos(data: Any, target_keys: list) -> Any:
    if isinstance(data, dict):
        for key, value in data.items():
            if str(key).split(":")[-1] in target_keys: 
                return value
            if isinstance(value, (dict, list)):
                res = buscar_en_datos(value, target_keys)
                if res is not None: 
                    return res
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                res = buscar_en_datos(item, target_keys)
                if res is not None: 
                    return res
    return None

def parsear_fecha_hora(valor: Any) -> Optional[datetime]:
    if not valor:
        return None
    try:
        dt = datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except ValueError:
        return None

# ============================================================
# LÓGICA DE BASE DE DATOS POSTGRESQL Y MODO OFFLINE
# ============================================================
def _operacion_db_sync(id_empleado: str, fecha_hora: datetime, ip_dispositivo: str, origen: str, id_tienda: str = STORE_ID) -> bool:
    limite_tiempo = fecha_hora - timedelta(minutes=DUPLICATE_MINUTES)

    # MODO OFFLINE
    if not DB_ENGINE:
        archivo_respaldo = obtener_ruta_respaldo()
        if os.path.exists(archivo_respaldo):
            try:
                with open(archivo_respaldo, "r", encoding="utf-8") as f:
                    lineas = f.readlines()

                for linea in reversed(lineas):
                    if f"EMP: {id_empleado}" in linea:
                        partes = linea.split("|")
                        for p in partes:
                            if "FECHA_HORA:" in p:
                                str_fh = p.replace("FECHA_HORA:", "").strip()
                                try:
                                    dt_existente = datetime.fromisoformat(str_fh)
                                    if limite_tiempo <= dt_existente <= fecha_hora:
                                        print(f"⚠️ [DUPLICADO DETECTADO - OFFLINE] Emp: {id_empleado} a las {fecha_hora}")
                                        logger.warning(f"Marcaje duplicado omitido (Modo Offline) -> Emp: {id_empleado} | Hora: {fecha_hora}")
                                        return False
                                except ValueError:
                                    continue
            except Exception as e:
                logger.error(f"Error leyendo respaldo TXT para duplicados: {e}")

        print(f"💾 [GUARDADO EN RESPALDO LOCAL] Emp: {id_empleado} | Hora: {fecha_hora}")
        return True

    # MODO ONLINE (POSTGRESQL)
    fecha_solo = fecha_hora.date()
    hora_solo = fecha_hora.strftime("%H:%M:%S")

    if fecha_hora.time() <= time(6, 0, 0):
        fecha_solo -= timedelta(days=1)
    dia_anterior_valor = 1 if fecha_solo < date.today() else 0

    # Conversión segura para id_tienda
    try:
        id_tienda_num = int(id_tienda)
    except (ValueError, TypeError):
        id_tienda_num = None

    try:
        with DB_ENGINE.begin() as conn:
            # Búsqueda de duplicados por cod_emp, fecha y rango horario
            query_dup = text(f"""
                SELECT 1 
                FROM {TABLE_NAME} 
                WHERE cod_emp = :cod_emp 
                  AND fecha = :fecha
                  AND hora >= :limite_hora
                  AND hora <= :hora
                LIMIT 1
            """)
            duplicado = conn.execute(query_dup, {
                "cod_emp": id_empleado,
                "fecha": fecha_solo,
                "limite_hora": limite_tiempo.strftime("%H:%M:%S"),
                "hora": hora_solo
            }).fetchone()

            if duplicado:
                print(f"⚠️ [DUPLICADO DETECTADO - OMITIDO DE BD] Emp: {id_empleado} | Hora: {fecha_hora}")
                logger.warning(f"Marcaje duplicado omitido -> Emp: {id_empleado} | Hora: {fecha_hora}")
                return False

            # Inserción con la estructura real de campos de la BD
            query_insert = text(f"""
                INSERT INTO {TABLE_NAME} (cod_emp, fecha, hora, ip, diaanterior, id_tienda, created_at)
                VALUES (:cod_emp, :fecha, :hora, :ip, :diaanterior, :id_tienda, :created_at)
            """)
            conn.execute(query_insert, {
                "cod_emp": id_empleado,
                "fecha": fecha_solo,
                "hora": hora_solo,
                "ip": ip_dispositivo,
                "diaanterior": dia_anterior_valor,
                "id_tienda": id_tienda_num,
                "created_at": datetime.now()
            })

        print(f"✅ [GUARDADO EXITOSO EN POSTGRESQL] Emp: {id_empleado} | Tabla: {TABLE_NAME}")
        logger.info(f"Marcaje registrado en PostgreSQL -> Emp: {id_empleado} | Tienda: {id_tienda} | {fecha_solo} {hora_solo} | Origen: {origen}")
        return True
    except Exception as e:
        print(f"❌ [ERROR AL GUARDAR EN BD] {e}. Se guardará en respaldo local TXT.")
        logger.error(f"Error procesando transacción en PostgreSQL: {e}. Se mantendrá respaldo local.")
        return True

async def procesar_y_guardar_evento(body_data: dict, origen="PULL") -> bool:
    id_empleado = str(buscar_en_datos(body_data, ["employeeNoString", "employeeNo"]) or "").strip()

    if not id_empleado: 
        return False

    fecha_hora = parsear_fecha_hora(buscar_en_datos(body_data, ["dateTime", "time"])) or datetime.now()
    ip_dispositivo = body_data.get("ipAddress") or BIOMETRIC_IP

    id_tienda_evento = body_data.get("id_tienda")
    id_tienda = str(id_tienda_evento).strip() if id_tienda_evento else STORE_ID

    print("\n" + "="*60)
    print(f"🔥 [MARCAJE EN TIEMPO REAL DETECTADO]")
    print(f"   👤 EMPLEADO: {id_empleado}")
    print(f"   🏢 TIENDA  : {id_tienda}")
    print(f"   🕒 FECHA   : {fecha_hora.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   🌐 ORIGEN  : {origen} ({ip_dispositivo})")

    procesado = await asyncio.to_thread(_operacion_db_sync, id_empleado, fecha_hora, ip_dispositivo, origen, id_tienda)

    if procesado:
        await guardar_respaldo_local_txt(id_empleado, fecha_hora, ip_dispositivo, origen, id_tienda)

    print("="*60 + "\n")
    return procesado

# ============================================================
# RECEPCIÓN EN TIEMPO REAL (ISAPI HIKVISION CON PARSEO BALANCEADO)
# ============================================================
async def escuchar_stream_en_vivo():
    url_stream = f"http://{BIOMETRIC_IP}/ISAPI/Event/notification/alertStream"
    auth = httpx.DigestAuth(BIOMETRIC_USER, BIOMETRIC_PASS)

    while True:
        try:
            async with httpx.AsyncClient(auth=auth, verify=False, timeout=httpx.Timeout(60.0, read=None)) as client:
                async with client.stream("GET", url_stream) as response:

                    if response.status_code == 401:
                        logger.error(f"Error 401: Credenciales incorrectas ({BIOMETRIC_USER}:{BIOMETRIC_PASS}) o ISAPI deshabilitado en {BIOMETRIC_IP}.")
                        await asyncio.sleep(10)
                        continue

                    response.raise_for_status()
                    logger.info(f"Conexión en TIEMPO REAL con Biométrico Hikvision ({BIOMETRIC_IP}) establecida.")

                    buffer = ""
                    async for chunk in response.aiter_text():
                        if not chunk: 
                            continue
                        buffer += chunk

                        while True:
                            inicio = buffer.find("{")
                            if inicio == -1:
                                break

                            llaves_abiertas = 0
                            fin = -1
                            for idx in range(inicio, len(buffer)):
                                if buffer[idx] == "{":
                                    llaves_abiertas += 1
                                elif buffer[idx] == "}":
                                    llaves_abiertas -= 1
                                    if llaves_abiertas == 0:
                                        fin = idx
                                        break

                            if fin != -1:
                                posible_json = buffer[inicio:fin+1]
                                buffer = buffer[fin+1:]

                                try:
                                    data = json.loads(posible_json)
                                    evento = buscar_en_datos(data, ["AccessControllerEvent", "AcsEvent"]) or data
                                    asyncio.create_task(procesar_y_guardar_evento(evento, origen="STREAM"))
                                except json.JSONDecodeError:
                                    continue
                            else:
                                break

                        if len(buffer) > 200000:
                            buffer = ""

        except httpx.ReadTimeout:
            logger.warning("Reconectando stream Hikvision por timeout de lectura...")
            await asyncio.sleep(2)
        except httpx.HTTPStatusError as http_err:
            logger.error(f"Error HTTP en Stream: {http_err}. Reintentando en 5 segundos...")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("Stream ISAPI detenido correctamente.")
            break
        except Exception as e:
            logger.error(f"Error en recepción en tiempo real: {e}. Reintentando en 5 segundos...")
            await asyncio.sleep(5)

async def sincronizar_marcajes_biometrico():
    auth = httpx.DigestAuth(BIOMETRIC_USER, BIOMETRIC_PASS)
    url = f"http://{BIOMETRIC_IP}/ISAPI/AccessControl/AcsEvent?format=json"

    async with httpx.AsyncClient(auth=auth, verify=False) as client:
        while True:
            try:
                ahora = datetime.now()
                payload = {
                    "AcsEventCond": {
                        "searchID": "1", 
                        "searchResultPosition": 0, 
                        "maxResults": 100,
                        "major": 5, 
                        "minor": 0,
                        "startTime": (ahora - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S"),
                        "endTime": (ahora + timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S")
                    }                    
                }

                response = await client.post(url, json=payload, timeout=10)
                if response.status_code == 200:
                    eventos = buscar_en_datos(response.json(), ["InfoList"]) or []
                    for ev in eventos:
                        if ev.get("employeeNoString"):
                            await procesar_y_guardar_evento(ev, origen="BUFFER")
            except asyncio.CancelledError:
                logger.info("Sincronizador Polling detenido.")
                break
            except Exception as e:
                logger.error(f"Error en sincronización por polling: {e}")

            await asyncio.sleep(10)

# ============================================================
# CICLO DE VIDA Y API FASTAPI
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global DB_ENGINE

    task_pull = None
    task_stream = None
    task_sync_respaldos = None

    if ENABLE_HARDWARE:
        user_encoded = urllib.parse.quote_plus(DB_USER)
        pass_encoded = urllib.parse.quote_plus(DB_PASS)

        db_url = f"postgresql+psycopg2://{user_encoded}:{pass_encoded}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

        try:
            DB_ENGINE = create_engine(
                db_url,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            with DB_ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Conexión con PostgreSQL verificada con éxito.")
        except Exception as e:
            logger.error(f"Error crítico al conectar a PostgreSQL: {e}")

        task_pull = asyncio.create_task(sincronizar_marcajes_biometrico())
        task_stream = asyncio.create_task(escuchar_stream_en_vivo())
        task_sync_respaldos = asyncio.create_task(tarea_sincronizar_respaldos_pendientes())
        logger.info("Servicios de monitoreo y sincronización activados.")
    else:
        logger.info("--------------------------------------------------")
        logger.info(" MODO OFFLINE ACTIVADO (ENABLE_HARDWARE=0)")
        logger.info(" Omitiendo conexión a PostgreSQL y tareas de biométrico.")
        logger.info("--------------------------------------------------")

    yield

    if ENABLE_HARDWARE:
        tasks = [t for t in [task_pull, task_stream, task_sync_respaldos] if t is not None]
        for t in tasks:
            t.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if DB_ENGINE:
            DB_ENGINE.dispose()
            logger.info("Motor de base de datos liberado.")

app = FastAPI(title="Hikvision Attendance API", lifespan=lifespan)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verificar_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Acceso denegado")

@app.get("/")
async def raiz():
    return {"message": "API de Hikvision funcionando correctamente", "status": "online"}

@app.post("/api/v1/hikvision/webhook")
async def recibir_webhook(request: Request):
    try:
        content_type = request.headers.get("content-type", "")

        if "application/json" in content_type:
            body = await request.json()
            evento = body.get("AccessControllerEvent", body)
            asyncio.create_task(procesar_y_guardar_evento(evento, origen="WEBHOOK"))
            return Response(content="Event accepted", status_code=202)

        elif "multipart" in content_type:
            form = await request.form()
            evento_raw = form.get("event_log") or form.get("AccessControllerEvent")

            if evento_raw:
                data = json.loads(evento_raw) if isinstance(evento_raw, str) else evento_raw
                evento = data.get("AccessControllerEvent", data)
                asyncio.create_task(procesar_y_guardar_evento(evento, origen="WEBHOOK"))
                return Response(content="Event accepted", status_code=202)

            for key, value in form.items():
                if isinstance(value, str) and "employeeNo" in value:
                    try:
                        data = json.loads(value)
                        evento = data.get("AccessControllerEvent", data)
                        asyncio.create_task(procesar_y_guardar_evento(evento, origen="WEBHOOK"))
                        return Response(content="Event accepted", status_code=202)
                    except json.JSONDecodeError:
                        continue

        body_bytes = await request.body()
        if body_bytes:
            texto = body_bytes.decode("utf-8", errors="ignore")
            if "{" in texto and "}" in texto:
                inicio = texto.find("{")
                fin = texto.rfind("}") + 1
                data = json.loads(texto[inicio:fin])
                evento = data.get("AccessControllerEvent", data)
                asyncio.create_task(procesar_y_guardar_evento(evento, origen="WEBHOOK"))
                return Response(content="Event accepted", status_code=202)

        return Response(content="No readable event data", status_code=400)

    except Exception as e:
        logger.error(f"Error procesando Webhook de Hikvision: {e}")
        return Response(content="Error internal processing", status_code=500)

@app.get("/api/v1/hikvision/reporte-respaldo")
async def obtener_reporte_respaldo(_=Depends(verificar_api_key)):
    archivo_respaldo = obtener_ruta_respaldo()
    if not os.path.exists(archivo_respaldo):
        return {"status": "ok", "total_registros": 0, "lineas": []}

    def _leer():
        with open(archivo_respaldo, "r", encoding="utf-8") as f:
            return f.readlines()

    lineas = await asyncio.to_thread(_leer)
    return {
        "status": "ok",
        "total_registros": len(lineas),
        "archivo": archivo_respaldo,
        "lineas": [linea.strip() for linea in lineas]
    }

@app.post("/api/v1/hikvision/simular-marcaje")
async def simular_marcaje(id_empleado: str, _=Depends(verificar_api_key)):
    payload_simulado = {
        "employeeNoString": id_empleado,
        "dateTime": datetime.now().isoformat(),
        "ipAddress": BIOMETRIC_IP,
        "id_tienda": STORE_ID
    }
    resultado = await procesar_y_guardar_evento(payload_simulado, origen="PRUEBA_MANUAL")
    return {
        "status": "procesado" if resultado else "ignorado (duplicado o error BD)",
        "empleado": id_empleado,
        "tienda": STORE_ID,
        "fecha_hora": datetime.now().isoformat()
    }

if __name__ == "__main__":
    multiprocessing.freeze_support()
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT, reload=False)    