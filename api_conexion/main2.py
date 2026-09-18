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
# RUTAS Y CONFIGURACIÓN PERSISTENTE
# ============================================================
def obtener_directorio_ejecutable() -> str:
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

import os


def obtener_ruta_respaldo() -> str:
    carpeta_destino = os.path.join(obtener_directorio_ejecutable(), "respaldo")
    os.makedirs(carpeta_destino, exist_ok=True)
    return os.path.join(carpeta_destino, "respaldo_marcajes.txt")

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
        logger.info(f"Configuración cargada desde: {config_path}")
    except Exception as e:
        logger.error(f"Error leyendo {config_path}: {e}")

SERVER_PORT = int(CFG.get("SERVER_PORT", "8000"))
ENABLE_HARDWARE = CFG.get("ENABLE_HARDWARE", "0") == "1"
STORE_ID = CFG.get("STORE_ID") or CFG.get("ID_TIENDA") or "GENERAL"

IP_PRINCIPAL = CFG.get("IP_PRINCIPAL", CFG.get("BIOMETRIC_IP", "192.168.20.210"))
IP_SECUNDARIO = CFG.get("IP_SECUNDARIO", "192.168.20.211")

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

# Almacenamiento en memoria para control de duplicados cuando la DB no está activa
MEMORIA_DUPLICADOS: dict = {}

# ============================================================
# PARSEO Y BÚSQUEDA DE DATOS
# ============================================================
def buscar_en_datos(data: Any, target_keys: list) -> Any:
    if isinstance(data, dict):
        for key, value in data.items():
            if str(key).split(":")[-1] in target_keys: 
                return value
            if isinstance(value, (dict, list)):
                res = buscar_en_datos(value, target_keys)
                if res is not None: return res
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                res = buscar_en_datos(item, target_keys)
                if res is not None: return res
    return None

def parsear_fecha_hora(valor: Any) -> Optional[datetime]:
    if not valor: return None
    try:
        dt = datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except ValueError:
        return None

# ============================================================
# LÓGICA DE BASE DE DATOS Y RESPALDO
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

def _operacion_db_sync(id_empleado: str, fecha_hora: datetime, ip_dispositivo: str, origen: str, id_tienda: str = STORE_ID) -> bool:
    limite_tiempo = fecha_hora - timedelta(minutes=DUPLICATE_MINUTES)

    # 🔹 SI NO HAY CONEXIÓN/HARDWARE DESACTIVADO: Validar duplicados en memoria (Modo Offline)
    if not DB_ENGINE or not ENABLE_HARDWARE:
        ultimo_marcaje = MEMORIA_DUPLICADOS.get(id_empleado)
        if ultimo_marcaje and (fecha_hora - ultimo_marcaje) < timedelta(minutes=DUPLICATE_MINUTES):
            print(f"⚠️ [DUPLICADO DETECTADO - OFFLINE] Emp: {id_empleado} a las {fecha_hora}")
            logger.warning(f"Marcaje duplicado omitido (Modo Offline) -> Emp: {id_empleado} | Hora: {fecha_hora}")
            return False
        
        MEMORIA_DUPLICADOS[id_empleado] = fecha_hora
        return True

    fecha_solo = fecha_hora.date()
    hora_solo = fecha_hora.strftime("%H:%M:%S")

    if fecha_hora.time() <= time(6, 0, 0):
        fecha_solo -= timedelta(days=1)
    dia_anterior_valor = 1 if fecha_solo < date.today() else 0

    try:
        id_tienda_num = int(id_tienda)
    except (ValueError, TypeError):
        id_tienda_num = None

    try:
        with DB_ENGINE.begin() as conn:
            query_dup = text(f"""
                SELECT 1 FROM {TABLE_NAME} 
                WHERE cod_emp = :cod_emp AND fecha = :fecha
                  AND hora >= :limite_hora AND hora <= :hora
                LIMIT 1
            """)
            duplicado = conn.execute(query_dup, {
                "cod_emp": id_empleado,
                "fecha": fecha_solo,
                "limite_hora": limite_tiempo.strftime("%H:%M:%S"),
                "hora": hora_solo
            }).fetchone()

            if duplicado:
                print(f"⚠️ [DUPLICADO DETECTADO] Emp: {id_empleado} a las {fecha_hora}")
                logger.warning(f"Marcaje duplicado omitido -> Emp: {id_empleado} | Hora: {fecha_hora} | IP: {ip_dispositivo}")
                return False

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

        logger.info(f"✅ [GUARDADO EXITOSO EN POSTGRESQL] Emp: {id_empleado} | Tabla: {TABLE_NAME}")
        return True
    except Exception as e:
        logger.error(f"Error procesando transacción en PostgreSQL: {e}")
        return True

async def procesar_y_guardar_evento(body_data: dict, origen="PULL", ip_override: Optional[str] = None) -> bool:
    id_empleado = str(buscar_en_datos(body_data, ["employeeNoString", "employeeNo"]) or "").strip()
    if not id_empleado: 
        return False

    fecha_hora = parsear_fecha_hora(buscar_en_datos(body_data, ["dateTime", "time"])) or datetime.now()
    ip_dispositivo = ip_override or body_data.get("ipAddress") or IP_PRINCIPAL

    id_tienda_evento = body_data.get("id_tienda")
    id_tienda = str(id_tienda_evento).strip() if id_tienda_evento else STORE_ID

    fecha_str = fecha_hora.strftime("%Y-%m-%d %H:%M:%S")
    
    mensaje_consola = (
        f"============================================================\n"
        f"============================================================\n"
        f"🔥 [MARCAJE EN TIEMPO REAL DETECTADO]\n"
        f"   👤 EMPLEADO: {id_empleado}\n"
        f"   🏢 TIENDA  : {id_tienda}\n"
        f"   🕒 FECHA   : {fecha_str}\n"
        f"   🌐 ORIGEN  : {origen} ({ip_dispositivo})\n"
        f"============================================================"
    )
    print(mensaje_consola)

    procesado = await asyncio.to_thread(_operacion_db_sync, id_empleado, fecha_hora, ip_dispositivo, origen, id_tienda)
    if procesado:
        await guardar_respaldo_local_txt(id_empleado, fecha_hora, ip_dispositivo, origen, id_tienda)

    return procesado

# ============================================================
# LÓGICA DE CLONACIÓN Y SINCRONIZACIÓN DE CREDENCIALES (ISAPI)
# ============================================================
async def replicar_usuario_y_huellas(ip_origen: str, ip_destino: str, id_empleado: str) -> bool:
    auth = httpx.DigestAuth(BIOMETRIC_USER, BIOMETRIC_PASS)
    async with httpx.AsyncClient(auth=auth, verify=False, timeout=12.0) as client:
        try:
            url_search = f"http://{ip_origen}/ISAPI/AccessControl/UserInfo/Search?format=json"
            req_user = {"UserInfoSearchCond": {"searchID": "1", "maxResults": 1, "EmployeeNoList": [{"employeeNo": str(id_empleado)}]}}
            res_user = await client.post(url_search, json=req_user)
            
            if res_user.status_code != 200:
                logger.error(f"Error consultando usuario {id_empleado} en {ip_origen}: HTTP {res_user.status_code}")
                return False

            users = buscar_en_datos(res_user.json(), ["UserInfo"]) or []
            if not users:
                logger.warning(f"Usuario {id_empleado} no encontrado en el biométrico principal ({ip_origen})")
                return False
            
            user_payload = {"UserInfo": users[0]}

            url_set_user = f"http://{ip_destino}/ISAPI/AccessControl/UserInfo/SetUp?format=json"
            res_set_user = await client.put(url_set_user, json=user_payload)
            if res_set_user.status_code not in [200, 201]:
                logger.error(f"Error guardando usuario {id_empleado} en {ip_destino}: {res_set_user.text}")
                return False

            url_get_fp = f"http://{ip_origen}/ISAPI/AccessControl/FingerPrintUpload?format=json"
            req_fp = {"FingerPrintCond": {"searchID": "1", "employeeNo": str(id_empleado)}}
            res_fp = await client.post(url_get_fp, json=req_fp)

            if res_fp.status_code == 200:
                fp_data = res_fp.json()
                if "FingerPrintCfg" in fp_data:
                    url_set_fp = f"http://{ip_destino}/ISAPI/AccessControl/FingerPrintSetUp?format=json"
                    res_set_fp = await client.post(url_set_fp, json=fp_data)
                    if res_set_fp.status_code in [200, 201]:
                        logger.info(f"✅ Usuario {id_empleado} y huellas replicados con éxito en {ip_destino}")
                        return True
                    else:
                        logger.error(f"Error guardando huella de {id_empleado} en {ip_destino}: {res_set_fp.text}")
            
            logger.info(f"Usuario {id_empleado} replicado en {ip_destino} (sin huellas adjuntas).")
            return True

        except Exception as e:
            logger.error(f"Error en replicación de usuario {id_empleado} ({ip_origen} -> {ip_destino}): {e}")
            return False

# ============================================================
# RECEPCIÓN EN TIEMPO REAL PARALELA (MULTIDISPOSITIVO)
# ============================================================
async def escuchar_stream_por_ip(ip_dispositivo: str):
    url_stream = f"http://{ip_dispositivo}/ISAPI/Event/notification/alertStream"
    auth = httpx.DigestAuth(BIOMETRIC_USER, BIOMETRIC_PASS)

    while True:
        try:
            async with httpx.AsyncClient(auth=auth, verify=False, timeout=httpx.Timeout(60.0, read=None)) as client:
                async with client.stream("GET", url_stream) as response:
                    if response.status_code == 401:
                        logger.error(f"Error 401 en {ip_dispositivo}: Credenciales incorrectas.")
                        await asyncio.sleep(10)
                        continue

                    response.raise_for_status()
                    logger.info(f"Stream activo con biométrico en {ip_dispositivo}")

                    buffer = ""
                    async for chunk in response.aiter_text():
                        if not chunk: continue
                        buffer += chunk

                        while True:
                            inicio = buffer.find("{")
                            if inicio == -1: break
                            llaves_abiertas = 0
                            fin = -1
                            for idx in range(inicio, len(buffer)):
                                if buffer[idx] == "{": llaves_abiertas += 1
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
                                    asyncio.create_task(procesar_y_guardar_evento(evento, origen="STREAM", ip_override=ip_dispositivo))
                                except json.JSONDecodeError:
                                    continue
                            else:
                                break

                        if len(buffer) > 200000: buffer = ""

        except httpx.ReadTimeout:
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error en stream {ip_dispositivo}: {e}. Reintentando en 5s...")
            await asyncio.sleep(5)

# ============================================================
# CICLO DE VIDA Y API FASTAPI
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global DB_ENGINE
    tareas_stream = []

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

        ips_a_escuchar = list(set([IP_PRINCIPAL, IP_SECUNDARIO]))
        for ip in ips_a_escuchar:
            if ip:
                tareas_stream.append(asyncio.create_task(escuchar_stream_por_ip(ip)))
        logger.info(f"Servicios de monitoreo y sincronización activados para: {', '.join(ips_a_escuchar)}")
    else:
        # 🔹 NOTIFICACIÓN DE MODO OFFLINE EN LOGS Y CONSOLA
        logger.info("--------------------------------------------------")
        logger.info(" MODO OFFLINE ACTIVADO (ENABLE_HARDWARE=0)")
        logger.info(" Omitiendo conexión a PostgreSQL y tareas de biométrico.")
        logger.info("--------------------------------------------------")
        
        print("\n--------------------------------------------------")
        print(" ⚠️  MODO OFFLINE ACTIVADO (ENABLE_HARDWARE=0)")
        print(" Omitiendo conexión a PostgreSQL y tareas de biométrico.")
        print("--------------------------------------------------\n")

    yield

    for t in tareas_stream:
        t.cancel()
    if tareas_stream:
        await asyncio.gather(*tareas_stream, return_exceptions=True)
    if DB_ENGINE:
        DB_ENGINE.dispose()

app = FastAPI(title="Hikvision Attendance API", lifespan=lifespan)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verificar_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Acceso denegado")

@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "Hikvision Attendance API",
        "hardware_enabled": ENABLE_HARDWARE,
        "store_id": STORE_ID
    }

# 🔹 RUTAS DE SIMULACIÓN Y MARCAJE MANUAL
@app.post("/api/v1/hikvision/simular-marcaje")
async def endpoint_simular_marcaje(id_empleado: str = "TEST", id_tienda: str = "1", _=Depends(verificar_api_key)):
    """Simula un marcaje local pasando los parámetros por Query o JSON."""
    payload = {
        "employeeNo": id_empleado,
        "id_tienda": id_tienda,
        "dateTime": datetime.now().isoformat()
    }
    procesado = await procesar_y_guardar_evento(payload, origen="PRUEBA_MANUAL")
    return {"status": "procesado", "guardado": procesado}

@app.post("/api/v1/hikvision/marcaje-manual")
async def endpoint_marcaje_manual(request: Request, _=Depends(verificar_api_key)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Formato JSON inválido en el cuerpo de la petición.")
    
    procesado = await procesar_y_guardar_evento(body, origen="PRUEBA_ESTRES")
    return {"status": "procesado", "guardado": procesado}

@app.post("/api/v1/hikvision/replicar-empleado")
async def endpoint_replicar_empleado(id_empleado: str, _=Depends(verificar_api_key)):
    exito = await replicar_usuario_y_huellas(IP_PRINCIPAL, IP_SECUNDARIO, id_empleado)
    if not exito:
        raise HTTPException(status_code=500, detail="No se pudo completar la transferencia del usuario/huella.")
    
    return {
        "status": "exitoso",
        "empleado": id_empleado,
        "origen": IP_PRINCIPAL,
        "destino": IP_SECUNDARIO
    }

if __name__ == "__main__":
    multiprocessing.freeze_support()
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT, reload=False)