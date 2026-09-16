import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
import json
import logging
import multiprocessing
import os
import subprocess
import sys
from typing import Any, Optional
import urllib.parse
from fastapi import Depends, FastAPI, HTTPException, Request, Response, Security
from fastapi.security.api_key import APIKeyHeader
import httpx
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import uvicorn

# ============================================================
# CONFIGURACIÓN Y LOGS
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("HikvisionAPI")

def obtener_directorio_config():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    # Sube un nivel usando '..' para buscar en la carpeta raíz 'API PARA CAPTADOR'
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def obtener_ruta_respaldo() -> str:
    carpeta_destino = os.path.join(obtener_directorio_config(), "output", "main")
    os.makedirs(carpeta_destino, exist_ok=True)
    return os.path.join(carpeta_destino, "respaldo_marcajes.txt")

# Carga de archivo config.txt
config_path = os.path.join(obtener_directorio_config(), "config.txt")
CFG = {}
if os.path.exists(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                try:
                    key, val = line.split("=", 1)
                    CFG[key.strip()] = val.strip()
                except ValueError:
                    continue
else:
    logger.warning(f"No se encontró {config_path}. Se usará la configuración por defecto.")

# PUERTO FIJADO ESTRICTAMENTE A 8000
SERVER_PORT = 8000

BIOMETRIC_IP = CFG.get("BIOMETRIC_IP", "192.168.20.215")
BIOMETRIC_USER = CFG.get("BIOMETRIC_USER", "admin")
BIOMETRIC_PASS = CFG.get("BIOMETRIC_PASS", "admin123")
DB_HOST = CFG.get("DB_HOST", "192.168.50.19")
DB_NAME = CFG.get("DB_NAME", "BIHR")
DB_USER = CFG.get("DB_USER", "sa")
DB_PASS = CFG.get("DB_PASS", "")

# Ajuste de nombre de tabla para SQL Server
RAW_TABLE_NAME = CFG.get("TABLE_NAME", "dbo.marcaje")
TABLE_NAME = ".".join([f"[{part}]" for part in RAW_TABLE_NAME.replace("[", "").replace("]", "").split(".")])

DUPLICATE_MINUTES = int(CFG.get("DUPLICATE_MINUTES", "2"))
API_SECRET_KEY = CFG.get("API_SECRET_KEY", "cambiar_este_token_por_uno_seguro")

file_lock = asyncio.Lock()
DB_ENGINE: Optional[Engine] = None

# ============================================================
# INSTALACIÓN AUTOMÁTICA DEL DRIVER ODBC
# ============================================================
def verificar_e_instalar_odbc():
    """Verifica si los drivers ODBC 18 o 17 existen; si no, ejecuta el instalador MSI silencioso."""
    drivers_instalados = pyodbc.drivers()
    logger.info(f"Drivers ODBC detectados en el sistema: {drivers_instalados}")
    
    driver_18 = "ODBC Driver 18 for SQL Server"
    driver_17 = "ODBC Driver 17 for SQL Server"
    
    if driver_18 in drivers_instalados or driver_17 in drivers_instalados:
        logger.info("Driver ODBC para SQL Server ya instalado correctamente.")
        return

    logger.warning("No se detectó un driver ODBC válido para SQL Server. Intentando instalación automática...")

    base_path = getattr(sys, '_MEIPASS', obtener_directorio_config())
    msi_path = os.path.join(base_path, "msodbcsql18_x64.msi")
    if not os.path.exists(msi_path):
        msi_path = os.path.join(base_path, "msodbcsql17_x64.msi")

    if os.path.exists(msi_path):
        try:
            logger.info(f"Ejecutando instalador silencioso desde: {msi_path}")
            comando = f'msiexec /i "{msi_path}" /qn IACCEPTMSODBCSQLLICENSERTERMS=YES'
            subprocess.run(comando, shell=True, check=True, capture_output=True, text=True)
            logger.info("Instalación del driver ODBC finalizada exitosamente.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error al intentar instalar el driver ODBC (Código {e.returncode}): {e.stderr}")
        except Exception as e:
            logger.error(f"Excepción inesperada instalando el driver ODBC: {e}")
    else:
        logger.error(f"No se encontró el instalador MSI en {msi_path}.")

# ============================================================
# RESPALDO LOCAL TXT
# ============================================================
def _escribir_archivo(ruta: str, linea: str):
    with open(ruta, "a", encoding="utf-8") as f:
        f.write(linea)

async def guardar_respaldo_local_txt(id_empleado: str, fecha_hora: datetime, ip_dispositivo: str, origen: str):
    archivo_respaldo = obtener_ruta_respaldo()
    linea = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | EMP: {id_empleado} | FECHA_HORA: {fecha_hora.strftime('%Y-%m-%d %H:%M:%S')} | IP: {ip_dispositivo} | ORIGEN: {origen}\n"
    
    async with file_lock:
        try:
            await asyncio.to_thread(_escribir_archivo, archivo_respaldo, linea)
            logger.info(f"Respaldo TXT guardado para {id_empleado}")
        except Exception as e:
            logger.error(f"Error escribiendo en el respaldo TXT: {e}")

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
    if not valor: return None
    try:
        dt = datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except ValueError:
        return None

# ============================================================
# LÓGICA DE BASE DE DATOS SQL SERVER
# ============================================================
def _operacion_db_sync(id_empleado: str, fecha_hora: datetime, ip_dispositivo: str, origen: str) -> bool:
    if not DB_ENGINE:
        logger.error("Sin conexión a la base de datos SQL Server.")
        return False

    fecha_solo = fecha_hora.date()
    hora_solo = fecha_hora.strftime("%H:%M:%S")
    
    if fecha_hora.time() <= time(6, 0, 0):
        fecha_solo -= timedelta(days=1)
    dia_anterior_valor = 1 if fecha_solo < date.today() else 0

    limite_tiempo = fecha_hora - timedelta(minutes=DUPLICATE_MINUTES)

    try:
        with DB_ENGINE.begin() as conn:
            query_dup = text(f"""
                SELECT TOP 1 1 
                FROM {TABLE_NAME} 
                WHERE id_empleado = :id_empleado 
                  AND fecha_hora >= :limite_tiempo
                  AND fecha_hora <= :fecha_hora
            """)
            duplicado = conn.execute(query_dup, {
                "id_empleado": id_empleado,
                "limite_tiempo": limite_tiempo,
                "fecha_hora": fecha_hora
            }).fetchone()

            if duplicado:
                logger.warning(f"Marcaje duplicado omitido -> Emp: {id_empleado} | Hora: {fecha_hora}")
                return False

            query_insert = text(f"""
                INSERT INTO {TABLE_NAME} (id_empleado, fecha_hora, fecha, hora, ip_dispositivo, origen, dia_anterior)
                VALUES (:id_empleado, :fecha_hora, :fecha, :hora, :ip_dispositivo, :origen, :dia_anterior)
            """)
            conn.execute(query_insert, {
                "id_empleado": id_empleado,
                "fecha_hora": fecha_hora,
                "fecha": fecha_solo,
                "hora": hora_solo,
                "ip_dispositivo": ip_dispositivo,
                "origen": origen,
                "dia_anterior": dia_anterior_valor
            })

        logger.info(f"Marcaje registrado en SQL Server -> Emp: {id_empleado} | {fecha_solo} {hora_solo} | Origen: {origen}")
        return True
    except Exception as e:
        logger.error(f"Error procesando transacción en SQL Server: {e}")
        return False

async def procesar_y_guardar_evento(body_data: dict, origen="PULL") -> bool:
    id_empleado = str(buscar_en_datos(body_data, ["employeeNoString", "employeeNo"]) or "").strip()
    if not id_empleado: 
        return False
        
    fecha_hora = parsear_fecha_hora(buscar_en_datos(body_data, ["dateTime", "time"])) or datetime.now()
    ip_dispositivo = body_data.get("ipAddress") or BIOMETRIC_IP
    
    procesado = await asyncio.to_thread(_operacion_db_sync, id_empleado, fecha_hora, ip_dispositivo, origen)
    
    if procesado:
        await guardar_respaldo_local_txt(id_empleado, fecha_hora, ip_dispositivo, origen)
        
    return procesado

# ============================================================
# RECEPCIÓN EN TIEMPO REAL (ISAPI HIKVISION)
# ============================================================
async def escuchar_stream_en_vivo():
    url_stream = f"http://{BIOMETRIC_IP}/ISAPI/Event/notification/alertStream"
    auth = httpx.DigestAuth(BIOMETRIC_USER, BIOMETRIC_PASS)
    
    while True:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=None)) as client:
                async with client.stream("GET", url_stream, auth=auth) as response:
                    response.raise_for_status()
                    logger.info("Conexión en TIEMPO REAL con Biométrico Hikvision establecida.")
                    
                    buffer = ""
                    async for chunk in response.aiter_text():
                        if not chunk: continue
                        buffer += chunk
                        
                        while "{" in buffer and "}" in buffer:
                            inicio = buffer.find("{")
                            fin = buffer.find("}", inicio)
                            if fin != -1:
                                posible_json = buffer[inicio:fin+1]
                                try:
                                    data = json.loads(posible_json)
                                    evento = buscar_en_datos(data, ["AccessControllerEvent", "AcsEvent"]) or data
                                    asyncio.create_task(procesar_y_guardar_evento(evento, origen="STREAM"))
                                    buffer = buffer[fin+1:]
                                except json.JSONDecodeError:
                                    siguiente_fin = buffer.find("}", fin + 1)
                                    if siguiente_fin != -1:
                                        fin = siguiente_fin
                                    else:
                                        break
                            else:
                                break
                                
                        if len(buffer) > 100000:
                            buffer = ""
                            
        except httpx.ReadTimeout:
            logger.warning("Reconectando stream Hikvision por timeout...")
        except asyncio.CancelledError:
            logger.info("Stream ISAPI detenido correctamente.")
            break
        except Exception as e:
            logger.error(f"Error en recepción en tiempo real: {e}. Reintentando en 5 segundos...")
            await asyncio.sleep(5)

async def sincronizar_marcajes_biometrico():
    auth = httpx.DigestAuth(BIOMETRIC_USER, BIOMETRIC_PASS)
    url = f"http://{BIOMETRIC_IP}/ISAPI/AccessControl/AcsEvent?format=json"
    
    async with httpx.AsyncClient() as client:
        while True:
            try:
                ahora = datetime.now()
                payload = {
                    "AcsEventCond": {
                        "searchID": "1", "searchResultPosition": 0, "maxResults": 100,
                        "major": 5, "minor": 0,
                        "startTime": (ahora - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S"),
                        "endTime": (ahora + timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S")
                    }
                }
                
                response = await client.post(url, json=payload, auth=auth, timeout=10)
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
    user_encoded = urllib.parse.quote_plus(DB_USER)
    pass_encoded = urllib.parse.quote_plus(DB_PASS)
    
    db_url = f"mssql+pyodbc://{user_encoded}:{pass_encoded}@{DB_HOST}/{DB_NAME}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    
    try:
        DB_ENGINE = create_engine(db_url, pool_size=10, max_overflow=20, pool_pre_ping=True, pool_recycle=3600)
        with DB_ENGINE.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexión con SQL Server (ODBC 18) verificada con éxito.")
    except Exception as e:
        logger.warning(f"Error conectando con ODBC 18: {e}. Intentando con ODBC 17...")
        try:
            db_url_17 = f"mssql+pyodbc://{user_encoded}:{pass_encoded}@{DB_HOST}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
            DB_ENGINE = create_engine(db_url_17, pool_size=10, max_overflow=20, pool_pre_ping=True, pool_recycle=3600)
            with DB_ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Conexión con SQL Server (ODBC 17) verificada.")
        except Exception as ex:
            logger.error(f"Error crítico al conectar a SQL Server: {ex}")

    task_pull = asyncio.create_task(sincronizar_marcajes_biometrico())
    task_stream = asyncio.create_task(escuchar_stream_en_vivo())
    logger.info("Servicios de monitoreo activados.")
    
    yield
    
    task_pull.cancel()
    task_stream.cancel()
    await asyncio.gather(task_pull, task_stream, return_exceptions=True)
    
    if DB_ENGINE:
        DB_ENGINE.dispose()
        logger.info("Motor de base de datos liberado.")

app = FastAPI(title="Hikvision Attendance API", lifespan=lifespan)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verificar_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Acceso denegado")

@app.post("/api/v1/hikvision/webhook")
async def recibir_webhook(request: Request, _=Depends(verificar_api_key)):
    try:
        body = await request.json()
        evento = body.get("AccessControllerEvent", body)
        asyncio.create_task(procesar_y_guardar_evento(evento, origen="WEBHOOK"))
        return Response(content="Event accepted", status_code=202)
    except json.JSONDecodeError:
        return Response(content="Invalid JSON", status_code=400)

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
        "ipAddress": BIOMETRIC_IP
    }
    resultado = await procesar_y_guardar_evento(payload_simulado, origen="PRUEBA_MANUAL")
    return {
        "status": "procesado" if resultado else "ignorado (duplicado o error BD)",
        "empleado": id_empleado,
        "fecha_hora": datetime.now().isoformat()
    }

if __name__ == "__main__":
    multiprocessing.freeze_support()
    verificar_e_instalar_odbc()
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT, reload=False)