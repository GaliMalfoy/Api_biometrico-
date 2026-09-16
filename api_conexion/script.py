import os
import sys
from datetime import datetime

# 1. Definición de las funciones de ruta y guardado
def obtener_directorio_config():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def obtener_ruta_respaldo():
    """Crea la estructura output/main/ si no existe y devuelve la ruta al TXT."""
    carpeta_destino = os.path.join(obtener_directorio_config(), "output", "main")
    os.makedirs(carpeta_destino, exist_ok=True)
    return os.path.join(carpeta_destino, "respaldo_marcajes.txt")

def probar_guardado_respaldo(id_empleado: str, origen: str = "PRUEBA_LOCAL"):
    ruta_archivo = obtener_ruta_respaldo()
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    linea = f"{ahora} | EMP: {id_empleado} | FECHA_HORA: {ahora} | IP: 127.0.0.1 | ORIGEN: {origen}\n"
    
    try:
        with open(ruta_archivo, "a", encoding="utf-8") as f:
            f.write(linea)
        print(f"✅ ¡Éxito! Registro escrito en:\n   {ruta_archivo}")
    except Exception as e:
        print(f"❌ Error al escribir: {e}")

# 2. Ejecución de la prueba
if __name__ == "__main__":
    print("Iniciando prueba de escritura en carpeta output/main/...")
    probar_guardado_respaldo(id_empleado="103", origen="TEST_MANUAL")