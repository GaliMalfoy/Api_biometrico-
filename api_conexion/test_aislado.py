import os
from datetime import datetime
# Importa tus funciones desde el script principal (asumiendo que se llama main.py)
from main import (
    buscar_en_datos,
    obtener_ruta_respaldo,
    parsear_fecha_hora,
)

def test_parseos():
    print("--- Testing Parseo de Fechas e ISAPI ---")
    
    # 1. Test ISO UTC
    dt1 = parsear_fecha_hora("2026-09-18T10:30:00Z")
    assert dt1 == datetime(2026, 9, 18, 10, 30, 0), "Falló parseo ISO UTC"

    # 2. Test ISO con Offset
    dt2 = parsear_fecha_hora("2026-09-18T10:30:00-04:00")
    assert dt2.hour == 10, "Falló parseo con Offset"

    # 3. Test Búsqueda Anidada en JSON de Hikvision (ISAPI)
    payload_ejemplo = {
        "AccessControllerEvent": {
            "employeeNoString": "10024",
            "subEventType": 25
        }
    }
    emp = buscar_en_datos(payload_ejemplo, ["employeeNoString", "employeeNo"])
    assert emp == "10024", "Falló búsqueda anidada de empleado"

    # 4. Test Generación de Ruta de Respaldo
    ruta_txt = obtener_ruta_respaldo()
    print(f"Ruta calculada para el TXT: {ruta_txt}")
    assert "respaldo_marcajes.txt" in ruta_txt, "Falló la generación de la ruta del TXT"

    print("✅ Todas las pruebas unitarias pasaron correctamente.\n")

if __name__ == "__main__":
    test_parseos()