from datetime import datetime
import os
import pytest
from fastapi.testclient import TestClient

# Importamos la aplicación y funciones del archivo principal (asumiendo que se llama main.py)
from main import (
    API_SECRET_KEY,
    app,
    buscar_en_datos,
    parsear_fecha_hora,
    obtener_ruta_respaldo,
)

client = TestClient(app)
HEADERS = {"X-API-Key": API_SECRET_KEY}


# ============================================================
# 1. PRUEBAS DE UTILIDADES Y PARSEO
# ============================================================
def test_parsear_fecha_hora_valida():
    val_iso = "2026-06-06T10:30:00Z"
    dt = parsear_fecha_hora(val_iso)
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 6
    assert dt.hour == 10


def test_parsear_fecha_hora_invalida():
    assert parsear_fecha_hora("texto-invalido") is None
    assert parsear_fecha_hora(None) is None


def test_buscar_en_datos_anidados():
    data = {
        "AccessControllerEvent": {
            "employeeNoString": "12345",
            "dateTime": "2026-06-06T08:00:00",
        }
    }
    resultado = buscar_en_datos(
        data, ["employeeNoString", "employeeNo"]
    )
    assert resultado == "12345"


# ============================================================
# 2. PRUEBAS DE SEGURIDAD (API KEY)
# ============================================================
def test_webhook_sin_api_key():
    response = client.post(
        "/api/v1/hikvision/webhook", json={"employeeNoString": "999"}
    )
    assert response.status_code == 403


def test_webhook_con_api_key_invalida():
    response = client.post(
        "/api/v1/hikvision/webhook",
        headers={"X-API-Key": "clave_falsa"},
        json={"employeeNoString": "999"},
    )
    assert response.status_code == 403


# ============================================================
# 3. PRUEBAS DE ENDPOINTS (WEBHOOK Y RESPALDO)
# ============================================================
def test_webhook_recibe_evento():
    payload = {
        "AccessControllerEvent": {
            "employeeNoString": "EMP001",
            "dateTime": datetime.now().isoformat(),
            "ipAddress": "192.168.20.215",
        }
    }
    response = client.post(
        "/api/v1/hikvision/webhook", headers=HEADERS, json=payload
    )
    # FastAPI responde con 202 Accepted cuando encola la tarea en background
    assert response.status_code == 202
    assert response.content == b"Event accepted"


def test_obtener_reporte_respaldo():
    response = client.get(
        "/api/v1/hikvision/reporte-respaldo", headers=HEADERS
    )
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert data["status"] == "ok"
    assert "total_registros" in data
    assert isinstance(data["lineas"], list)