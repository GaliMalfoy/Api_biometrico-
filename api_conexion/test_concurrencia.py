import httpx
import asyncio

url = "http://localhost:8000/api/v1/hikvision/webhook"
headers = {"X-API-Key": "12345"}
payload = {
    "AccessControllerEvent": {
        "employeeNoString": "EMP_CONCURRENTE_99",
        "dateTime": "2026-09-16T13:00:00",
        "ipAddress": "192.168.20.215"
    }
}

async def enviar_peticion(client):
    response = await client.post(url, json=payload, headers=headers)
    return response.status_code

async def main():
    async with httpx.AsyncClient() as client:
        # Disparar 50 peticiones exactamente IDÉNTICAS al mismo milisegundo
        tareas = [enviar_peticion(client) for _ in range(50)]
        resultados = await asyncio.gather(*tareas)
        print("Resultados de las 50 peticiones concurrentes:", resultados)

asyncio.run(main())