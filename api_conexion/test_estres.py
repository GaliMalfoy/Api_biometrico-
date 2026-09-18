import asyncio
import httpx
import time

# URL exacta del endpoint configurado en tu main.py
URL = "http://localhost:8000/api/v1/hikvision/marcaje-manual"

# Debe coincidir con API_SECRET_KEY de tu config.txt / main.py
API_KEY = "12345" 

HEADERS = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

async def enviar_peticion(client, i):
    payload = {
        "employeeNoString": f"100{i}",
        "dateTime": "2026-09-17T16:00:00Z",
        "ipAddress": "192.168.20.210",
        "id_tienda": "1"
    }
    
    try:
        response = await client.post(URL, json=payload, headers=HEADERS)
        print(f"[{i}] HTTP {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[{i}] Error: {e}")

async def main():
    print("Iniciando prueba de estrés...")
    start_time = time.time()
    
    async with httpx.AsyncClient() as client:
        tasks = [enviar_peticion(client, i) for i in range(50)]
        await asyncio.gather(*tasks)
        
    print(f"Prueba finalizada en {time.time() - start_time:.2f} segundos.")

if __name__ == "__main__":
    asyncio.run(main())