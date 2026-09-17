import asyncio
import time
import httpx

URL = "http://127.0.0.1:8000/api/v1/hikvision/simular-marcaje?id_empleado=TEST"
HEADERS = {"X-API-Key": "12345"}  # Ajusta a tu API_SECRET_KEY

async def hacer_peticion(client, i, sem):
    async with sem:
        try:
            r = await client.post(URL, headers=HEADERS, timeout=10.0)
            if r.status_code == 200:
                print(f"[{i}] OK: {r.json()}")
            else:
                print(f"[{i}] HTTP {r.status_code}: {r.text}")
        except Exception as e:
            print(f"[{i}] Error: {e}")

async def main():
    print("Iniciando prueba de estrés...")
    inicio = time.time()
    
    # Control de concurrencia explícito
    sem = asyncio.Semaphore(50) 
    
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
    async with httpx.AsyncClient(limits=limits) as client:
        tareas = [hacer_peticion(client, i, sem) for i in range(50)]
        await asyncio.gather(*tareas)
        
    fin = time.time()
    print(f"Prueba finalizada en {fin - inicio:.2f} segundos.")

if __name__ == "__main__":
    asyncio.run(main())