import asyncio
import httpx
import time

# URL de tu API (ajustada al endpoint de simulación)
URL = "http://localhost:8000/api/v1/hikvision/simular-marcaje?id_empleado=999"

# Headers asegurando que todos los valores sean estrictamente strings (str)
HEADERS = {
    "X-API-Key": str("12345")
}

TOTAL_PETICIONES = 100  # Cantidad total de peticiones a enviar
CONCURRENCIA = 80     # Peticiones simultáneas

async def hacer_peticion(client, semaphore, idx):
    async with semaphore:
        inicio = time.time()
        try:
            response = await client.post(URL, headers=HEADERS, timeout=10.0)
            duracion = time.time() - inicio
            print(f"[{idx}] Estado: {response.status_code} | Tiempo: {duracion:.2f}s")
        except Exception as e:
            print(f"[{idx}] Error: {e}")

async def main():
    semaphore = asyncio.Semaphore(CONCURRENCIA)
    async with httpx.AsyncClient() as client:
        print(f"Iniciando prueba de estrés: {TOTAL_PETICIONES} peticiones (Concurrencia: {CONCURRENCIA})...")
        tiempo_inicio_total = time.time()
        
        tareas = [hacer_peticion(client, semaphore, i) for i in range(TOTAL_PETICIONES)]
        await asyncio.gather(*tareas)
        
        tiempo_total = time.time() - tiempo_inicio_total
        print(f"\nPrueba finalizada en {tiempo_total:.2f} segundos.")

if __name__ == "__main__":
    asyncio.run(main())