import time
import httpx
import asyncio

URL = "http://localhost:8000/api/v1/hikvision/webhook"
HEADERS = {
    "X-API-Key": "12345",
    "Content-Type": "application/json"
}

PAYLOAD = {
    "AccessControllerEvent": {
        "employeeNoString": "EMP_STRESS_TEST",
        "dateTime": "2026-09-16T14:00:00",
        "ipAddress": "192.168.20.215"
    }
}

async def enviar_peticion(client, index):
    inicio = time.time()
    try:
        # Modificamos ligeramente el ID para simular diferentes empleados o probar duplicados
        PAYLOAD["AccessControllerEvent"]["employeeNoString"] = f"EMP_{index % 10}"
        response = await client.post(URL, json=PAYLOAD, headers=HEADERS, timeout=10.0)
        duracion = time.time() - inicio
        return response.status_code, duracion
    except Exception as e:
        return 0, 0.0

async def main():
    total_peticiones = 1000
    concurrencia = 50
    
    print(f"Iniciando prueba de estrés: {total_peticiones} peticiones con concurrencia de {concurrencia}...")
    
    inicio_total = time.time()
    
    async with httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(concurrencia)
        
        async def sem_task(i):
            async with semaphore:
                return await enviar_peticion(client, i)
        
        tareas = [sem_task(i) for i in range(total_peticiones)]
        resultados = await asyncio.gather(*tareas)
        
    tiempo_total = time.time() - inicio_total
    
    # Analizar resultados
    status_codes = {}
    tiempos = []
    exitosas = 0
    
    for status, duracion in resultados:
        status_codes[status] = status_codes.get(status, 0) + 1
        if status > 0:
            tiempos.append(duracion)
        if status == 202:
            exitosas += 1

    print("\n--- RESULTADOS DE LA PRUEBA DE ESTRÉS ---")
    print(f"Tiempo total: {tiempo_total:.2f} segundos")
    print(f"Peticiones por segundo: {total_peticiones / tiempo_total:.2f} req/s")
    print(f"Códigos de estado recibidos: {status_codes}")
    if tiempos:
        print(f"Tiempo promedio de respuesta: {(sum(tiempos) / len(tiempos)) * 1000:.2f} ms")

if __name__ == "__main__":
    asyncio.run(main())