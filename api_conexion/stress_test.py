import asyncio
import time
import httpx

# Configuración de la prueba de estrés
URL = "http://localhost:8000/api/v1/hikvision/marcaje-manual"
HEADERS = {
    "X-API-Key": "12345",
    "Content-Type": "application/json"
}


async def enviar_peticion(client: httpx.AsyncClient, index: int, semaphore: asyncio.Semaphore):
    # Payload local para evitar condiciones de carrera en concurrencia
    payload = {
        "AccessControllerEvent": {
            "employeeNoString": f"EMP_{1000 + (index % 100)}",
            "dateTime": "2026-09-16T14:00:00",
            "ipAddress": "192.168.20.215"
        }
    }

    async with semaphore:
        inicio = time.perf_counter()
        try:
            response = await client.post(URL, json=payload, headers=HEADERS, timeout=10.0)
            duracion = time.perf_counter() - inicio
            return response.status_code, duracion
        except Exception as e:
            return 0, 0.0


async def main():
    total_peticiones = 1000
    concurrencia = 50

    print(f"🚀 Iniciando prueba de estrés:")
    print(f"   - Target: {URL}")
    print(f"   - Peticiones totales: {total_peticiones}")
    print(f"   - Concurrencia: {concurrencia}\n")

    # Configuración de pool de conexiones optimizada para estrés
    limits = httpx.Limits(max_keepalive_connections=concurrencia, max_connections=concurrencia * 2)
    semaphore = asyncio.Semaphore(concurrencia)

    inicio_total = time.perf_counter()

    async with httpx.AsyncClient(limits=limits) as client:
        tareas = [enviar_peticion(client, i, semaphore) for i in range(total_peticiones)]
        resultados = await asyncio.gather(*tareas)

    tiempo_total = time.perf_counter() - inicio_total

    # Métricas y estadísticas
    status_codes = {}
    tiempos = []

    for status, duracion in resultados:
        status_codes[status] = status_codes.get(status, 0) + 1
        if status > 0:
            tiempos.append(duracion)

    # Acepta tanto 200 OK como 202 Accepted como exitosas
    exitosas = status_codes.get(200, 0) + status_codes.get(202, 0)
    fallidas = total_peticiones - exitosas

    print("=" * 45)
    print("      📊 RESULTADOS DE LA PRUEBA DE ESTRÉS")
    print("=" * 45)
    print(f"⏱️  Tiempo total:                {tiempo_total:.2f} segundos")
    print(f"⚡ Peticiones por segundo (RPS): {total_peticiones / tiempo_total:.2f} req/s")
    print(f"✅ Peticiones exitosas (200/202): {exitosas}")
    print(f"❌ Peticiones fallidas:           {fallidas}")
    print(f"📋 Desglose de HTTP Status:      {status_codes}")

    if tiempos:
        promedio_ms = (sum(tiempos) / len(tiempos)) * 1000
        min_ms = min(tiempos) * 1000
        max_ms = max(tiempos) * 1000
        print(f"📈 Latencia Promedio:            {promedio_ms:.2f} ms")
        print(f"🚀 Latencia Mínima:              {min_ms:.2f} ms")
        print(f"🐢 Latencia Máxima:              {max_ms:.2f} ms")
    print("=" * 45)


if __name__ == "__main__":
    asyncio.run(main())