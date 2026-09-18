import asyncio
import httpx

# Configuración de prueba
BASE_URL = "http://127.0.0.1:8000"
API_KEY = "12345"  # Debe coincidir con la clave configurada en la API


async def enviar_marcaje(client: httpx.AsyncClient, idx: int) -> int:
    headers = {"X-API-Key": API_KEY}
    url = f"{BASE_URL}/api/v1/hikvision/simular-marcaje?id_empleado={1000 + idx}&id_tienda=1"
    try:
        resp = await client.post(url, headers=headers)
        return resp.status_code
    except httpx.RequestError as exc:
        print(f"Error de red en la petición {idx}: {exc}")
        return 0


async def main():
    # Límite de conexiones para la prueba de carga
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)

    async with httpx.AsyncClient(limits=limits, timeout=10.0) as client:
        # Dispara 30 peticiones concurrentes con distintos IDs de empleado
        tareas = [enviar_marcaje(client, i) for i in range(30)]
        resultados = await asyncio.gather(*tareas)

        exitosas = resultados.count(200)
        fallidas = len(resultados) - exitosas

        print("=" * 40)
        print(f"Peticiones completadas: {len(resultados)}")
        print(f"Respuestas HTTP 200 (OK): {exitosas}")
        print(f"Respuestas con error/403/500: {fallidas}")
        print("=" * 40)


if __name__ == "__main__":
    asyncio.run(main())