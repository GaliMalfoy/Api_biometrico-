# API Biométrico - Integración Hikvision

Backend desarrollado en **FastAPI** para la recepción, procesamiento y almacenamiento en tiempo real de eventos de dispositivos de control de acceso biométrico **Hikvision** (compatible con modelos como la serie DS-K1T8003) mediante **ISAPI** y escuchas HTTP (*HTTP Listeners*).

## 🚀 Características Principales

* **Recepción en Tiempo Real:** Captura de eventos biométricos (asistencia, accesos correctos/denegados, aperturas de puerta) enviados por los dispositivos.
* **Autenticación Segura:** Manejo de autenticación Digest (*Digest Authentication*) para la comunicación con el hardware.
* **Arquitectura Dual de Base de Datos:** Cuenta con puntos de entrada separados: `main.py` para **SQL Server** y `main2.py` para **PostgreSQL**.
* **Empaquetado Independiente:** Configurado para compilarse como un ejecutable autónomo mediante PyInstaller.
* **Pruebas Automatizadas:** Suite de pruebas unitarias implementada con `pytest`.

## 🛠️ Tecnologías Utilizadas

* **Python** 
* **FastAPI** (Framework web asíncrono)
* **Uvicorn** (Servidor ASGI)
* **PostgreSQL / SQL Server** (Gestión de bases de datos)
* **PyTest** (Testing)
* **PyInstaller** (Generación de ejecutables)

---

## 📋 Requisitos Previos

Asegúrate de tener instalado en tu entorno de desarrollo:
* Python 3.10 o superior.
* Gestor de paquetes `pip`.
**ODBC Driver for SQL Server:** Obligatorio si vas a utilizar el módulo o variante de SQL Server para que el driver de conexión funcione correctamente en Windows/Linux.
---

## ⚙️ Instalación y Configuración

1. **Clona el repositorio:**
   ```bash
   git clone [https://github.com/GaliMalfoy/Api_biometrico-.git](https://github.com/GaliMalfoy/Api_biometrico-.git)
   cd Api_biometrico-



2. Crea y activa un entorno virtual:
Bash

python -m venv venv
# En Windows:
venv\Scripts\activate
# En Linux/Mac:
source venv/bin/activate



3. Instala las dependencias:
  pip install -r requirements.txt


4. Configura las variables de entorno:
Crea un archivo .env en la raíz del proyecto basándote en la configuración de tus conexiones de base de datos y parámetros del servidor.

   🏃‍♂️ Ejecución de la Aplicación

   Para iniciar el servidor en modo de desarrollo local, ejecuta:

   uvicorn main:app --reload --host 0.0.0.0 --port 8000

La documentación interactiva de la API (Swagger UI) estará disponible en:
http://localhost:8000/docs


🧪 Ejecutar Pruebas

Para correr la suite de pruebas unitarias:

pytest

💡 Conexión con Dispositivos Hikvision}

Configura la dirección IP de tu dispositivo biométrico Hikvision para que apunte al servidor donde corre esta API utilizando el protocolo ISAPI (Notificaciones de eventos vía HTTP Host / Listener).
