from locust import HttpUser, task, between

class HikvisionUser(HttpUser):
    wait_time = between(0.01, 0.1)  # Prácticamente sin pausa entre peticiones

    @task
    def enviar_webhook_estres(self):
        headers = {"X-API-Key": "12345", "Content-Type": "application/json"}
        payload = {
            "AccessControllerEvent": {
                "employeeNoString": "EMP_STRESS_01",
                "dateTime": "2026-09-16T12:00:00",
                "ipAddress": "192.168.20.215"
            }
        }
        self.client.post("/api/v1/hikvision/webhook", json=payload, headers=headers)