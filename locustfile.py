from locust import HttpUser, task, between
import time


class LROUser(HttpUser):
    wait_time = between(1, 3)

    @task
    def create_and_poll_task(self):
        # 1. Создаём задачу
        response = self.client.post(
            "/lro-create",
            data={"text": "test task"},
        )

        if response.status_code == 202:
            task_id = response.json()["task_id"]
            owner_id = response.headers.get("Anonymous-Id")

            # 2. Ждём пока воркер обработает
            time.sleep(5)

            # 3. Проверяем статус
            result = self.client.post(
                "/lro-get",
                json={"task_id": task_id},
                headers={"Anonymous-Id": owner_id},
            )

            if result.status_code == 200:
                data = result.json()
                if data.get("status") == 1:  # DONE_USUAL
                    print(f"Task {task_id} completed successfully")
                else:
                    print(f"Task {task_id} status: {data.get('status')}")
