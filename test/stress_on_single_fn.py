from locust import HttpUser, task, between

class MyUser(HttpUser):
    wait_time = between(1, 5)  # 设置每个用户的等待时间范围
    host = "http://127.0.0.1:3002"

    @task
    def my_task(self):
        # self.client.post("/path", json={"key": "value"})  # 发送POST请求
        self.client.post("/fn2")

    