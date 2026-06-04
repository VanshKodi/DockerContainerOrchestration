import os
import time

import requests

backend_url = "http://127.0.0.1:20000/"
auth_token = "changeme"


def handle_process(container: dict):
    print(f"Handle: {container.get('id', '?')} — {container.get('name', '?')}")


def start():
    try:
        resp = requests.get(backend_url, timeout=10)
        resp.raise_for_status()
        print(f"Start: Backend is online — {backend_url} returned {resp.status_code}")
    except requests.RequestException as e:
        print(f"Start: Backend at {backend_url} is unreachable — {e}")

    update()


def update():
    headers = {"Authorization": f"Bearer {auth_token}"}
    try:
        resp = requests.get(f"{backend_url}crud/containers", headers=headers, timeout=10)
        resp.raise_for_status()
        containers = resp.json()
        for container in containers:
            handle_process(container)
    except requests.RequestException as e:
        print(f"Update: Backend at {backend_url} is unreachable — {e}")



def main(interval_seconds: int = 60):
    start()
    while True:
        time.sleep(interval_seconds)
        update()


if __name__ == "__main__":
    main()
