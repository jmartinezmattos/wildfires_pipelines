import time

def wait_for_task(task, poll=10):
    while True:
        status = task.status()
        state = status["state"]

        if state == "COMPLETED":
            return True
        elif state in ["FAILED", "CANCELLED"]:
            print("Task failed:", status.get("error_message"))
            return False

        time.sleep(poll)