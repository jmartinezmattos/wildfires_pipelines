import ee
import time

ee.Authenticate()
ee.Initialize(project="cellular-retina-276416")

gaul = ee.FeatureCollection("FAO/GAUL/2015/level0")
uruguay = gaul.filter(ee.Filter.eq("ADM0_NAME", "Uruguay")).geometry()

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