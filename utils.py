import ee
import time
import subprocess
import os

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

def move_data_from_gcs_to_local(bucket_path_lists, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    
    for gcs_path in bucket_path_lists:
        cmd = ["gsutil", "-m" , "cp", "-r", gcs_path, local_dir]
        
        try:
            subprocess.run(cmd, check=True, shell=True)
            print(f"File downloaded: {gcs_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error downloading {gcs_path}: {e}")

def move_data_from_local_to_gcs(local_path, gcs_bucket_path):
    cmd = ["gsutil", "-m" , "cp", "-r", local_path, gcs_bucket_path]
    
    try:
        subprocess.run(cmd, check=True, shell=True)
        print(f"File uploaded: {local_path} to {gcs_bucket_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error uploading {local_path}: {e}")

    return gcs_bucket_path + "/" + os.path.basename(local_path)