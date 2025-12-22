import os
import shutil
from inference import inference
from uruguay_tiles import get_uruguay_tiles
from utils import move_data_from_local_to_gcs

OUTPUT_BUCKET_PATH = "gs://wildfires_data_um/inferences"

def delete_local_files(paths):
    for path in paths:
        if not os.path.exists(path):
            print(f"Path not found: {path}")
            continue

        if os.path.isfile(path):
            os.remove(path)
            print(f"Deleted file: {path}")

        elif os.path.isdir(path):
            shutil.rmtree(path)
            print(f"Deleted directory: {path}")

def inference_pipeline():

    tiles_path=get_uruguay_tiles(max_tiles=50)
    
    inferences_path = inference(images_dir=tiles_path)

    gcs_output_path = move_data_from_local_to_gcs(inferences_path, OUTPUT_BUCKET_PATH)

    print(f"Inferences saved at: {gcs_output_path}")

    delete_local_files([tiles_path, inferences_path])

if __name__ == "__main__":
    inference_pipeline()