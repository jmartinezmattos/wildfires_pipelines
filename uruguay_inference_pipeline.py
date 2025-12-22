from inference import inference
from uruguay_tiles import get_uruguay_tiles


def inference_pipeline():

    tiles_path=get_uruguay_tiles()
    
    inference(images_dir=tiles_path)

if __name__ == "__main__":
    inference_pipeline()