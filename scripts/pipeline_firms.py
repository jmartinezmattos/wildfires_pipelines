import os
import tqdm
import pandas as pd
from datetime import datetime
from inference import inference
from firms_alerts import firms_alerts_by_dates
from image_from_coordinates import download_image_from_coordinates
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_datetime_from_firms_row(row):
    
    acq_date = row['acq_date']
    acq_time = str(row['acq_time']).zfill(4)
    firms_datetime = f"{acq_date}T{acq_time[:2]}{acq_time[2:]}:00"

    return firms_datetime

def download_images_for_firms_alerts_parallel(alerts):
    output_dir = f"./data/wildfire_rgb_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    os.makedirs(output_dir, exist_ok=True)
    
    args_list = [(row['latitude'], row['longitude'], get_datetime_from_firms_row(row), output_dir) for _, row in alerts.iterrows()]

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(download_image_from_coordinates, *args) for args in args_list]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error downloading image: {e}")

    return output_dir

def download_images_for_firms_alerts(alerts):

    output_dir = f"./data/wildfire_rgb_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    for _, row in tqdm.tqdm(alerts.iterrows(), total=alerts.shape[0], desc="Downloading images"):
        
        lat = row['latitude']
        lon = row['longitude']
        firms_datetime = get_datetime_from_firms_row(row)
        
        download_image_from_coordinates(
            lat=lat,
            lon=lon,
            firms_datetime=firms_datetime,
            output_dir=output_dir,
            satellite="sentinel-2",
            format="PNG",
            copy_to_gcs=False,
        )

    return output_dir

def firms_pipeline():

    dates = ["2025-01-01", "2025-01-06", "2025-01-11", "2025-01-16", "2025-01-21", "2025-01-26", "2025-01-31"]

    dates = ["2025-01-02", "2025-01-07", "2025-01-12", "2025-01-17", "2025-01-22", "2025-01-27", "2025-02-01"]

    #dates = ["today", "yesterday"]

    firms_files = firms_alerts_by_dates(dates)

    dfs = [pd.read_csv(f) for f in firms_files]

    all_alerts = pd.concat(dfs, ignore_index=True)

    images_dir = download_images_for_firms_alerts_parallel(all_alerts)

    print(f"Images downloaded to: {images_dir}")

    inferences_path = inference(images_dir=images_dir)

    print(f"Inferences saved at: {inferences_path}")

if __name__ == "__main__":

    firms_pipeline()