import os
import ee
import datetime
import requests
from dotenv import load_dotenv
from utils import wait_for_task
from datetime import timezone
import subprocess

ee.Authenticate()
ee.Initialize(project="cellular-retina-276416")

load_dotenv(".env")
BUCKET = os.getenv("BUCKET_NAME")
BUFFER_METERS_SENTINEL = int(os.getenv("BUFFER_METERS_SENTINEL", "2000"))
THUMB_SIZE = int(os.getenv("THUMB_SIZE", "1024"))
BUCKET_NAME = os.getenv("BUCKET_NAME")

SATELLITE_LIST=["landsat-8", "sentinel-2", "aqua"]

def download_image_from_coordinates(lat, lon, firms_datetime, output_dir, satellite="sentinel-2", format="PNG", copy_to_gcs=True, time_widnow_hours=10):
    point = ee.Geometry.Point([lon, lat])

    if satellite == "landsat-8":
        buffer_m = 3000
        scale = 30
    elif satellite == "sentinel-2":
        buffer_m = 2000
        scale = 10
    elif satellite == "aqua":
        buffer_m = 5000
        scale = 500
    elif satellite == "fengyun":
        buffer_m = 5000
        scale = 1000
    else:
        raise ValueError(f"Satellite not supported: {satellite}")

    region = point.buffer(buffer_m).bounds()

    alert_dt = datetime.datetime.strptime(firms_datetime, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    min_dt = alert_dt - datetime.timedelta(hours=time_widnow_hours)
    max_dt = alert_dt + datetime.timedelta(hours=time_widnow_hours)

    collection = get_collection_from_coordinates(min_dt, max_dt, point, satellite=satellite)

    if collection is None:
        print(f"No images found for {lon}, {lat} at {alert_dt} in {satellite}.")
        return None

    size = collection.size().getInfo()
    if size == 0:
        print("No images found.")
        return None

    image = ee.Image(collection.first())

    if satellite == "landsat-8":
        bands = ['SR_B4', 'SR_B3', 'SR_B2']
        image = image.select(bands).multiply(0.0000275).add(-0.2)
    elif satellite == "sentinel-2":
        bands = ['B4', 'B3', 'B2']
        image = image.select(bands)
    elif satellite == "aqua":
        bands = ['sur_refl_b01','sur_refl_b04','sur_refl_b03']
        image = image.select(bands)
    elif satellite == "fengyun":
        bands = ['Channel0001','Channel0002','Channel0003']
        image = image.select(bands)

    image_time = ee.Date(image.get('system:time_start')).format('YYYYMMdd_HHmmss').getInfo()

    prefix = f"wildfire_rgb_{satellite}_{lat}_{lon}_{image_time}"

    if format.lower() == "tiff":

        task = ee.batch.Export.image.toCloudStorage(
            image=image,
            description=f"EXPORT_{prefix}",
            bucket=BUCKET,
            fileNamePrefix=prefix,
            region=region,
            scale=scale,
            crs="EPSG:4326",
            fileFormat="GeoTIFF",
            maxPixels=1e13
        )

        task.start()
        print("Export started to GCS…")

        success = wait_for_task(task)

        if not success:
            print("Export failed.")
            return None

        gcs_path = f"gs://{BUCKET}/{prefix}.tif"
        print("Export completed:", gcs_path)

        return gcs_path

    elif format.lower() == "png":

        png_url = image.visualize(
            bands=bands,
            min=0,
            max=3000
        ).getThumbURL({
            "region": region,
            "scale": scale,
            "crs": "EPSG:4326",
            "format": "png"
        })

        print("Downloading PNG from:")
        print(png_url)

        os.makedirs(output_dir, exist_ok=True)
        png_local_path = f"{output_dir}/{prefix}.png"

        r = requests.get(png_url)
        r.raise_for_status()
        with open(png_local_path, "wb") as f:
            f.write(r.content)

        print("PNG saved locally as:", png_local_path)

        if copy_to_gcs:
            gcs_dir = f"gs://{BUCKET_NAME}/firms_alerts/"

            cmd = ["gsutil", "cp", png_local_path, gcs_dir]
            print("Running command: ", " ".join(cmd))
            gcs_path = gcs_dir + png_local_path
            try:
                subprocess.run(cmd, check=True, shell=True)
                print(f"File uploaded: {gcs_path}")
            except subprocess.CalledProcessError as e:
                print(f"Error uploading {gcs_path}: {e}")

        return png_local_path

def get_collection_from_coordinates(alert_dt, max_dt, point, satellite="sentinel-2"):

    if satellite == "sentinel-2":
        collection_string = "COPERNICUS/S2_SR_HARMONIZED"
        cloud_property = "CLOUDY_PIXEL_PERCENTAGE"
        # cloud_filter = ee.Filter.lt(cloud_property, CLOUD_FILTER_PERCENTAGE)
    elif satellite == "landsat-8":
        collection_string = "LANDSAT/LC08/C02/T1_L2"
        # cloud_filter = ee.Filter.lt('CLOUD_COVER', CLOUD_FILTER_PERCENTAGE)
    elif satellite == "aqua":
        collection_string = "MODIS/061/MYD09GA"
        cloud_filter = None
    elif satellite == "fengyun":
        collection_string = "CMA/FY4A/AGRI/L1"
        cloud_filter = None
    else:
        raise ValueError(f"Satellite not supported: {satellite}")

    collection = ee.ImageCollection(collection_string).filterBounds(point).filterDate(alert_dt, max_dt)

    if collection.size().eq(0).getInfo():
        return None

    #if cloud_filter:
    #    collection = collection.filter(cloud_filter)

    collection = collection.sort('system:time_start')

    return collection

def test():
    lat, lon = -34.5338,-56.2831

    s = "2024-01-20T17:57:00"
    
    for satellite in SATELLITE_LIST:
        print(f"Testing export for satellite: {satellite}")
        download_image_from_coordinates(lat, lon, s, satellite, format="PNG")


if __name__ == "__main__":
    test()
