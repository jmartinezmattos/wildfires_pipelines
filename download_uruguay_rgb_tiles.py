
import os
import ee
from dotenv import load_dotenv
from utils import download_thumbnail, uruguay
import requests
from datetime import datetime, timedelta

ee.Authenticate()
ee.Initialize(project="cellular-retina-276416")

load_dotenv(".env")
BUCKET = os.getenv("BUCKET_NAME")

URUGUAY = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017") \
            .filter(ee.Filter.eq("country_na", "Uruguay")) #Check this line

GRID_SIZE_KM = 4  # 4x4 km
GRID_SIZE_DEG = GRID_SIZE_KM / 111  # Aproximación: 1° ~ 111 km

BUFFER_METERS = 2000
THUMB_SIZE = 1024

def download_thumbnail(image, filename, point, satellite="sentinel-2"):

    region = point.buffer(BUFFER_METERS).bounds().getInfo()['coordinates'][0]

    if satellite == "landsat-8":
        bands = ['SR_B4', 'SR_B3', 'SR_B2']
        image = image.select(bands).multiply(0.0000275).add(-0.2)
        vmin, vmax = 0, 0.3
    elif satellite == "sentinel-2":
        bands = ['B4', 'B3', 'B2']
        vmin, vmax = 0, 6000
    elif satellite == "aqua":
        bands = ['sur_refl_b01','sur_refl_b04','sur_refl_b03']
        vmin, vmax = 0, 5000
    elif satellite == "fengyun":
        bands = ['Channel0001','Channel0002','Channel0003']
        vmin, vmax = 0, 4000
    else:
        raise ValueError(f"Satellite not supported: {satellite}")

    try:
        thumb_url = image.getThumbURL({
            'dimensions': THUMB_SIZE, # pixels
            'region': region, # geografic region
            'bands': bands,
            'min': vmin,
            'max': vmax,
        })
        r = requests.get(thumb_url)
        if r.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(r.content)
            return True
        else:
            print(f"Error HTTP {r.status_code} downloading {filename}")
    except Exception as e:
        print(f"Error downloading {filename}: {e}")
    return False


def generate_uruguay_tiles(grid_size_deg=GRID_SIZE_DEG):

    print("Generating tiles for Uruguay...")

    bounds = URUGUAY.geometry().bounds().getInfo()['coordinates'][0]
    lon_min = min([c[0] for c in bounds])
    lon_max = max([c[0] for c in bounds])
    lat_min = min([c[1] for c in bounds])
    lat_max = max([c[1] for c in bounds])
    
    tiles = []
    lon = lon_min
    while lon < lon_max:
        lat = lat_min
        while lat < lat_max:
            square = ee.Geometry.Rectangle([lon, lat, lon+grid_size_deg, lat+grid_size_deg])
            tiles.append(square)
            lat += grid_size_deg
        lon += grid_size_deg
    
    return tiles

def download_tile(tile, tile_num, satellite="sentinel-2"):
    centroid = tile.centroid()

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=7)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    if satellite == "sentinel-2":
        collection = ee.ImageCollection("COPERNICUS/S2_SR") \
            .filterBounds(tile) \
            .filterDate(start_str, end_str) \
            .sort("CLOUDY_PIXEL_PERCENTAGE") \
            .first()
    elif satellite == "landsat-8":
        collection = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2") \
            .filterBounds(tile) \
            .filterDate(start_str, end_str) \
            .first()
    else:
        raise ValueError(f"Unsupported satellite: {satellite}")

    try:
        image = ee.Image(collection)
    except Exception:
        print(f"[{tile_num}] No images found in last 7 days")
        return False

    filename = f"data/uruguay_tiles/tile_{tile_num:05d}.png"

    return download_thumbnail(
        image=image,
        filename=filename,
        point=centroid,
        satellite=satellite
    )

if __name__ == "__main__":

    tiles = generate_uruguay_tiles()

    download_thumbnail()
    