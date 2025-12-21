import os
import ee
import csv
import numpy as np
import requests
import pickle
from tqdm import tqdm
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


ee.Authenticate()
ee.Initialize(project="cellular-retina-276416")

load_dotenv(".env")

GRID_SIZE_KM = 4
GRID_SIZE_DEG = GRID_SIZE_KM / 111  # Aproximación

DATA_DIR = f"data/uruguay_tiles_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
CSV_PATH = os.path.join(DATA_DIR, "metadata.csv")

TILES_PATH = os.path.join("data", "tiles.pkl")

MAX_THREADS = int(os.getenv("MAX_THREADS", "10"))

os.makedirs(DATA_DIR, exist_ok=True)


URUGUAY = (
    ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017")
    .filter(ee.Filter.eq("country_na", "Uruguay"))
)


def init_csv():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "image_name",
                "lon_min", "lat_min", "lon_max", "lat_max",
                "lon_center", "lat_center",
                "timestamp_utc"
            ])


def save_tiles(tiles, path=TILES_PATH):
    with open(path, "wb") as f:
        pickle.dump(tiles, f)

def load_tiles(path=TILES_PATH):
    if os.path.exists(path):
        with open(path, "rb") as f:
            tiles = pickle.load(f)
        return tiles
    return None



def generate_uruguay_tiles(grid_size_deg=GRID_SIZE_DEG):

    uruguay_geom = URUGUAY.geometry()
    bounds = uruguay_geom.bounds().getInfo()["coordinates"][0]

    lon_min = min(c[0] for c in bounds)
    lon_max = max(c[0] for c in bounds)
    lat_min = min(c[1] for c in bounds)
    lat_max = max(c[1] for c in bounds)

    lons = np.arange(lon_min, lon_max, grid_size_deg)
    lats = np.arange(lat_min, lat_max, grid_size_deg)

    tiles = []
    total = len(lons) * len(lats)

    with tqdm(total=total, desc="Generating Uruguay tiles") as pbar:
        for lon in lons:
            for lat in lats:
                square = ee.Geometry.Rectangle(
                    [lon, lat, lon + grid_size_deg, lat + grid_size_deg],
                    proj="EPSG:4326",
                    geodesic=False
                )

                if square.intersects(uruguay_geom, ee.ErrorMargin(1)).getInfo():
                    tiles.append(square)

                pbar.update(1)

    return tiles

def create_tile(args):
    lon, lat, grid_size_deg, uruguay_geom = args
    square = ee.Geometry.Rectangle(
        [lon, lat, lon + grid_size_deg, lat + grid_size_deg],
        proj="EPSG:4326",
        geodesic=False
    )
    if square.intersects(uruguay_geom, ee.ErrorMargin(1)).getInfo():
        return square
    return None

def generate_uruguay_tiles_parallel(grid_size_deg=GRID_SIZE_DEG, max_workers=8):
    uruguay_geom = URUGUAY.geometry()
    bounds = uruguay_geom.bounds().getInfo()["coordinates"][0]

    lon_min = min(c[0] for c in bounds)
    lon_max = max(c[0] for c in bounds)
    lat_min = min(c[1] for c in bounds)
    lat_max = max(c[1] for c in bounds)

    lons = np.arange(lon_min, lon_max, grid_size_deg)
    lats = np.arange(lat_min, lat_max, grid_size_deg)

    tiles = []
    total = len(lons) * len(lats)

    args_list = [(lon, lat, grid_size_deg, uruguay_geom) for lon in lons for lat in lats]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(create_tile, args) for args in args_list]
        for f in tqdm(as_completed(futures), total=total, desc="Generating Uruguay tiles"):
            result = f.result()
            if result:
                tiles.append(result)

    return tiles


def download_latest_sentinel2_rgb(square, tile_num, start_date, end_date):

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(square)
        .filterDate(start_date, end_date)
        .sort("system:time_start", False)
    )

    if collection.size().eq(0).getInfo():
        print(f"[Tile {tile_num}] Sin imágenes")
        return

    image = ee.Image(collection.first())

    timestamp = (
        ee.Date(image.get("system:time_start"))
        .format("YYYY-MM-dd HH:mm:ss")
        .getInfo()
    )

    image = image.select(["B4", "B3", "B2"])

    url = image.getThumbURL({
        "region": square,
        "dimensions": 512,
        "format": "png",
        "min": 0,
        "max": 3000
    })

    response = requests.get(url)

    if response.status_code != 200:
        print(f"[Tile {tile_num}] Error descargando")
        return

    file_name = f"tile_{tile_num}.png"
    file_path = os.path.join(DATA_DIR, file_name)

    with open(file_path, "wb") as f:
        f.write(response.content)

    coords = square.bounds().getInfo()["coordinates"][0]
    lon_min = min(c[0] for c in coords)
    lon_max = max(c[0] for c in coords)
    lat_min = min(c[1] for c in coords)
    lat_max = max(c[1] for c in coords)

    lon_center = (lon_min + lon_max) / 2
    lat_center = (lat_min + lat_max) / 2

    with open(CSV_PATH, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            file_name,
            lon_min, lat_min, lon_max, lat_max,
            lon_center, lat_center,
            timestamp
        ])


if __name__ == "__main__":

    init_csv()

    tiles = load_tiles()
    if tiles is None:
        tiles = generate_uruguay_tiles_parallel()
        save_tiles(tiles)
    else:
        print("Tiles cargados desde disco.")

    print(f"Total de tiles: {len(tiles)}")

    end_date = ee.Date(datetime.utcnow())
    start_date = end_date.advance(-20, "day")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

        futures = {executor.submit(download_latest_sentinel2_rgb, square, i, start_date, end_date): i
                   for i, square in enumerate(tiles)}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Descargando tiles"):
            try:
                future.result()
            except Exception as e:
                i = futures[future]
                print(f"Error descargando tile {i}: {e}")

