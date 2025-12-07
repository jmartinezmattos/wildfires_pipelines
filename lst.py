#Land Surface Temperature (LST)
import ee
import datetime
from utils import wait_for_task

ee.Authenticate()
ee.Initialize(project="cellular-retina-276416")

def download_modis_lst():
    gaul = ee.FeatureCollection("FAO/GAUL/2015/level0")
    uruguay = gaul.filter(ee.Filter.eq("ADM0_NAME", "Uruguay")).geometry()

    # --- DATE RANGE: LAST 1 DAY ---
    end = datetime.date.today()
    start = end - datetime.timedelta(days=3)

    # MODIS Terra LST daily
    collection = (
        ee.ImageCollection("MODIS/061/MOD11A1")
        .filterDate(str(start), str(end))
        .filterBounds(uruguay)
        .sort("system:time_start", False)
    )

    image = ee.Image(collection.first())

    if image is None:
        print("No MODIS LST image found for the period.")
        return None

    # --- Extract day LST band and scale ---
    # LST = DN * 0.02  → Kelvin
    lst_day = image.select("LST_Day_1km").multiply(0.02).rename("LST_Day_K")

    lst_clipped = lst_day.clip(uruguay)

    bucket = "wildfires_data_um"
    today = datetime.datetime.now().strftime("%Y%m%d")
    prefix = f"lst/MODIS_LST_Uruguay_{today}"

    task = ee.batch.Export.image.toCloudStorage(
        image=lst_clipped,
        description="MODIS_LST_Uruguay",
        bucket=bucket,
        fileNamePrefix=prefix,
        region=uruguay.bounds(),
        scale=1000,
        crs="EPSG:4326",
        fileFormat="GeoTIFF",
        maxPixels=1e13
    )

    task.start()
    print("Export started… waiting for completion.")

    success = wait_for_task(task)

    if not success:
        return None

    gcs_path = f"gs://{bucket}/{prefix}.tif"
    print("Export completed:", gcs_path)
    return gcs_path

if __name__ == "__main__":
    result = download_modis_lst()
    print("Returned:", result)
