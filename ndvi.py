import ee
import datetime

ee.Authenticate()
ee.Initialize(project="cellular-retina-276416")

def ndvi():
    gaul = ee.FeatureCollection("FAO/GAUL/2015/level0")
    uruguay = gaul.filter(ee.Filter.eq("ADM0_NAME", "Uruguay")).geometry()

    end = datetime.date.today()
    start = end - datetime.timedelta(days=7)

    col = (
        ee.ImageCollection("MODIS/061/MOD09GA")
        .filterBounds(uruguay)
        .filterDate(str(start), str(end))
        .sort("system:time_start", False)
    )

    img = ee.Image(col.first())

    ndvi = img.normalizedDifference(["sur_refl_b02", "sur_refl_b01"]).rename("NDVI")
    ndvi = ndvi.clip(uruguay)

    today_str = datetime.datetime.now().strftime('%Y%m%d')

    file_name = f'ndvi/NDVI_Uruguay_{today_str}'

    task = ee.batch.Export.image.toCloudStorage(
        image=ndvi,
        description='NDVI_Uruguay_Export',
        bucket='wildfires_data_um',            
        fileNamePrefix=file_name,
        region=uruguay.bounds(),
        scale=500,
        crs='EPSG:4326',
        fileFormat='GeoTIFF',
        maxPixels=1e13
    )

    task.start()
    print("Export started to GCS bucket 'wildfires_data_um'")

if __name__ == "__main__":
    ndvi()
