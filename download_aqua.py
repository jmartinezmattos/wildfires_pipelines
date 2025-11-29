import ee
import datetime
import requests

ee.Authenticate()
ee.Initialize(project="cellular-retina-276416")

OUTFILE = "uruguay_modis_aqua_rgb.tif"

today = datetime.date.today()
start = ee.Date(today.isoformat()).advance(-30, "day")
end = ee.Date(today.isoformat()).advance(1, "day")

uruguay = ee.FeatureCollection("FAO/GAUL/2015/level0") \
    .filter(ee.Filter.eq("ADM0_NAME", "Uruguay")) \
    .geometry()

collection = (
    ee.ImageCollection("MODIS/061/MOD09GA")
    .filterDate(start, end)
    .select(["sur_refl_b01", "sur_refl_b04", "sur_refl_b03"])
    .sort("system:time_start", False)
)

size = collection.size().getInfo()
if size == 0:
    raise Exception("No MODIS AQUA images found in the last 30 days!")

image = ee.Image(collection.first()).clip(uruguay)

url = image.getDownloadURL({
    "scale": 500,
    "crs": "EPSG:4326",
    "region": uruguay,
    "format": "GEO_TIFF"
})

data = requests.get(url)
with open(OUTFILE, "wb") as f:
    f.write(data.content)

print(f"Saved: {OUTFILE} (latest available MODIS AQUA image)")
