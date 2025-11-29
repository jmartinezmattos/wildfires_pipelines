import ee
import requests
import datetime
import json

ee.Authenticate()
ee.Initialize(project="cellular-retina-276416")

# 1. Load exact Uruguay boundary
gaul = ee.FeatureCollection("FAO/GAUL/2015/level0")
uruguay = gaul.filter(ee.Filter.eq("ADM0_NAME", "Uruguay")).geometry()

# Convert region to client-side GeoJSON
uruguay_geojson = uruguay.getInfo()   # <--- fix!!

# 2. Date range
end = datetime.date.today()
start = end - datetime.timedelta(days=7)

# 3. MODIS data
col = (
    ee.ImageCollection("MODIS/061/MOD09GA")
    .filterBounds(uruguay)
    .filterDate(str(start), str(end))
    .sort("system:time_start", False)
)

img = ee.Image(col.first())

# 4. NDVI
ndvi = img.normalizedDifference(["sur_refl_b02", "sur_refl_b01"]).rename("NDVI")
ndvi = ndvi.clip(uruguay)

# 5. Download
url = ndvi.getDownloadURL({
    "scale": 500,
    "crs": "EPSG:4326",
    "region": json.dumps(uruguay_geojson),   # <--- also fix
    "format": "GEO_TIFF"
})

output = "uruguay_modis_ndvi_exact.tif"
r = requests.get(url)

with open(output, "wb") as f:
    f.write(r.content)

print("Saved:", output)
