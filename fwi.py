import ee
import datetime
from gee_fwi.FWI import FWICalculator
from gee_fwi.FWIInputs import FWI_GFS_GSMAP
import requests

ee.Authenticate()
ee.Initialize(project="cellular-retina-276416")

def fwi():

    obs = datetime.date.today() - datetime.timedelta(days=1)
    timezone = 'America/Montevideo'

    bounds = ee.Geometry.BBox(-60, -35, -50, -30)

    inputs = FWI_GFS_GSMAP(obs, timezone, bounds)
    calculator = FWICalculator(obs, inputs)
    calculator.set_previous_codes()
    fwi = calculator.compute()

    uruguay = ee.FeatureCollection('FAO/GAUL/2015/level0') \
        .filter(ee.Filter.eq('ADM0_NAME', 'Uruguay'))

    fwi_uruguay = fwi.clip(uruguay)

    # Obtener URL de descarga del GeoTIFF
    # url = fwi_uruguay.getDownloadURL({
    #     'name': 'FWI_Uruguay_' + obs.strftime('%Y%m%d'),
    #     'scale': 1000,           # resolución en metros
    #     'region': uruguay.geometry().bounds().getInfo(),
    #     'crs': 'EPSG:4326',
    #     'fileFormat': 'GeoTIFF'
    # })


    task = ee.batch.Export.image.toCloudStorage(
        image=fwi_uruguay,
        description='FWI_Uruguay_Export',
        bucket='wildfires_data_um',
        fileNamePrefix='fwi/FWI_Uruguay_' + obs.strftime('%Y%m%d'),
        region=uruguay.geometry().bounds().getInfo()['coordinates'],
        scale=1000,
        crs='EPSG:4326',
        fileFormat='GeoTIFF',
        maxPixels=1e13
    )

    task.start()

if __name__ == "__main__":
    fwi()