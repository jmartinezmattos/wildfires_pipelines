import os
from metrics.fwi import fwi
from metrics.ndvi import ndvi
from metrics.lst import download_modis_lst
from utils import move_data_from_gcs_to_local
from metrics.download_aqua import export_modis_aqua_rgb

def pipeline_metrics():

    gcs_paths = []

    print("Starting data exports to GCS bucket...")
    
    fwi_path = fwi()
    gcs_paths.append(fwi_path)
    print("FWI exported to:", fwi_path)
    
    ndvi_path = ndvi()
    gcs_paths.append(ndvi_path)
    print("NDVI exported to:", ndvi_path)
    
    lst_path = download_modis_lst()
    gcs_paths.append(lst_path)
    print("LST exported to:", lst_path)
    
    aqua_rgb_path = export_modis_aqua_rgb()
    gcs_paths.append(aqua_rgb_path)
    print("MODIS AQUA RGB exported to:", aqua_rgb_path)
    
    print("All exports completed.")

    local_dir = "data"
    os.makedirs(local_dir, exist_ok=True)
    move_data_from_gcs_to_local(gcs_paths, local_dir)

if __name__ == "__main__":

    pipeline_metrics()