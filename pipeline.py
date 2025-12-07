from download_aqua import export_modis_aqua_rgb
from fwi import fwi
from lst import download_modis_lst
from ndvi import ndvi
from firms_alerts import firms_alerts


if __name__ == "__main__":
    print("Starting data exports to GCS bucket...")
    
    fwi_path = fwi()
    print("FWI exported to:", fwi_path)
    
    ndvi_path = ndvi()
    print("NDVI exported to:", ndvi_path)
    
    lst_path = download_modis_lst()
    print("LST exported to:", lst_path)
    
    aqua_rgb_path = export_modis_aqua_rgb()
    print("MODIS AQUA RGB exported to:", aqua_rgb_path)
    
    alerts_path = firms_alerts()

    print("All exports completed.")