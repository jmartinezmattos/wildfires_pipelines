import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
import subprocess
from utils import move_data_from_local_to_gcs
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv(".env")
EDL_TOKEN = os.getenv("FIRMS_TOKEN")
BUCKET_NAME = os.getenv("BUCKET_NAME")

sensor_basenames = {
    "MODIS": ["modis-c6.1", "MODIS_C6_1_South_America_MCD14DL_NRT_"],
    "NOAA20": ["noaa-20-viirs-c2", "J1_VIIRS_C2_South_America_VJ114IMGTDL_NRT_"],
    "NOAA21": ["noaa-21-viirs-c2","J2_VIIRS_C2_South_America_VJ214IMGTDL_NRT_"],
    "SUOMI": ["suomi-npp-viirs-c2", "SUOMI_VIIRS_C2_South_America_VNP14IMGTDL_NRT_"],
}

def download_file_with_token(url, token, output_path):
    headers = {
        "Authorization": f"Bearer {token}"
    }
    try:
        with requests.get(url, headers=headers, stream=True) as r:
            if r.status_code == 404:
                print(f"File not found at URL: {url}")
                return False
            r.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"File downloaded: {output_path}")
        return output_path
    except Exception as e:
        print("Error in download:", e)
        return False

def get_url_and_filename(date, sensor):
    year = date.year
    julian_day = date.timetuple().tm_yday
    julian_date = f"{year}{julian_day:03d}"
    sensor_basename = sensor_basenames[sensor]
    url = f"https://nrt3.modaps.eosdis.nasa.gov/archive/FIRMS/{sensor_basename[0]}/South_America/{sensor_basename[1]}{julian_date}.txt"
    output_file = f"{sensor_basename[1]}{julian_date}.txt"
    return url, output_file

def filter_uruguay_coordinates(input_file, output_file=None):
    df = pd.read_csv(input_file)
    
    lat_min, lat_max = -35.0, -30.0
    lon_min, lon_max = -58.5, -53.0

    df_uy = df[
        (df['latitude'] >= lat_min) & (df['latitude'] <= lat_max) &
        (df['longitude'] >= lon_min) & (df['longitude'] <= lon_max)
    ]
    
    if output_file:
        df_uy.to_csv(output_file, index=False)
        print(f"File saved: {output_file}")
    
    return df_uy

def create_kml_from_csv(df, output_file):
    kml_header = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
    <Document>
    <name>FIRMS Points</name>
    """
    kml_footer = "</Document>\n</kml>"

    placemarks = ""
    for _, row in df.iterrows():
        placemarks += f"""    <Placemark>
        <name>{row['acq_date']} {row['acq_time']}</name>
        <description>Brightness: {row['brightness']}, Confidence: {row['confidence']}</description>
        <Point>
            <coordinates>{row['longitude']},{row['latitude']},0</coordinates>
        </Point>
    </Placemark>
    """

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(kml_header + placemarks + kml_footer)

    print(f"KML generated correctly: {output_file}")

def download_and_process(date, sensor):
    """Función que descarga y filtra un archivo para una fecha dada."""
    url, filename = get_url_and_filename(date, sensor)
    local_txt = os.path.join("data/firms_alerts_nrt", filename)

    downloaded_file = download_file_with_token(url, EDL_TOKEN, local_txt)
    if not downloaded_file:
        return None

    uru_csv = downloaded_file.replace(".txt", "_Uruguay.csv")
    filter_uruguay_coordinates(downloaded_file, uru_csv)

    return downloaded_file, uru_csv

def firms_alerts_by_dates(dates, sensor="NOAA21", copy_to_gcs=False, delete_local=False, output_dir="data/firms_alerts_nrt"):
    os.makedirs(output_dir, exist_ok=True)

    normalized_dates = []
    for d in dates:
        if isinstance(d, str):
            if d == "today":
                normalized_dates.append(datetime.utcnow())
            elif d == "yesterday":
                normalized_dates.append(datetime.utcnow() - timedelta(days=1))
            else:
                normalized_dates.append(datetime.strptime(d, "%Y-%m-%d"))
        else:
            normalized_dates.append(d)

    generated_files = []
    uru_files = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_date = {executor.submit(download_and_process, date, sensor): date for date in normalized_dates}
        for future in as_completed(future_to_date):
            result = future.result()
            if result:
                downloaded_file, uru_csv = result
                generated_files.extend([downloaded_file, uru_csv])
                uru_files.append(uru_csv)
    
    if copy_to_gcs and generated_files:
        gcs_dir = f"gs://{BUCKET_NAME}/firms_alerts/"
        for file_path in generated_files:
            move_data_from_local_to_gcs(file_path, gcs_dir)
            if delete_local:
                os.remove(file_path)
                print(f"Deleted local file: {file_path}")

    return uru_files


def test():

    dates = [
    "2025-01-10",
    "2025-01-11",
    "2025-01-15"
    ]

    firms_alerts_by_dates(
        dates,
        sensor="NOAA21",
        copy_to_gcs=False,
        delete_local=False
    )

    dates = ["today", "yesterday"]

    firms_alerts_by_dates(
        dates,
        sensor="NOAA21",
        copy_to_gcs=False,
        delete_local=False
    )

if __name__ == "__main__":
    test()
