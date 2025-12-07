import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd

load_dotenv(".env")
EDL_TOKEN = os.getenv("FIRMS_TOKEN")

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
                return False
            r.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"File downloaded: {output_path}")
        return True
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

def firms_alerts():
    today = datetime.utcnow()
    yesterday = today - timedelta(days=1)
    today_url, today_output_file = get_url_and_filename(today, "MODIS")
    yesterday_url, yesterday_output_file = get_url_and_filename(yesterday, "MODIS")

    download_file_with_token(today_url, EDL_TOKEN, today_output_file)
    download_file_with_token(yesterday_url, EDL_TOKEN, yesterday_output_file)

    uru_df_yesterday = filter_uruguay_coordinates(yesterday_output_file, os.path.basename(yesterday_output_file).replace(".txt", "_Uruguay.csv"))
    uru_df_today = filter_uruguay_coordinates(today_output_file, os.path.basename(today_output_file).replace(".txt", "_Uruguay.csv"))

    create_kml_from_csv(uru_df_yesterday, os.path.basename(yesterday_output_file).replace(".txt", "_Uruguay.kml"))
    create_kml_from_csv(uru_df_today, os.path.basename(today_output_file).replace(".txt", "_Uruguay.kml"))

if __name__ == "__main__":
    firms_alerts()