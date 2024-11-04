import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import urllib.request
import gzip
from io import BytesIO
import geopandas as gpd


BASE_URL = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
FIELD_DICT = {
    'details': ['EVENT_TYPE', 'DAMAGE_PROPERTY', "EVENT_ID"],
    'locations': ["EVENT_ID", 'LATITUDE', 'LONGITUDE']
}

def get_files() -> list:
    """Parse HTML to get download paths"""
    resp = requests.get(BASE_URL)
    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table')
    hrefs = []
    for row in table.find_all('tr'):  # Skip header row
        el = row.find_all('td')
        if len(el) > 1:
            if el[0].text.startswith('StormEvents'):
                hrefs.append(el[0].text)
    return hrefs

def download_file(f: str) -> str:
    """Download and decompress CSV data"""
    with urllib.request.urlopen(BASE_URL + f) as response:
        compressed_data = response.read()
    with gzip.GzipFile(fileobj=BytesIO(compressed_data)) as gz_file:
        decompressed_data = gz_file.read()
    return decompressed_data

def expand_value(value: str) -> str:
    """Convert abbreviations to numbers"""
    value = str(value)
    if value == 'nan' or value == '0' or value == '0.00' or len(value) == 1:
        return 0
    if value.endswith('M'):
        return float(value[:-1]) * 1_000_000
    elif value.endswith('K'):
        return float(value[:-1]) * 1_000
    elif value.endswith('B'):
        return float(value[:-1]) * 1_000_000_000
    else:
        raise RuntimeError(value)  # No suffix, assume it's already numeric


def download_data() -> None:
    """Run all functions to download datasets, subset columns, merge data, and save to a file"""
    hrefs = get_files()
    all_df = {t: [] for t in FIELD_DICT}
    for ind, h in enumerate(hrefs):
        print(f'{ind} / {len(hrefs)}')
        h_type = h.split('_')[1].split('-')[0]
        if h_type == 'fatalities':
            continue
        tmp_dat = download_file(h, 'dst')
        df = pd.read_csv(BytesIO(tmp_dat))
        all_df[h_type].append(df[FIELD_DICT[h_type]])
    for a in all_df:
        pd.concat(all_df[a]).to_csv(f'{a}_all.csv')

def merge_intersect() -> None:
    """Merge detail and location datasets, intersect with polygon layer, and aggregate"""
    pts = pd.read_csv('locations_all.csv')
    details = pd.read_csv('details_all.csv')
    states = gpd.read_file('conus_grid.gpkg')[['id', 'geometry']]

    all_data = pts.merge(details, on='EVENT_ID', suffixes=('_p', '_d'))
    all_data['DAMAGE_PROPERTY'] = all_data['DAMAGE_PROPERTY'].apply(expand_value)
    gdf_points = gpd.GeoDataFrame(
        all_data,
        geometry=gpd.points_from_xy(all_data['LONGITUDE'], all_data['LATITUDE']),
        crs="EPSG:4326"  # Set CRS to WGS84
    )
    states = states.to_crs(gdf_points.crs)
    intersected = gpd.sjoin(gdf_points, states, how="inner", predicate="intersects")

    aggregated_df = intersected.groupby(['id', 'EVENT_TYPE'])['DAMAGE_PROPERTY'].sum().reset_index(name='mag')
    most_common_cat = aggregated_df.loc[aggregated_df.groupby('id')['mag'].idxmax()].reset_index(drop=True)

    most_common_cat.to_csv('aggregated.csv')

def main():
    """Get data, aggregate, and save processed dataset"""
    download_data()
    merge_intersect()

if __name__ == '__main__':
    main()
