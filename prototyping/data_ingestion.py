from contextlib import closing

import pandas as pd

# import requests
# import urllib.request
# import zipfile
import io
import requests
import zipfile
# import StringIO


class Data_Ingestion():
    """A class to represent data downloaded from OpenDAtaSUS API"""

    def __init__(self):
        pass

    def download_vaccinated_people_info(self, n=10000):
        """
        Returns data downloaded from a HTTP request to OpenDataSUS API in pandas DataFrame type.

        Also print number of records in downloaded request.

            Parameters:
            ----------
                n : int
                    target number of documents to be downloaded
            
            Returns:
            ----------
                pd.DataFrame
                    DataFrame of OpenDataSUS API data
        """
        r =requests.post(
            'https://imunizacao-es.saude.gov.br/_search',
            auth=requests.auth.HTTPBasicAuth(
                'imunizacao_public',
                'qlto5t&7r_@+#Tlstigi'),
            json={"size":f"{n}"}
        )
        
        # if status code is different than 200, some problem has ocurred and no data has been downloaded.
        if r.status_code != 200:
            raise Exception(f"Error in data request: status_code = {r.status_code}")
        
        # align data structure to get only relevant data in dataframe
        data = [document['_source'] for document in r.json()['hits']['hits']]
        print(f"Gathered {len(data)} records.")
        return pd.DataFrame.from_records(data)
    
    def download_establishment_data_zip(self, url:str, save_path: str, chunk_size=128) -> None:
        # def download_url(url, save_path, chunk_size=128):
        r = requests.get(url, stream=True)
        with open(save_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
    
    def unzip_establishment_data(self, path_to_zip_file: str, directory_to_extract_to: str) -> None:
        with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
            zip_ref.extractall(directory_to_extract_to)