import s3fs
import sys
from itertools import product
import xarray as xr
import intake
import yaml
import netCDF4 

# Code modified from https://github.com/drakkar-workshop/drakkar2025-demo-swot-ocean/blob/main/notebooks/Wavenumber_spectra_CalVal.ipynb

import pyinterp
from widetrax import DataPreprocessing as dp
from widetrax import Spectra as sp

def load_data(sim_name,area,start_date, end_date):
  # Load name stored from metadata file
  with open("./metadata_example.yaml", "r") as f:
    metadata = yaml.safe_load(f)

  base_s3_folder = metadata[sim_name]["model_dataset"]["base_s3_folder"]
  endpoint_url = metadata[sim_name]["model_dataset"]["endpoint_url"]


  # Definition cycles
  file_path = "https://minio.lab.dive.edito.eu/project-meom-ige/cycles_periods.csv"
  matching_cycles = dp.get_matching_cycles(file_path, start_date, end_date)

  # load data using widetrax 
  datasets_dict = dp.read_swot_ncfiles_S3subfolders(
  base_s3_folder,
  matching_cycles,
  endpoint_url,
  area)

  sorted_datasets = dp.sort_datasets_by_time(datasets_dict)
  has_converged, filled_datasets = dp.fill_nan(sorted_datasets)
  segments_dict = sp.retrieve_segments(filled_datasets,FileType = "NetCDF")
  return segments_dict 
      