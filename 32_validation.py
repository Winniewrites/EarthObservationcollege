#!/usr/bin/env python
# coding: utf-8

# <img src="https://raw.githubusercontent.com/EO-College/cubes-and-clouds/main/icons/cnc_3icons_process_circle.svg"
#      alt="Cubes & Clouds logo"
#      style="float: center; margin-right: 10px;" />

# # 3.2 Validation
# In this exercise, we focus on the validation of the results we have produced. In general, the accuracy of a satellite derived product is expressed by comparing it to in-situ measurements. Furthermore, we will compare the resuling snow cover time series to the runoff of the catchment to check the plausibility of the observed relationship.
# 
# The steps involved in this analysis:
# - Generate Datacube time-series of snowmap,
# - Load _in-situ_ datasets: snow depth station measurements,
# - Pre-process and filter _in-situ_ datasets to match area of interest, 
# - Perform validation of snow-depth measurements,
# - Plausibility check with runoff of the catchment
# 
# More information on the openEO Python Client: https://open-eo.github.io/openeo-python-client/index.html

# Start by creating the folders and data files needed to complete the exercise.

# In[1]:


get_ipython().system('cp -r $DATA_PATH/32_results/ $HOME/')
get_ipython().system('cp -r $DATA_PATH/32_data/ $HOME/')


# In[2]:


get_ipython().system('cp -r $DATA_PATH/_32_cubes_utilities.py $HOME/')


# ## Libraries

# In[3]:


import json
from datetime import date
import numpy as np
import pandas as pd

import xarray as xr
import rioxarray as rio

import matplotlib.pyplot as plt
import rasterio
from rasterio.plot import show

import geopandas as gpd
import leafmap.foliumap as leafmap

import openeo
from _32_cubes_utilities import ( calculate_sca,
                                 station_temporal_filter,
                                 station_spatial_filter,
                                 binarize_snow,
                                 format_date,
                                 assign_site_snow,
                                 validation_metrics)


# ## Login
# Connect to the Copernicus Dataspace Ecosystem.

# In[4]:


conn = openeo.connect('https://openeo.dataspace.copernicus.eu/')


# Login.

# In[5]:


conn.authenticate_oidc()


# Check if the login worked.

# In[ ]:


conn.describe_account()


# ## Region of Interest

# Load the Val Passiria Catchment, our region of interest. And plot it.

# In[7]:


catchment_outline = gpd.read_file('32_data/catchment_outline.geojson')


# In[8]:


center = (float(catchment_outline.centroid.y), float(catchment_outline.centroid.x))
m = leafmap.Map(center=center, zoom=10)
m.add_vector('32_data/catchment_outline.geojson', layer_name="catchment")
m


# ## Generate Datacube of Snowmap

# We have prepared the workflow to generate the snow map as a python function `calculate_sca()`. The `calculate_sca()` is from `_32_cubes_utilities` and is used to reproduce the snow map process graph in openEO

# In[9]:


bbox = catchment_outline.bounds.iloc[0]
temporal_extent = ["2018-02-01", "2018-06-30"]
snow_map_cloud_free = calculate_sca(conn, bbox, temporal_extent)
snow_map_cloud_free


# ## Load snow-station in-situ data
# Load the _in-situ_ datasets, snow depth station measurements. They have been compiled in the ClirSnow project and are available here: [Snow Cover in the European Alps](https://zenodo.org/record/5109574) with stations in our area of interest. 
# 
# We have made the data available for you already. We can load it directly.

# In[10]:


# load snow station datasets from zenodo:: https://zenodo.org/record/5109574
station_df = pd.read_csv("32_data/data_daily_IT_BZ.csv")
station_df = station_df.assign(Date=station_df.apply(format_date, axis=1))
# the format_date function, from _32_cubes_utilities was used to stringify each Datetime object in the dataframe
# station_df.head()


# In[11]:


# load additional metadata for acessing the station geometries
station_df_meta = pd.read_csv("32_data/meta_all.csv")
station_df_meta.head()


# ## Pre-process and filter _in-situ_ snow station measurements
# 
# ### Filter Temporally
# Filter the in-situ datasets to match the snow-map time series using the function `station_temporal_filter()` from `cubes_utilities.py`, which merges the station dataframe with additional metadata needed for the Lat/Long information and convert them to geometries

# In[12]:


start_date = "2018-02-01"
end_date = "2018-06-30"

snow_stations = station_temporal_filter(station_daily_df = station_df, 
                                        station_meta_df = station_df_meta,
                                        start_date = start_date,
                                        end_date = end_date)
snow_stations.head()


# ### Filter Spatially
# Filter the in-situ datasets into the catchment area of interest using `station_spatial_filter()` from `cubes_utilities.py`.

# In[13]:


catchment_stations = station_spatial_filter(snow_stations, catchment_outline)
catchment_stations.head()


# ### Plot the filtered stations
# Visualize location of snow stations

# In[14]:


print("There are", len(np.unique(catchment_stations.Name)), "unique stations within our catchment area of interest")


# **_Quick Hint: Remember the number of stations within the catchment for the final quiz exercise_**

# ### Convert snow depth to snow presence
# The stations are measuring snow depth. We only need the binary information on the presence of snow (yes, no). We use the `binarize_snow()`  function from `cubes_utilities.py` to assign 0 for now snow and 1 for snow in the **snow_presence** column.

# In[15]:


catchment_stations = catchment_stations.assign(snow_presence=catchment_stations.apply(binarize_snow, axis=1))
catchment_stations.head()


# ### Save the pre-processed snow station measurements
# Save snow stations within catchment as GeoJSON

# In[16]:


with open("32_results/catchment_stations.geojson", "w") as file:
    file.write(catchment_stations.to_json())


# ## Extract SCA from the data cube per station

# ### Prepare snow station data for usage in openEO
# Create a buffer of approximately 80 meters (0.00075 degrees) around snow stations and visualize them.

# In[17]:


catchment_stations_gpd = gpd.read_file("32_results/catchment_stations.geojson")
mappy =leafmap.Map(center=center, zoom=16)
mappy.add_vector('32_data/catchment_outline.geojson', layer_name="catchment")
mappy.add_gdf(catchment_stations_gpd, layer_name="catchment_station")

catchment_stations_gpd["geometry"] = catchment_stations_gpd.geometry.buffer(0.00075)
mappy.add_gdf(catchment_stations_gpd, layer_name="catchment_station_buffer")
mappy


# Convert the unique geometries to Feature Collection to be used in a openEO process.

# In[18]:


catchment_stations_fc = json.loads(
    catchment_stations_gpd.geometry.iloc[:5].to_json()
)


# ### Extract SCA from the data cube per station
# We exgtract the SCA value of our data cube at the buffered station locations. Therefore we use the process `aggregate_spatial()` with the aggregation method `median()`. This gives us the most common value in the buffer (snow or snowfree).

# In[19]:


snowmap_per_station= snow_map_cloud_free.aggregate_spatial(catchment_stations_fc, reducer="median")
snowmap_per_station


# Create a batch job on the cloud platform. And start it.

# In[20]:


snowmap_cloudfree_json = snowmap_per_station.save_result(format="JSON")
job = snowmap_cloudfree_json.create_job(title="snow_map")
job.start_and_wait()


# Check the status of the job. And download once it's finished.

# In[21]:


job.status()


# In[22]:


if job.status() == "finished":
    results = job.get_results()
    results.download_files("32_results/snowmap/")


# Open the snow covered area time series extracted at the stations. We'll have a look at it in a second.

# In[23]:


with open("32_results/snowmap/timeseries.json","r") as file:
    snow_time_series = json.load(file)


# ## Combine station measurements and the extracted SCA from our data cube
# The **station measurements** are **daily** and all of the stations are combined in **one csv file**. 
# The **extracted SCA values** are in the best case **six-daily** (Sentinel-2 repeat rate) and also all stations are in **one json file**.
# We will need to join the the extracted SCA with the station measurements by station (and time (selecting the corresponding time steps)

# ### Extract snow values from SCA extracted at the station location
# Let's have a look at the data structure first

# In[24]:


dates = [k.split("T")[0] for k in snow_time_series]
snow_val_smartino = [snow_time_series[k][0][0] for k in snow_time_series]
snow_val_rifiano = [snow_time_series[k][1][0] for k in snow_time_series]
snow_val_plata = [snow_time_series[k][2][0] for k in snow_time_series]
snow_val_sleonardo = [snow_time_series[k][3][0] for k in snow_time_series]
snow_val_scena = [snow_time_series[k][4][0] for k in snow_time_series]


# ### Match in-situ measurements to dates in SCA 
# Let's have a look at the in-situ measurement data set.

# In[25]:


catchment_stations_gpd.sample(10)


# We are going to extract each station and keep only the dates that are available in the SCA results.

# In[26]:


catchment_stations_gpd_smartino = catchment_stations_gpd.query("Name == 'S_Martino_in_Passiria_Osservatore'")
catchment_stations_gpd_smartino = catchment_stations_gpd_smartino[
    catchment_stations_gpd_smartino.id.isin(dates)
]

catchment_stations_gpd_rifiano = catchment_stations_gpd.query("Name == 'Rifiano_Beobachter'")
catchment_stations_gpd_rifiano = catchment_stations_gpd_rifiano[
    catchment_stations_gpd_rifiano.id.isin(dates)
]

catchment_stations_gpd_plata = catchment_stations_gpd.query("Name == 'Plata_Osservatore'")
catchment_stations_gpd_plata = catchment_stations_gpd_plata[
    catchment_stations_gpd_plata.id.isin(dates)
]

catchment_stations_gpd_sleonardo = catchment_stations_gpd.query("Name == 'S_Leonardo_in_Passiria_Osservatore'")
catchment_stations_gpd_sleonardo = catchment_stations_gpd_sleonardo[
    catchment_stations_gpd_sleonardo.id.isin(dates)
]

catchment_stations_gpd_scena = catchment_stations_gpd.query("Name == 'Scena_Osservatore'")
catchment_stations_gpd_scena = catchment_stations_gpd_scena[
    catchment_stations_gpd_scena.id.isin(dates)
]


# ### Combine in-situ measurements with SCA results at the stations 
# The in situ measurements and the SCA are combined into one data set per station. This will be the basis for the validation.

# In[27]:


smartino_snow = assign_site_snow(catchment_stations_gpd_smartino, snow_val_smartino)
rifiano_snow = assign_site_snow(catchment_stations_gpd_rifiano, snow_val_rifiano)
plata_snow = assign_site_snow(catchment_stations_gpd_plata, snow_val_plata)
sleonardo_snow = assign_site_snow(catchment_stations_gpd_sleonardo, snow_val_sleonardo)
scena_snow = assign_site_snow(catchment_stations_gpd_scena, snow_val_scena)                                                                    


# Let's have a look at the SCA extracted at the station San Martino and it's in situ measurements.

# In[28]:


catchment_stations_gpd_plata.sample(5)


# Display snow presence threshold in in-situ data for Plato Osservatore

# In[29]:


catchment_stations_gpd_plata.plot(x="id", y="HS_after_gapfill",rot=45,kind="line",marker='o')
plt.axhline(y = 0.4, color = "r", linestyle = "-")
plt.show()


# ## Validate the SCA results with the snow station measurements 
# Now that we have combined the SCA results with the snow station measurements we can start the actual validation. A **confusion matrix** compares the classes of the station data to the classes of the SCA result. The numbers can be used to calculate the accuracy (correctly classified cases / all cases).
# 
# |             | no_snow | snow    |
# |-------------|---------|---------|
# | **no_snow** | correct | error   |
# | **snow**    | error   | correct |

# In[30]:


import seaborn as sns


# In[31]:


fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(10, 6))

fig.suptitle("Error matrices for snow stations within our selected Catchment")
sns.heatmap(validation_metrics(smartino_snow)[1], annot=True, xticklabels=["No Snow", "Snow"], yticklabels=["No Snow", "Snow"], ax=ax1)
ax1.set_title("San Martino in Passiria Osservatore")
ax1.set(xlabel="Predicted label", ylabel="True label")


sns.heatmap(validation_metrics(rifiano_snow)[1], annot=True, xticklabels=["No Snow", "Snow"], yticklabels=["No Snow", "Snow"], ax=ax2)
ax2.set_title("Rifiano Beobachter")
ax2.set(xlabel="Predicted label", ylabel="True label")


sns.heatmap(validation_metrics(plata_snow)[1], annot=True, xticklabels=["No Snow", "Snow"], yticklabels=["No Snow", "Snow"], ax=ax3)
ax3.set_title("Plata Osservatore")
ax3.set(xlabel="Predicted label", ylabel="True label")


sns.heatmap(validation_metrics(scena_snow)[1], annot=True, xticklabels=["No Snow", "Snow"], yticklabels=["No Snow", "Snow"], ax=ax4)
ax4.set_title("Scena Osservatore")
ax4.set(xlabel="Predicted label", ylabel="True label")

fig.tight_layout()


# The **accuracy** of the snow estimate from the satellite image computation for each station is shown below: 
# 
# 
# | **On-site snow station**             | **Accuracy**|
# |--------------------------------------|-------------|
# | San Martino in Passiria Osservatore  | **100.00%** |
# | Rifiano Beobachter                   | **100.00%** |
# | Plata Osservatore                    |    82.61%   |
# | San Leonardo in Passiria Osservatore |    NaN      |
# | Scena Osservatore                    |    96.15%   |

# The fifth and last station **San Leonardo in Passiria Osservatore** recorded **_NaNs_** for snow depths for our selected dates, which could potentially be as a results of malfunctioning on-site equipments. Hence, we are not able to verify for it. But overall, the validation shows a 100% accuracy for stations **San Martino in Passiria Osservatore** and **Rifiano Beobachter**, while station **Plata Osservatore** has a lot more False Positive (4) than the other stations.This shows a good match between estimated snow values from satellite datasets and on-the ground measurements of the presence of snow. 

# ## Compare to discharge data
# In addition to computing metrics for validating the data, we also check the plausibility of our results. We compare our results with another measure with a known relationship. In this case, we compare the **snow cover area** time series with the **discharge** time-series at the main outlet of the catchment. We suspect that after snow melting starts, with a temporal lag, the runoff will increase. Let's see if this holds true.

# Load the discharge data at Meran, the main outlet of the catchment. We have prepared this data set for you, it's extracted from Eurac's [Environmental Data Platform Alpine Drought Observatory Discharge Hydrological Datasets](https://edp-portal.eurac.edu/discovery/9e195271-02ae-40be-b3a7-525f57f53c80)). 

# In[32]:


discharge_ds = pd.read_csv('32_data/ADO_DSC_ITH1_0025.csv', 
                           sep=',', index_col='Time', parse_dates=True)
discharge_ds.head()


# Load the SCA time series we have generated in a previous exercise. It's the time series of the aggregated snow cover area percentage for the whole catchment.

# In[33]:


snow_perc_df = pd.read_csv("32_data/filtered_snow_perc.csv", 
                          sep=',', index_col='time', parse_dates=True)


# Let's plot the relationship between the snow covered area and the discharge in the catchment.

# In[34]:


start_date = date(2018, 2, 1)
end_date = date(2018, 6, 30)
# filter discharge data to start and end dates
discharge_ds = discharge_ds.loc[start_date:end_date]

ax1 = discharge_ds.discharge_m3_s.plot(label='Discharge', xlabel='', ylabel='Discharge (m$^3$/s)')
ax2 = snow_perc_df["perc_snow"].plot(marker='o', secondary_y=True, label='SCA', xlabel='', ylabel='Snow cover area (%)')
ax1.legend(loc='center left', bbox_to_anchor=(0, 0.6))
ax2.legend(loc='center left', bbox_to_anchor=(0, 0.5))
plt.show()


# The relationship looks as expected! Once the snow cover decreases the runoff increases!
