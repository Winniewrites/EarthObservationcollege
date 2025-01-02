#!/usr/bin/env python
# coding: utf-8

# <img src="https://raw.githubusercontent.com/EO-College/cubes-and-clouds/main/icons/cnc_3icons_share_cirlce.svg"
#      alt="Cubes & Clouds logo"
#      style="float: center; margin-right: 10px;" />

# # 3.3 Data Sharing
# Science is much more impactful once it's shared. Therefore, we are going to learn how to 
# open up our scientific output from a cloud platform, so that is openly available - and 
# has the chance to make the impact it should.
# - Reuse the workflow we have used before for creating the snow covered area
# - Select AOI,
# - Recreate process graph, 
# - Download results for one time-step
#   - A Snow Cover Area map in the COG format
#   - A STAC metadata item that is provided with the result from openEO at CDSE
# - Adapt the STAC item
# - Upload the results and make them available openly via a STAC browser and web map
# 

# ## Libraries

# Start by creating the folders and data files needed to complete the exercise

# In[1]:


get_ipython().system('cp -r $DATA_PATH/33_results/ $HOME/')


# In[2]:


get_ipython().system('cp $DATA_PATH/_33_cubes_utilities.py $HOME/')


# In[3]:


import json
import os
import subprocess
from datetime import datetime

import openeo
import numpy as np
import leafmap
import geopandas as gpd
import shapely
from shapely.geometry import Polygon

import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter

import rioxarray as rio
import xarray
from osgeo import gdal

from _33_cubes_utilities import (
    calculate_sca,
    visualize_bbox,
    create_bounding_box,
    extract_metadata_geometry, 
    extract_metadata_time
)


# ## Login

# Connect to the copernicus dataspace ecosystem.

# In[4]:


conn = openeo.connect('https://openeo.dataspace.copernicus.eu/')


# Authenticate login

# In[5]:


conn.authenticate_oidc()


# Check if the login worked

# In[ ]:


conn.describe_account()


# ## Select an Area of Interest and Time Frame
# 
# Start by selecting a center point of the area you would like to analyse from the map shown below. The starting extent is the full alps. Zoom in to an area and choose a region that has not been mapped yet. *Make sure not to overlap too much with already mapped areas by having a look at the [STAC Collection](https://esa.pages.eox.at/cubes-and-clouds-catalog/browser/#/?.language=en)*. It's a community mapping project :)
# Create a 1 km bounding box around it. This will be the area you are calculating the snow covered area for. 
# 

# **Attention:**
#  Execute the cell below to show the map. Zoom to a location you want to analyze. Use the location symbol to select a point. A marker appears on the map. This is the center of your area of interest

# In[7]:


m = leafmap.Map(center=(47.005, 11.507), zoom=7.5)
m


# **Attention:**
#  Now this cell will get the coordinates of the marker you have placed. This will create a 1 km bounding box around the chosen location. And visualize it in the map above. *The marker moves to the center when you zoom in*

# In[8]:


feat = m.draw_features
geom = feat[0]['geometry']['coordinates']

# set distance of 1 km around bbox
distance_km = 1

# Create a bounding box around the point
bbox = create_bounding_box(geom[0], geom[1], distance_km)
visualize_bbox(m, bbox)


# Now we'll select the time frame. We'll start with the winter months of 2023. 

# In[9]:


temporal_extent = ["2023-02-01", "2023-06-01"]


# ## Reuse the process graph of the snow covered area data cube
# We've saved the python code that we had used to create the snow cover area data cube into a python function `calculate_sca()`. It's stored in `cubes_utilities.py`. It creates a 4 dimensional data cube with the dimensions: x, y, time, bands.
# As parameters we have exposed the bounding box and temporal extent. We will update them with the choices we have made above. 

# In[10]:


snow_map_4dcube = calculate_sca(conn, bbox, temporal_extent)
snow_map_4dcube


# ## Reduce the time dimension
# We want to calculate the SCA for the winter period of a given year. Therefore, we need to reduce the values along the time dimension. We'll use the process `reduce_dimension()` with a `median()` to accomplish this. We are directly continuing to build on our process graph that we have loaded above.

# In[11]:


snow_map_3dcube = snow_map_4dcube.reduce_dimension(reducer="median", dimension="t")
snow_map_3dcube


# ## Download result
# To finish our process graph we add the `save_result()` process choosing the `GTiff` format. It creates a COG out of the box with openEO on CDSE.

# In[12]:


# create a batch job
snowmap_cog = snow_map_3dcube.save_result(format = "GTiff") #, options = {"overviews": "AUTO"})


# We register the job as a batch job on the backend and start the processing. Depending on the traffic on the backend, this usually takes between 1 to 5 minutes.

# In[13]:


job = snowmap_cog.create_job(title="snowmap_cog")
job.start_and_wait()


# Now let's wait until the job is finished and then download the results.

# In[14]:


if job.status() == "finished":
    results = job.get_results()
    results.download_files("33_results/")


# Add statistics to the dataset via gdal, such as a summary of the values within the dataset and also some metadata, i.e. the legend (TIFFTAGS).  And we reduce the datatype to the lowest possible datatype supported by COG uint8, since only have three values to represent (0, 1, 2). If you're interested you can check what happened via `!gdalinfo 33_results/openEO_uint8.tif`

# In[16]:


get_ipython().system('gdal_translate -mo {TIFFTAG_IMAGEDESCRIPTION}=SnowCoveredArea_0=nosnow_1=snow_2-nodatavalue=cloud -ot Byte -of COG -a_nodata 2 -stats "33_results/openEO.tif" "33_results/openEO_uint8.tif"')


# ## Load results
# Now we can open the COG and visualize it. 

# In[18]:


snowmap = rio.open_rasterio("33_results/openEO_uint8.tif", decode_coords="all")
snowmap


# Now, we check if the nodata value can be determined directly from the COG metadata

# In[19]:


snowmap.rio.nodata


# Now, we make a plot of the snowmap keeping in mind that `0 = no snow`, `1 = snow`, and `2 = clouds (nodata value)`

# In[20]:


snowmap.plot(levels=[0, 1, 2])
plt.title("Spatial distribution of snow, no snow and cloudy pixels")
plt.ylabel("Latitude")
plt.xlabel("Longitude")
plt.tight_layout()


# Let's have a look at the histogram to understand the distribution of the values in our map

# In[21]:


data = snowmap.values.flatten()
snowmap.plot.hist(xticks = [0, 1, 2], weights=np.ones(len(data)) / len(data))

plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.title("Distribution of snow, no snow and cloud pixels")
plt.show()


# ## Load STAC metadata
# In addition to the COG we also receive STAC metadata for our result.
# Let's have a look at it.

# In[22]:


stac_collection = results.get_metadata()
stac_collection


# ### Adding Author of the data
# 
# Add your information to become visible as author of the data -  description of each field can be found here: https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md#provider-object
# 
# Please note that leaving the field empty will lead to failed validation of STAC item

# **Attention:** Enter your full name and a short description of the snowmap you generated e.g. `name = "Jane Doe"` and `description = "snow map of Merano"`

# In[25]:


name = ""
description = ""


# In[26]:


author = [{
    "name": name,
    "description": description,
    "roles": ["processor"],
}]

providers = stac_collection["providers"] + author

author_id = [nam[:2] for nam in author[0]["name"].split(" ")]

# generate timestamp
ts = datetime.now().isoformat()
ts = ts.split("T")[0]


# Extract bbox information and temporal extent from the STAC collection that was delivered with the result from OpenEO. We are reusing it to create our STAC item. We have prepared these function for you `extract_metadata_geometry` and `extract_metadata_time`

# In[27]:


geometry = extract_metadata_geometry(stac_collection)[1]


# In[28]:


start_time, end_time = extract_metadata_time(stac_collection)


# Since we calculated the statistics and renamed the file, we have to add this new file name to the STAC item.

# In[29]:


filename = "openEO_uint8.tif"


# Let's create the actual STAC item describing your data! As talked about in previous lessons, STAC item has various required fields which need to be present and filled correctly. For the field ID we assign the fixed name snowcover and the initials of your name. That will be visible on the STAC browser once you have submitted the result!

# In[38]:


stac_item = {
    "type": "Feature", 
    "stac_version": stac_collection["stac_version"],
    "stac_extensions": [],
    "id": "snowcover_" + "".join(author_id).lower()+ "_" + str(ts),
    "geometry": geometry,
    "bbox": bbox,
    "properties": {
       "datetime": None, 
        "start_datetime": start_time,
        "end_datetime": end_time,
        "providers" : providers
                 },
    
    "links": stac_collection["links"],
    "assets": {"visual": {
      "href": filename,
      "type": "image/tiff; application=geotiff; profile=cloud-optimized",
      "title": "Snow coverage",
      "roles": [
        "data"
              ]
            }
        },
}


# In[39]:


stac_item


# Saving the resulting item as stac_item.json into results folder

# In[ ]:


stac_json = json.dumps(stac_item)
with open("33_results/stac_item.json", "w") as file:
    file.write(stac_json)


# Validating that STAC item is important - non valid STAC will not be displayed in the STAC browser after upload

# In[41]:


from stac_validator import stac_validator
import requests
stac = stac_validator.StacValidate()
f = open('33_results/stac_item.json')
data = json.load(f)
stac.validate_dict(data)
print(stac.message)


# ### Now it is time to upload solution to the submission folder and make results visible in STAC browser

# Upload both the STAC json file and the final .tif file to "submissions" folder in your home directory
# 
# You can use the code below to copy the results to the submissions folder

# In[49]:


get_ipython().system('cp ./33_results/stac_item.json ~/submissions/')
get_ipython().system('cp ./33_results/openEO_uint8.tif ~/submissions/')


# And now by executing the cell below, update of the STAC browser will start. By this, you are uploading your results to the openly available STAC browser. This might take some minutes.

# In[50]:


env_var1 = os.getenv('EMAIL')
curl_command = f"curl -X POST -F token=glptt-42d31ac6f592a9e321d0e4877e654dc50dcf4854 -F ref=main -F 'variables[USER_DIRECTORY]=\"{env_var1}\"' https://gitlab.eox.at/api/v4/projects/554/trigger/pipeline" 
process = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()


# ### Your results are online!
# You can now browse your results together with all the other submissions at the publicly available STAC Catalog! You can check your snow cover map, that you are correctly listed as the author and that your contribution has the correct name. The license on the STAC Collection "Cubes and Clouds: Snow Cover" is CC-BY-4.0. The STAC Collection also has it's own DOI.
# 
# Congratulations you have just contributed to a community mapping project that is completely open source, open data and FAIR! Make sure to show it also to your friends, colleagues or potential employers :)
# 
# https://esa.pages.eox.at/cubes-and-clouds-catalog/browser/#/?.language=en

# If you would like to redo your submission, you can still update your files in submissions folder and once ready, run again the code in the cell above. 

# **Attention:** If you have previously opened the STAC browser, your old submission will be cached and not directly displayed. To circumvent this, open a private window from your browser.

# Happy coding!
