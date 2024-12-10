#!/usr/bin/env python
# coding: utf-8

# <img src="https://raw.githubusercontent.com/EO-College/cubes-and-clouds/main/icons/cnc_3icons_process_circle.svg"
#      alt="Cubes & Clouds logo"
#      style="float: center; margin-right: 10px;" />

# # 2.3 Data Access and Basic Processing
# 
# ## Resample Operators
# 
# Sometimes we need to align the spatial or temporal dimension of two datacubes, so that they can be merged together.

# ### `resample_cube_spatial`: spatial resampling Sentinel-2 to match Landsat

# Start importing the necessary libraries and initialize a local connection for Client-Side Processing.

# In[ ]:


import openeo
from openeo.local import LocalConnection
local_conn = LocalConnection('')


# Create two datacubes, one for Sentinel-2 and one for Landsat

# In[ ]:


url = "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a"

spatial_extent = {"west": 11.4, "east": 11.42, "south": 45.5, "north": 45.52}
temporal_extent = ["2023-06-01", "2023-06-30"]
bands = ["red","nir"]

s2_cube = local_conn.load_stac(url=url,
   spatial_extent=spatial_extent,
   temporal_extent=temporal_extent,
   bands=bands
)
s2_cube.execute()


# In[ ]:


url = "https://planetarycomputer.microsoft.com/api/stac/v1/collections/landsat-c2-l2"
bands = ["red","nir08"]
l8_cube = local_conn.load_stac(url=url,
                    spatial_extent=spatial_extent,
                    temporal_extent=temporal_extent,
                    bands=bands)
l8_cube.execute()


# From the previous outputs, notice the shape difference in the spatial dimensions `x` and `y`.
# 
# This is due to the different resolution of the two collections: 10m for Sentinel-2, 30m for Landsat.
# 
# Now we use the `resample_cube_spatial` process to resample the Sentinel-2 data to match Landsat.

# In[ ]:


s2_cube_30m = s2_cube.resample_cube_spatial(target=l8_cube,method="average")
s2_cube_30m


# Check what happens to the datacube inspecting the resulting xArray object. Now the `x` and `y` shape is the same as Landsat:

# In[ ]:


s2_cube_30m.execute()

