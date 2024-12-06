#!/usr/bin/env python
# coding: utf-8

# <img src="https://raw.githubusercontent.com/EO-College/cubes-and-clouds/main/icons/cnc_3icons_process_circle.svg"
#      alt="Cubes & Clouds logo"
#      style="float: center; margin-right: 10px;" />

# # 2.3 Data Access and Basic Processing
# 
# ## Apply Operator
# 
# The apply operator employ a process on the datacube that calculates new pixel values for each pixel, based on n other pixels.
# 
# Let's start again with the same sample data from the Sentinel-2 STAC Collection, applying the filters directly in the `load_stac` call, to reduce the amount of data.

# In[ ]:


import openeo
from openeo.local import LocalConnection
local_conn = LocalConnection('')

url = "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a"
spatial_extent = {"west": 11.259613, "east": 11.406212, "south": 46.461019, "north": 46.522237}
temporal_extent = ['2022-07-10T00:00:00Z','2022-07-13T00:00:00Z']
bands = ["red","green","blue"]
properties = {"eo:cloud_cover": dict(lt=50)}
datacube = local_conn.load_stac(url=url,
                    spatial_extent=spatial_extent,
                    temporal_extent = temporal_extent,
                    bands=bands,
                    properties=properties
)

datacube.execute()


# Visualize the RGB bands of our sample dataset:

# In[ ]:


data = datacube.execute()
data[0].plot.imshow()


# ### Apply an arithmetic formula

# We would like to improve the previous visualization, rescaling all the pixels between 0 and 1.
# 
# We can use `apply` in combination with other `math` processes.

# In[ ]:


from openeo.processes import linear_scale_range
input_min = -0.1
input_max = 0.2
output_min = 0
output_max = 1

def rescale(x):
    return linear_scale_range(x,input_min,input_max,output_min,output_max)

scaled_data = datacube.apply(rescale)
scaled_data


# Visualize the result and see how `apply` scaled the data resulting in a more meaningful visualization:

# In[ ]:


scaled_data_xr = scaled_data.execute()
scaled_data_xr[0].plot.imshow()


# In[ ]:




