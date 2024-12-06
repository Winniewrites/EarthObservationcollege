#!/usr/bin/env python
# coding: utf-8

# <img src="https://raw.githubusercontent.com/EO-College/cubes-and-clouds/main/icons/cnc_3icons_process_circle.svg"
#      alt="Cubes & Clouds logo"
#      style="float: center; margin-right: 10px;" />

# # 2.3 Data Access and Basic Processing
# 
# ## Filter Operators
# 
# When interacting with large data collections, it is necessary to keep in mind that it's not possible to load everything!
# 
# Therefore, we always have to define our requirements in advance and apply them to the data using filter operators.
# 
# Let's start again with the same sample data from the Sentinel-2 STAC Collection with an additional filter.

# ### Properties Filter
# 
# When working with optical data like Sentinel-2, most of the times we would like to discard cloudy acquisitions as soon as possible.
# 
# We can do it using a property filter: in this case we want to keep only the acquisitions with less than 50% cloud coverage.

# In[3]:


properties = {"eo:cloud_cover": dict(lt=50)}


# In[4]:


import openeo
from openeo.local import LocalConnection
local_conn = LocalConnection('')

url = "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a"
spatial_extent = {"west": 11.1, "east": 11.5, "south": 46.1, "north": 46.5}

datacube = local_conn.load_stac(url=url,
                    spatial_extent=spatial_extent,
                    properties=properties)
datacube.execute()


# ### Temporal filter

# To slice along time the data collection with openEO, we can use the `filter_temporal` process.

# In[5]:


temporal_extent = ["2022-05-10T00:00:00Z","2022-06-30T00:00:00Z"]
temporal_slice = datacube.filter_temporal(temporal_extent)
temporal_slice.execute()


# After running the previous cell, it is visible that the result has less elements (or labels) in the temporal dimension `time`.
# 
# Additionally, the size of the selected data reduced a lot.

# **Quiz hint: look carefully at the dimensions of the resulting datacube!**

# ### Spatial filter

# To slice along the spatial dimensions the data collection with openEO, we can use `filter_bbox` or `filter_spatial` processes.

# The `filter_bbox` process is used with a set of coordinates:

# In[6]:


spatial_extent = {"west": 11.259613, "east": 11.406212, "south": 46.461019, "north": 46.522237}
spatial_slice = datacube.filter_bbox(spatial_extent)
spatial_slice.execute()


# **Quiz hint: look carefully at the dimensions of the loaded datacube!**

# ### Bands filter

# To slice along the bands dimension, keeping only the necessary bands, we can use the `filter_bands` process.

# In[7]:


bands = ["red","green","blue"]
bands_slice = datacube.filter_bands(bands)
bands_slice.execute()

