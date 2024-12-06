#!/usr/bin/env python
# coding: utf-8

# <img src="https://raw.githubusercontent.com/EO-College/cubes-and-clouds/main/icons/cnc_3icons_process_circle.svg"
#      alt="Cubes & Clouds logo"
#      style="float: center; margin-right: 10px;" />

# # 2.3 Data Access and Basic Processing
# 
# The exercise will use the openEO Python Client Side Processing functionality, which allows to experiment using openEO without a connection to an openEO back-end.

# **Quiz hint: remeber this information for the final quiz!**

# ## Lazy data loading
# 
# When accessing data using an API, most of the time the data is **lazily** loaded.
# 
# It means that only the metadata is loaded, so that it is possible to know about the data dimensions and their extents (spatial and temporal), the available bands and other additional information.
# 
# Let's start with a call to the openEO process `load_stac` for lazily loading some Sentinel-2 data from a public STAC Collection. _Please note that not every STAC Collection or Item is currently supported._
# 
# We need to specify an Area Of Interest (AOI) to get only part of the Collection, otherwise our code would try to load the metadata of all Sentinel-2 tiles available in the world!

# In[1]:


from openeo.local import LocalConnection
local_conn = LocalConnection('')

url = "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a"
spatial_extent = {"west": 11.1, "east": 11.5, "south": 46.1, "north": 46.5}

datacube = local_conn.load_stac(url=url,
                    spatial_extent=spatial_extent)
datacube


# Calling the `.execute()` method, the data will be lazily loaded and an `xArray.DataArray` object returned.
# 
# Running the next cell will show the selected data content with the dimension names and their extent:

# In[2]:


datacube.execute()


# From the output of the previous cell you can notice something really interesting: **the size of the selected data is more than 3 TB!**
# 
# But you should have noticed that it was too quick to download this huge amount of data.
# 
# This is what lazy loading allows: getting all the information about the data in a quick manner without having to access and download all the available files.

# **Quiz hint: look carefully at the dimensions of the loaded datacube!**
