#!/usr/bin/env python
# coding: utf-8

# <img src="https://raw.githubusercontent.com/EO-College/cubes-and-clouds/main/icons/cnc_3icons_process_circle.svg"
#      alt="Cubes & Clouds logo"
#      style="float: center; margin-right: 10px;" />

# # 2.3 Data Access and Basic Processing
# 
# ## Reduce Operators
# 
# When computing statistics over time or indices based on multiple bands, it is possible to use reduce operators.
# 
# In openEO we can use the [reduce_dimension](https://processes.openeo.org/#reduce_dimension) process, which applies a reducer to a data cube dimension by collapsing all the values along the specified dimension into an output value computed by the reducer.

# Reduce the temporal dimension to a single value, the mean for instance:

# In[ ]:


import openeo
from openeo.processes import clip
from openeo.local import LocalConnection
local_conn = LocalConnection('')

url = "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a"
spatial_extent = {"west": 11.259613, "east": 11.406212, "south": 46.461019, "north": 46.522237}
temporal_extent = ["2021-05-28T00:00:00Z","2021-06-30T00:00:00Z"]
bands = ["red","nir"]
datacube = local_conn.load_stac(url=url,
                                spatial_extent=spatial_extent,
                                temporal_extent=temporal_extent,
                                bands=bands)
datacube = datacube.apply(lambda x: clip(x,0,10000)) # Get rid of possible negative values

datacube_mean_time = datacube.reduce_dimension(dimension="time",reducer="mean")
datacube_mean_time


# Check what happens to the datacube inspecting the resulting xArray object:

# In[ ]:


datacube_mean_time.execute()


# It is possible to reduce in the same way all the available dimensions of the datacube.
# 
# We can, for instance, reduce the band dimension similarly as we did for the temporal dimension:

# In[ ]:


datacube_mean_band = datacube.reduce_dimension(dimension="band",reducer="mean")


# The result will now contain values resulting from the average of the bands:

# In[ ]:


datacube_mean_band.execute()


# **Quiz hint: look carefully at number of pixels of the loaded datacube!**

# The reducer could be again a single process, but when computing spectral indices like NDVI, NDSI etc. an arithmentical formula is used instead.
# 
# For instance, the [NDVI](https://en.wikipedia.org/wiki/Normalized_difference_vegetation_index) formula can be expressed using a `reduce_dimension` process over the `bands` dimension:
# 
# $$ NDVI = {{NIR - RED} \over {NIR + RED}} $$

# In[ ]:


def NDVI(data):
    red = data.array_element(index=0)
    nir = data.array_element(index=1)
    ndvi = (nir - red)/(nir + red)
    return ndvi

ndvi = datacube.reduce_dimension(reducer=NDVI,dimension="band")
ndvi_xr = ndvi.execute()
ndvi_xr


# Visualize a sample NDVI result:

# In[ ]:


get_ipython().run_cell_magic('time', '', 'ndvi_xr[0].plot.imshow(vmin=-1,vmax=1,cmap="Greens")\n')


# Additionally, it is possible to reduce both spatial dimensions of the datacube at the same time.
# 
# To do this, we need the `reduce_spatial` process.
# 
# This time we select a smaller area of interest, to reduce the amount of downloaded data:

# In[ ]:


url = "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a"
spatial_extent = {"west": 11.31369, "east": 11.31906, "south": 46.52167, "north": 46.52425}
temporal_extent = ["2021-01-01T00:00:00Z","2021-12-30T00:00:00Z"]
bands = ["red","nir"]
properties = {"eo:cloud_cover": dict(lt=15)}

datacube = local_conn.load_stac(url=url,
                                spatial_extent=spatial_extent,
                                temporal_extent=temporal_extent,
                                bands=bands,
                                properties=properties)
datacube = datacube.apply(lambda x: clip(x,0,10000)) # Get rid of possible negative values


# In[ ]:


datacube_spatial_median = datacube.reduce_spatial(reducer="median")
datacube_spatial_median


# Verify that the spatial dimensions were collapsed:

# In[ ]:


datacube_spatial_median.execute()


# We can combine this spatial reducer with the previous over bands to compute a time series of NDVI values:

# In[ ]:


ndvi_spatial = datacube_spatial_median.reduce_dimension(reducer=NDVI,dimension="band")


# In[ ]:


get_ipython().run_cell_magic('time', '', 'ndvi_spatial_xr = ndvi_spatial.execute()\nndvi_spatial_xr = ndvi_spatial_xr.compute()\n')


# Remember that calling `.compute()` on an xarray + dask based object will load into memory the data.
# In this case it will trigger the download of the data from the STAC Catalog and the processing defined as openEO process graph, computing the NDVI time series.

# Visualize the NDVI time series:

# In[ ]:


ndvi_spatial_xr.where(ndvi_spatial_xr<1).plot.scatter()

