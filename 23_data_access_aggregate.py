#!/usr/bin/env python
# coding: utf-8

# <img src="https://raw.githubusercontent.com/EO-College/cubes-and-clouds/main/icons/cnc_3icons_process_circle.svg"
#      alt="Cubes & Clouds logo"
#      style="float: center; margin-right: 10px;" />

# # 2.3 Data Access and Basic Processing
# 
# ## Aggregate Operators

# ### `aggregate_temporal_period`: temporal aggregation with predefined intervals

# Start importing the necessary libraries and initialize a local connection for Client-Side Processing.

# In[ ]:


import openeo
from openeo.local import LocalConnection
local_conn = LocalConnection('')


# Create the starting Sentinel-2 datacube:

# In[ ]:


url = "https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a"

spatial_extent = {"west": 11.4, "east": 11.42, "south": 45.5, "north": 45.52}
temporal_extent = ["2022-01-01", "2022-12-31"]
bands = ["red","green","blue"]

s2_cube = local_conn.load_stac(url=url,
   spatial_extent=spatial_extent,
   temporal_extent=temporal_extent,
   bands=bands
)
s2_cube.execute()


# We might be interested in aggregating our data over periods like week, month, year etc., defining what operation to use to combine the data available in the chosen period.
# 
# Using `aggregate_temporal_period` we can achieve this easily:

# In[ ]:


s2_monthly_min = s2_cube.aggregate_temporal_period(period="month",reducer="min")
s2_monthly_min


# Check what happens to the datacube inspecting the resulting xArray object. Now the `time` dimension has 12 labels, one for each month.

# In[ ]:


s2_monthly_min.execute()

