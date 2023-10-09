# WFM Heatmap Dashboard
Created by:  Andrew West  
Created on:  2023-05-05  
Purpose:  To easily view of the distribution of individual produce.  

A heatmap displaying the distribution of produce throughout England.
The dashboard is [hosted on DASH](https://dap-prd2-connect.azure.defra.cloud/wfm_heatmap/).

The dashboard offers a variable resolution ("100km2", "10km2", "1km2", "ha", "parcel"), they use grid base segementation, except parcel which is a KDE heatmap.
The dashboard has all sorts of metric (e.g. "gm_..."=gross margins, "ha"=hectares, "t"=tonnes, "n"=head, and a few extras).
Where "..." is representing all sorts of produce (e.g. peas, dairy, and many more).
