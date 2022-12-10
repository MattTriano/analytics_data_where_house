{% docs parcels_cc_locations %}

This data set includes property locations and attached spatial data for all Cook County parcels. Spatial matching was based on parcel centroids. Older properties may be missing latitude and longitude data because they are not contained in the most recent parcel shape layer. Brand new properties may be missing a mailing/property address because the postal service has not yet assigned the property an address. Data attached to each PIN is the most recent available. For example, Census tract data is drawn from the 2014-2018 5-year American Community Survey.

{% enddocs %}

{% docs parcels_cc_value_assessments %}

Historic land, building, and total assessed values for all Cook County parcels, from 1999 to present. The Assessor's Office uses these values for reporting, evaluating assessment performance over time, and research.

When working with Parcel Index Numbers (PINs) make sure to zero-pad them to 14 digits. Some datasets may lose leading zeros for PINs when downloaded.

This data is parcel-level. Each row contains the assessed values for a single PIN for a single year. Important notes:
Assessed values are available in three stages: 1) mailed, these are the initial values estimated by the Assessor's Office and mailed to taxpayers. 2) certified, these are values after the Assessor's Office closes appeals. 3) Board of Review certified, these are values after the Board of Review closes appeals.
The values in this data are assessed values, NOT market values. Assessed values must be adjusted by their level of assessment to arrive at market value. Note that levels of assessment have changed throughout the time period covered by this data set.
This data set will be updated roughly contemporaneously (monthly) with the Assessor's website as values are mailed and certified. However, note that there may be small discrepancies between the Assessor's site and this data set, as each pulls from a slightly different system. If you find a discrepancy, please email the Data Department using the contact link below.

{% enddocs %}

{% docs parcels_cc_nbhd_boundaries %}

Neighborhood polygons used by the Cook County Assessor's Office for valuation and reporting. These neighborhoods are specific to the Assessor. They are intended to represent homogenous housing submarkets, NOT Chicago community areas or municipalities.
These neighborhoods were reconstructed from individual parcels using spatial buffering and simplification. The full transformation script can be found on the Assessor's GitLab.

{% enddocs %}

<!-- ***************************** -->
<!-- Cook County Parcel_Sales Data -->
<!-- ***************************** -->
{% docs parcels_cc_sales %}
Parcel sales for real property in Cook County, from 1999 to present. The Assessor's Office uses this data in its modeling to estimate the fair market value of unsold properties.

When working with Parcel Index Numbers (PINs) make sure to zero-pad them to 14 digits.
Some datasets may lose leading zeros for PINs when downloaded.

Sale document numbers correspond to those of the Cook County Clerk, and can be used on the Clerk's website to find more information about each sale.

NOTE: These sales are unfiltered and include non-arms-length transactions. While the Data Department will upload what it has access to monthly, sales are reported on a lag, with many records not populating until months after their official recording date.

Current property class codes, their levels of assessment, and descriptions can be found on the Assessor's website. Note that class codes details can change across time.
{% enddocs %}


{% docs parcels_cc_sales__pin %}
    Property Index Number (uniquely defines a parcel of property).
{% enddocs %}
{% docs parcels_cc_sales__class %}
    Property class of the parcel. [This document](https://web.archive.org/web/20221115030733/https://prodassets.cookcountyassessor.com/s3fs-public/form_documents/classcode.pdf) provides definitions for all property class codes.
    Useful property class trends for this data set:
    
    * X-00 indicates the parcel is land,
    * X-99 indicates the parcel is a condominium,
    * 2-XX indicates the parcel is residential,
    * 3-XX indicates the parcel is a multifamily building, etc
{% enddocs %}
{% docs parcels_cc_sales__sale_date %}
    Date the sale was recorded (not executed).
{% enddocs %}
{% docs parcels_cc_sales__is_mydec_date %}
    Indicates whether the sale date has been overwritten with a more precise value from IDOR (Illinois Department of Revenue).    
    Prior Assessor ingest processes truncated sale dates to the first of the month. Not all sales can be updated with dates from IDOR.
{% enddocs %}
{% docs parcels_cc_sales__sale_document_num %}
    Sale document number. Corresponds to Clerk's document number. Testing
{% enddocs %}
{% docs parcels_cc_sales__is_multisale %}
    Indicates whether a parcel was sold individually or as part of a larger group of PINs
{% enddocs %}