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

<!-- ******************************** -->
<!-- Cook County Parcel Location Data -->
<!-- ******************************** -->
{% docs parcels_cc_locations %}

This data set includes property locations and attached spatial data for all Cook County parcels. Spatial matching was based on parcel centroids. Older properties may be missing latitude and longitude data because they are not contained in the most recent parcel shape layer. Brand new properties may be missing a mailing/property address because the postal service has not yet assigned the property an address. Data attached to each PIN is the most recent available. For example, Census tract data is drawn from the 2014-2018 5-year American Community Survey.

{% enddocs %}

{% docs parcels_cc_locations__pin %}
    Property Index Number (uniquely defines a parcel of property). Zero-pad to 14 digits to look up a given PIN through the [CCAO Property Search Portal](https://www.cookcountyassessor.com/address-search).
{% enddocs %}
{% docs parcels_cc_locations__property_address %}
    Street address of the property
{% enddocs %}
{% docs parcels_cc_locations__property_apt_no %}
    Apartment or unit number of the property, if applicable
{% enddocs %}
{% docs parcels_cc_locations__property_city %}
    City of the property
{% enddocs %}
{% docs parcels_cc_locations__property_zip %}
    9-digit zip code of the property
{% enddocs %}
{% docs parcels_cc_locations__nbhd %}
    3-digit assessor neighborhood, only unique when combined with township number
{% enddocs %}
{% docs parcels_cc_locations__township %}
    Township number
{% enddocs %}
{% docs parcels_cc_locations__township_name %}
    Township name
{% enddocs %}
{% docs parcels_cc_locations__municipality %}
    Municipality name
{% enddocs %}
{% docs parcels_cc_locations__municipality_fips %}
    Municipality FIPS code
{% enddocs %}
{% docs parcels_cc_locations__ward %}
    City of Chicago ward number
{% enddocs %}
{% docs parcels_cc_locations__puma %}
    PUMA (Public Use Microdata Area) code of the PUMA containing the property, 2018 defitition
{% enddocs %}
{% docs parcels_cc_locations__tract_geoid %}
    Full Census FIPS code of the tract containing the property
{% enddocs %}
{% docs parcels_cc_locations__tract_pop %}
    Census population of the tract containing the property, taken directly from 2014-2018 5-year ACS estimates
{% enddocs %}
{% docs parcels_cc_locations__tract_white_perc %}
    White percentage of population of tract containing the property, derived from 2014-2018 5-year ACS estimates using Census race classification
{% enddocs %}
{% docs parcels_cc_locations__tract_black_perc %}
    Black percentage of population of tract containing the property, derived from 2014-2018 5-year ACS estimates using Census race classification
{% enddocs %}
{% docs parcels_cc_locations__tract_asian_perc %}
    Asian percentage of population of tract containing the property, derived from 2014-2018 5-year ACS estimates using Census race classification
{% enddocs %}
{% docs parcels_cc_locations__tract_his_perc %}
    Hispanic percentage of population of tract containing the property, derived from 2014-2018 5-year ACS estimates using Census race classification
{% enddocs %}
{% docs parcels_cc_locations__tract_other_perc %}
    Other percentage of population of tract containing the property, derived from 2014-2018 5-year ACS estimates using Census race classification
{% enddocs %}
{% docs parcels_cc_locations__tract_midincome %}
    Median income of the tract containing the property, taken from 2014-2018 5-year ACS estimates
{% enddocs %}
{% docs parcels_cc_locations__commissioner_dist %}
    Cook County Commissioner district
{% enddocs %}
{% docs parcels_cc_locations__reps_dist %}
    Illinois state representative district
{% enddocs %}
{% docs parcels_cc_locations__senate_dist %}
    Illinois state senate district
{% enddocs %}
{% docs parcels_cc_locations__ssa_name %}
    City of Chicago Special Service Area name
{% enddocs %}
{% docs parcels_cc_locations__ssa_no %}
    City of Chicago Special Service Area number
{% enddocs %}
{% docs parcels_cc_locations__tif_agencynum %}
    Tax Increment Financing (TIF) district
{% enddocs %}
{% docs parcels_cc_locations__school_elem_district %}
    Name of the elementary/middle school district the property falls within, including CPS catchment zones
{% enddocs %}
{% docs parcels_cc_locations__school_hs_district %}
    Name of the high school district the property falls within, including CPS catchment zones
{% enddocs %}
{% docs parcels_cc_locations__mailing_address %}
    Mailing address of the property owner
{% enddocs %}
{% docs parcels_cc_locations__mailing_city %}
    Mailing city of property owner
{% enddocs %}
{% docs parcels_cc_locations__mailing_zip %}
    5-digit mailing zip code of property owner
{% enddocs %}
{% docs parcels_cc_locations__mailing_state %}
    Mailing state of property owner
{% enddocs %}
{% docs parcels_cc_locations__ohare_noise %}
    Indicator for properties near O'Hare airport flight paths
{% enddocs %}
{% docs parcels_cc_locations__floodplain %}
    Indicator for properties within FEMA-defined floodplains
{% enddocs %}
{% docs parcels_cc_locations__fs_flood_factor %}
    The property's First Street Flood Factor, a numeric integer from 1-10 (where 1 = minimal and 10 = extreme) based on flooding risk to the building footprint. Flood risk is defined as a combination of cumulative risk over 30 years and flood depth. Flood depth is calculated at the lowest elevation of the building footprint (large
{% enddocs %}
{% docs parcels_cc_locations__fs_flood_risk_direction %}
    The property's flood risk direction represented in a numeric value based on the change in risk for the location from 2020 to 2050 for the climate model realization of the RCP 4.5 mid emissions scenario. -1 = descreasing, 0 = stationary, 1 = increasing. Data provided by First Street and academics at UPenn.
{% enddocs %}
{% docs parcels_cc_locations__withinmr100 %}
    Indicator for properties within 100 feet of a major road. Roads taken from OpenStreetMap
{% enddocs %}
{% docs parcels_cc_locations__withinmr101300 %}
    Indicator for properties between 101-300 feet of a major road. Roads taken from OpenStreetMap
{% enddocs %}
{% docs parcels_cc_locations__indicator_has_address %}
    Indicator for whether PIN has complete property address
{% enddocs %}
{% docs parcels_cc_locations__indicator_has_latlon %}
    Indicator for whether PIN has latitude/longitude and attached spatial data (tract population, ward, etc.)
{% enddocs %}
{% docs parcels_cc_locations__longitude %}
    Longitude, derived from the centroid of the parcel polygon containing the property
{% enddocs %}
{% docs parcels_cc_locations__latitude %}
    Latitude, derived from the centroid of the parcel polygon containing the property
{% enddocs %}



