# variables = {
#     "incoming variable name from data provider": {
#         "vargem": "Synoptic Variable Table Vargem",
#         "VNUM": "Synotic VNUM",
#         "long_name": "Long name description",
#         "incoming_unit": "incoming data unit from data provider",
#         "final_unit": "final unit that POE is expecting (should be metric unit if keeping units in json slug)"
#     }
# }

# for example, here is a key from the variables dict used in the nasa globe ingest

variables = {
    "grass_min_temp": {
        "vargem": "GSMN",    # Grass Surface Minimum (temperature)
        "VNUM": "1",
        "long_name": "Daily Grass Minimum Temperature",
        "incoming_unit": "degC",
        "final_unit": "degC",
        "context": "min",
        "statistic": "24h"
    },
#    "mean_cloud": {
#        "vargem": "CHC1",    # CLouD Cover
#        "VNUM": "1",
#        "long_name": "Daily Mean Amount of Cloud",
#        "incoming_unit": "oktas",
#        "final_unit": "code",
#        "context": "mean",
#        "statistic": "10min"
#    },
    "total_rainfall": {
        "vargem": "P24I",    # PreCiPitatioN
        "VNUM": "1",
        "long_name": "Daily Total Rainfall",
        "incoming_unit": "mm",
        "final_unit": "mm",
        "context": "sum",
        "statistic": "24h"
    },
    "mean_humidity": {
        "vargem": "RELH",    # RELative Humidity
        "VNUM": "1",
        "long_name": "Daily Mean Relative Humidity",
        "incoming_unit": "pct",
        "final_unit": "pct",
        "context": "mean",
        "statistic": "24h"
    },

# How to report?    
    "max_mean_min_temp": {
        "vargem": "TMPF",    # TeMPerature (air, dry-bulb)
        "VNUM": "1",         # column 2 = daily mean; col 1 = max; col 3 = min
        "long_name": "Daily Mean Air Temperature",
        "incoming_unit": "degC",
        "final_unit": "degC",
        "context": "mean",
        "statistic": "24h"
    },

#    "mean_sea_temp": {
#        "vargem": "",    # Mean Sea Surface Temperature
#        "VNUM": "1",
#        "long_name": "Daily Mean Sea Temperature",
#        "incoming_unit": "degC",
#        "final_unit": "degC",
#        "context": "mean",
#        "statistic": "10min"
#    },
#    "visibility_hours": {
#        "vargem": "VSBY",    # VISiBility (hours of reduced)
#        "VNUM": "1",
#        "long_name": "Daily Number of Hours of Reduced Visibility",
#        "incoming_unit": "hours",
#        "final_unit": "hours",
#        "context": "mean",
#        "statistic": "10min"
#    },

# How to report?    
    "heat_index": {
        "vargem": "HIDX",    # Hong Kong Heat Index
        "VNUM": "1",         # column 2 = mean; col 1 = max
        "long_name": "Daily Mean Hong Kong Heat Index",
        "incoming_unit": "degC",
        "final_unit": "degC",
        "context": "mean",
        "statistic": "24h"
    },
    "dew_point_temp": {
        "vargem": "DWPF",    # Dew Point TemPerature
        "VNUM": "1",
        "long_name": "Daily Mean Dew Point Temperature",
        "incoming_unit": "degC",
        "final_unit": "degC",
        "context": "mean",
        "statistic": "24h"
    },
    "wet_bulb_temp": {
        "vargem": "TWBF",    # Wet Bulb TemPerature
        "VNUM": "1",
        "long_name": "Daily Mean Wet Bulb Temperature",
        "incoming_unit": "degC",
        "final_unit": "degC",
        "context": "mean",
        "statistic": "24h"
    },
    "mean_pressure": {
        "vargem": "PRES",    # Mean Sea Level Pressure
        "VNUM": "1",
        "long_name": "Daily Mean Pressure",
        "incoming_unit": "hPa", # not sure
        "final_unit": "Pa",
        "context": "mean",
        "statistic": "24h"
    },
    "total_evaporation": {
        "vargem": "EVAP",    # EVAPoration
        "VNUM": "1",
        "long_name": "Daily Total Evaporation",
        "incoming_unit": "mm",
        "final_unit": "mm",
        "context": "mean",
        "statistic": "24h"
    },
    "solar_radiation": {
        "vargem": "SOLR",    # Global Solar RaDiation
        "VNUM": "1",
        "long_name": "Daily Global Solar Radiation",
        "incoming_unit": "mjpm2", # not sure
        "final_unit": "wpm2",
        "context": "mean",
        "statistic": "24h"
    },
#    "uv_index": {
#        "vargem": "UVID",    # UV Index Daily
#        "VNUM": "1",         # column 1 = mean; col 2 = max
#        "long_name": "Daily Mean UV Index",
#        "incoming_unit": "dimensionless",
#        "final_unit": "Index", 
#        "context": "mean", Index units (unitless)
#        "statistic": "24h"
#    },
    "sunshine_hours": {
        "vargem": "MSUN",    # SunShine HouRs
        "VNUM": "1",
        "long_name": "Daily Total Bright Sunshine",
        "incoming_unit": "hours",
        "final_unit": "min",
        "context": "sum",
        "statistic": "24h"
    },
    "wind_direction": {
        "vargem": "DRCT",    # Wind DIRection (prevailing)
        "VNUM": "1",
        "long_name": "Daily Prevailing Wind Direction",
        "incoming_unit": "degrees",
        "final_unit": "degrees"
    },
    "mean_wind_speed": {
        "vargem": "SKNT",    # Wind SPeeD
        "VNUM": "1",
        "long_name": "Daily Mean Wind Speed",
        "incoming_unit": "km/h", # not sure
        "final_unit": "m/s",
        "context": "mean",
        "statistic": "24h"
    },
}