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
    "air_temp": {
        "vargem": "TMPF",
        "VNUM": "1",
        "long_name": "The latest 1-minute mean air temperature",
        "incoming_unit": "degC",
        "final_unit": "degC",
        "context": "mean",
        "statistic": "1 min"
    },
    "mean_humidity": {
        "vargem": "RELH",
        "VNUM": "1",
        "long_name": "The latest 1-minute mean relative humidity",
        "incoming_unit": "pct",
        "final_unit": "pct",
        "context": "mean",
        "statistic": "1 min"
    },    
    "wind_speed": {
        "vargem": "SKNT",
        "VNUM": "1",
        "long_name": "The latest 10-minute mean wind direction and wind speed and maximum gust",
        "incoming_unit": "km/h",
        "final_unit": "m/s",
        "context": "mean",
        "statistic": "10 min"
    },
    "wind_gust":{
        "vargem": "GUST",
        "VNUM": "1",
        "long_name": "The latest 10-minute maximum wind gust",
        "incoming_unit": "km/h",
        "final_unit": "m/s",
        "context": "max",
        "statistic": "10 min"
    },
    "mean_pressure": {
        "vargem": "PMSL",
        "VNUM": "1",
        "long_name": "The latest 1-minute mean sea level pressure",
        "incoming_unit": "hPa",
        "final_unit": "Pa",
        "context": "mean",
        "statistic": "1 min"
    },
    "solar_radiation": {
        "vargem": "SOLR",
        "VNUM": "1",
        "long_name": "Global Solar Radiation (Daily)",
        "incoming_unit": "W/m²",
        "final_unit": "W/m²"
    },
    "diffuse_radiation": {
        "vargem": "DIFR",
        "VNUM": "1",
        "long_name": "Diffuse Radiation (Daily)",
        "incoming_unit": "W/m²",
        "final_unit": "W/m²"
    },  
    "grass_min_temp": {
        "vargem": "TMPG",
        "VNUM": "1",
        "long_name": "the latest 1-minute mean grass temperature from 5:00 p.m. to 8:00 a.m. the following date Hong Kong time",
        "incoming_unit": "degC",
        "final_unit": "degC",
        "context": "mean",
        "statistic": "1 min"
    },
    "visibility": {
        "vargem": "VSBY",
        "VNUM": "1",
        "long_name": "the latest 10-minute mean visibility",
        "incoming_unit": "km",
        "final_unit": "statutemile",
        "context": "mean",
        "statistic": "10 min"
    },
}
