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
        "long_name": "Air Temperature (1-minute)",
        "incoming_unit": "degC",
        "final_unit": "degC",
        "context": "mean",
        "statistic": "1min"
    },
    "relative_humidity": {
        "vargem": "RELH",
        "VNUM": "1",
        "long_name": "Relative Humidity (1-minute)",
        "incoming_unit": "pct",
        "final_unit": "pct",
        "context": "mean",
        "statistic": "1min"
    },
    "wind_speed": {
        "vargem": "SKNT",
        "VNUM": "1",
        "long_name": "Wind Speed (10-minute average)",
        "incoming_unit": "km/h",
        "final_unit": "m/s",
        "context": "mean",
        "statistic": "10min"
    },
    "wind_direction": {
        "vargem": "DRCT",
        "VNUM": "1",
        "long_name": "Wind Direction (10-minute average)",
        "incoming_unit": "degrees",
        "final_unit": "degrees",
        "context": "mean",
        "statistic": "10min"
    },
    "wind_gust": {
        "vargem": "GUST",
        "VNUM": "1",
        "long_name": "Maximum Wind Gust (10-minute)",
        "incoming_unit": "km/h",
        "final_unit": "m/s",
        "context": "max",
        "statistic": "10min"
    },
    "sea_level_pressure": {
        "vargem": "PMSL",
        "VNUM": "1",
        "long_name": "Sea Level Pressure (1-minute)",
        "incoming_unit": "hPa",
        "final_unit": "Pa",
        "context": "mean",
        "statistic": "1min"
    },
    "solar_radiation": {
        "vargem": "SOLR",
        "VNUM": "1",
        "long_name": "Global Solar Radiation (1-minute)",
        "incoming_unit": "W/m^2",
        "final_unit": "W/m^2",
        "context": "mean",
        "statistic": "1min"
    },
    "visibility": {
        "vargem": "VSBY",
        "VNUM": "1",
        "long_name": "Visibility (10-minute)",
        "incoming_unit": "km",
        "final_unit": "km",
        "context": "mean",
        "statistic": "10min"
    },
    "precip_accum_one_hour": {
        "vargem": "P01I",
        "VNUM": "1",
        "long_name": "Precipitation Accumulation (1 hour)",
        "incoming_unit": "mm",
        "final_unit": "mm",
        "context": "sum",
        "statistic": "1h"
    },
    "precip_accum": {
        "vargem": "PREC",
        "VNUM": "1",
        "long_name": "Accumulated Precipitation",
        "incoming_unit": "mm",
        "final_unit": "mm",
        "context": "sum",
        "statistic": "24h"
    },
    "lightning_strike_count": {
        "vargem": "LTGS",
        "VNUM": "1",
        "long_name": "Lightning Strike Count (Hourly)",
        "incoming_unit": "count",
        "final_unit": "count",
        "context": "sum",
        "statistic": "hour"
    },
    "uv_index": {
        "vargem": "UVID",
        "VNUM": "1",
        "long_name": "Daily UV Index",
        "incoming_unit": "index",
        "final_unit": "index",
        "context": "mean",
        "statistic": "24h"
    },
    "gamma_radiation": {
        "vargem": "GRAD",
        "VNUM": "1",
        "long_name": "Hourly Gamma Radiation",
        "incoming_unit": "μSv/h",
        "final_unit": "μSv/h",
        "context": "mean",
        "statistic": "hour"
    },
    "weather_cond_code": {
        "vargem": "WNUM",
        "VNUM": "1",
        "long_name": "Weather Condition Code",
        "incoming_unit": "code",
        "final_unit": "code",
        "context": "categorical",
        "statistic": "24h"
    }
}