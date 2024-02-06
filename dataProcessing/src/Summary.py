import pandas as pd

summary = pd.DataFrame(
    columns=[
        "Site Name",
        "Production",
        "Irradiance",
        "Inverter",
        "Max Inverter Blanks",
    ]
)
production_status = ""
irradiance_status = ""
inverter_status = ""
max_missing_count = 0
max_missing_inverters = ""
