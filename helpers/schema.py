from google.cloud.bigquery import SchemaField

location_dim_schema = [
    SchemaField("id", "STRING", mode="REQUIRED", max_length=64),
    SchemaField("city", "STRING", mode="REQUIRED", max_length=50),
    SchemaField("country", "STRING", mode="REQUIRED", max_length=50),
    SchemaField("state", "STRING", mode="REQUIRED", max_length=50),
    SchemaField("latitude", "FLOAT", mode="REQUIRED"),
    SchemaField("longitude", "FLOAT", mode="REQUIRED"),
    SchemaField("timezone", "STRING", mode="REQUIRED", max_length=50),
    SchemaField("timezone_offset", "INTEGER", mode="REQUIRED"),
    SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]

weather_type_dim_schema = [
    SchemaField("id", "STRING", mode="REQUIRED", max_length=64),
    SchemaField("weather", "STRING", mode="REQUIRED", max_length=50),
    SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]
weather_fact_schema = [
    SchemaField("id", "STRING", mode="REQUIRED", max_length=64),
    SchemaField("location_id", "STRING", max_length=64),
    SchemaField("date_id", "STRING", max_length=64),
    SchemaField("weather_type_id", "STRING", max_length=64),
    SchemaField("temperature", "FLOAT", mode="REQUIRED"),
    SchemaField("feels_like", "FLOAT", mode="REQUIRED"),
    SchemaField("pressure", "INTEGER", mode="REQUIRED"),
    SchemaField("humidity", "INTEGER", mode="REQUIRED"),
    SchemaField("dew_point", "FLOAT", mode="REQUIRED"),
    SchemaField("ultraviolet_index", "FLOAT", mode="REQUIRED"),
    SchemaField("clouds", "INTEGER", mode="REQUIRED"),
    SchemaField("visibility", "INTEGER", mode="REQUIRED"),
    SchemaField("date", "DATE", mode="REQUIRED"),
    SchemaField("wind_speed", "FLOAT", mode="REQUIRED"),
    SchemaField("wind_deg", "INTEGER", mode="REQUIRED"),
    SchemaField("sunrise", "INTEGER", mode="REQUIRED"),
    SchemaField("sunset", "INTEGER", mode="REQUIRED"),
    SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]

date_dim_schema = [
    SchemaField("id", "STRING", mode="REQUIRED", max_length=64),
    SchemaField("date", "DATE", mode="REQUIRED"),
    SchemaField("year", "INTEGER", mode="REQUIRED"),
    SchemaField("month", "INTEGER", mode="REQUIRED"),
    SchemaField("day", "INTEGER", mode="REQUIRED"),
    SchemaField("day_of_week", "STRING", mode="REQUIRED", max_length=50),
    SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]
