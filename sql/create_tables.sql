CREATE TABLE IF NOT EXISTS raw_weather (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    elevation FLOAT,
    time TIMESTAMP NOT NULL,
    temperature_2m FLOAT,
    relative_humidity_2m FLOAT,
    precipitation FLOAT,
    wind_speed_10m FLOAT,
    pressure_msl FLOAT,
    cloud_cover FLOAT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (city, time)
);

CREATE TABLE IF NOT EXISTS weather_hourly (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    elevation FLOAT,
    time TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    day_of_week VARCHAR(20) NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_daytime BOOLEAN NOT NULL,
    temperature_celsius FLOAT,
    temperature_fahrenheit FLOAT,
    humidity_percent FLOAT,
    precipitation_mm FLOAT,
    precipitation_category VARCHAR(20),
    wind_speed_kmh FLOAT,
    wind_category VARCHAR(20),
    pressure_hpa FLOAT,
    cloud_cover_percent FLOAT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (city, time)
);

CREATE TABLE IF NOT EXISTS daily_weather_summary (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    avg_temperature FLOAT,
    min_temperature FLOAT,
    max_temperature FLOAT,
    avg_humidity FLOAT,
    total_precipitation FLOAT,
    avg_wind_speed FLOAT,
    max_wind_speed FLOAT,
    avg_pressure FLOAT,
    avg_cloud_cover FLOAT,
    daytime_avg_temp FLOAT,
    nighttime_avg_temp FLOAT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (city, date)
);

CREATE TABLE IF NOT EXISTS spark_city_analytics (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    avg_temperature FLOAT,
    rolling_avg_7d FLOAT,
    temperature_rank INTEGER,
    city_mean_temp FLOAT,
    city_stddev_temp FLOAT,
    is_anomaly BOOLEAN,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (city, date)
);

CREATE TABLE IF NOT EXISTS pipeline_log (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    step VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    rows_affected INTEGER DEFAULT 0,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
