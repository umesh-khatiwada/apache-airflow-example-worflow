-- Used by 11_connections_and_hooks
CREATE TABLE IF NOT EXISTS weather_observations (
    id           SERIAL PRIMARY KEY,
    city         VARCHAR(50)  NOT NULL,
    temperature  NUMERIC(5, 1),
    windspeed    NUMERIC(5, 1),
    source       VARCHAR(10),
    run_id       VARCHAR(250) NOT NULL,
    observed_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);
