-- Dedicated metadata database for the local DuckLake catalog. Production should provision and
-- back this database up independently; sharing the compose PostgreSQL server is only a dev economy.
CREATE DATABASE walrus_ducklake;
