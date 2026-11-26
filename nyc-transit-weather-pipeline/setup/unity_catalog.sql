-- Unity Catalog set up for nyc-transit-weather-pipeline

-- Catalogs
create catalog if not exists nyc_transit_prod
comment 'Production: written only by deployed jobs'; 

create catalog if not exists nyc_transit_dev
comment 'Development and CI: per-developer and per-PR schemas'; 

-- Remove auto-created default schemas
drop schema if exists nyc_transit_prod.default; 
drop schema if exists nyc_transit_dev.default; 


-- Prod schemas
create schema if not exists nyc_transit_prod.landing
comment 'Volumes only: downloaded files and Auto Loader checkpoints'; 

create schema if not exists nyc_transit_prod.bronze
comment 'Unaltered source data as Delta tables'; 

create schema if not exists nyc_transit_prod.silver
comment 'Cleaned, typed, deduped'; 

create schema if not exists nyc_transit_prod.gold
comment 'Segment traversal joined to weather'; 

-- Dev schemas (layer schemas are created per developer / per PR by dbt and the bundle)
create schema if not exists nyc_transit_dev.landing
comment 'Volumes only: dev Auto Loader checkpoints'; 

-- Prod Volumes 
create volume if not exists nyc_transit_prod.landing.files
comment 'Downloaded files from all sources, unmodified'; 

create volume if not exists nyc_transit_prod.landing.checkpoints
comment 'Auto Loader state for prod streams'; 

-- Dev volumes
create volume if not exists nyc_transit_dev.landing.files
comment 'Test runs of ingestion code only; dev transformations read prod landing files';

create volume if not exists nyc_transit_dev.landing.checkpoints
comment 'Auto loader state for dev streams, one folder per owner'; 


