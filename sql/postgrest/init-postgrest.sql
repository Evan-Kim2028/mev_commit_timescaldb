-- Create the api schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS api;

-- Create the anon role if it doesn't exist
DO
$do$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'anon') THEN
      CREATE ROLE anon NOLOGIN;
   END IF;
END
$do$;

-- Grant usage on the api schema to anon
GRANT USAGE ON SCHEMA api TO anon;

-- Grant select on all EXISTING tables in api schema to anon
GRANT SELECT ON ALL TABLES IN SCHEMA api TO anon;

-- Grant select on all FUTURE tables in api schema to anon
ALTER DEFAULT PRIVILEGES IN SCHEMA api GRANT SELECT ON TABLES TO anon;