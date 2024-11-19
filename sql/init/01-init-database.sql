REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA api FROM PUBLIC;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS api;

-- Create roles with proper hierarchy for remote access
DO $$ 
BEGIN
    -- Create roles if they don't exist
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'api_user') THEN
        CREATE ROLE api_user LOGIN PASSWORD 'your_secure_password';
    END IF;
    
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'anon') THEN
        CREATE ROLE anon NOLOGIN;
    END IF;
END
$$
;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO api_user, anon;
GRANT USAGE ON SCHEMA api TO api_user, anon;

-- Grant table permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_user;
GRANT SELECT ON ALL TABLES IN SCHEMA api TO api_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon;
GRANT SELECT ON ALL TABLES IN SCHEMA api TO anon;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT ON TABLES TO api_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA api 
    GRANT SELECT ON TABLES TO api_user;

-- Grant specific permissions to postgres superuser
GRANT ALL PRIVILEGES ON DATABASE mev_commit_testnet TO postgres;
GRANT ALL ON SCHEMA public, api TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA api 
    GRANT ALL ON TABLES TO postgres;