# Step 1: Set Up PostgREST Roles and Permissions
Access the TimescaleDB container:

`bash
docker exec -it mev_timescaledb psql -U postgres -d mev_commit_testnet
`

# Step 2: Execute the following SQL commands to create and configure the `anon` role for PostgREST:

```SQL
CREATE ROLE anon NOLOGIN;

-- Grant SELECT permissions on all public schema tables to `anon`
GRANT USAGE ON SCHEMA public TO anon;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon;

-- Apply default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO anon;

-- Verify the tables are accessible by the anon role
SELECT tablename FROM pg_tables WHERE schemaname = 'public';
```


### Install cronjob
cronjob is required to schedule matierialized view queries.
`docker exec -it mev_timescaledb psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_cron;"`

