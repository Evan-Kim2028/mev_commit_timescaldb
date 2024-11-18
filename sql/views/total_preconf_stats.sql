-- Create the api schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS api;

-- Drop the materialized view if it exists to allow updates
DROP MATERIALIZED VIEW IF EXISTS api.total_preconf_stats CASCADE;

-- Create the materialized view
CREATE MATERIALIZED VIEW api.total_preconf_stats AS
WITH 
encrypted_stores AS (
    SELECT commitmentIndex, committer, commitmentDigest 
    FROM public.unopenedcommitmentstored
),
commit_stores AS (
    SELECT * FROM public.openedcommitmentstored
),
commits_processed AS (
    SELECT commitmentIndex, isSlash 
    FROM public.commitmentprocessed
),
l1_transactions AS (
    SELECT * FROM public.l1transactions
),
commitments_intermediate AS (
    SELECT 
        es.commitmentIndex,
        es.committer,
        es.commitmentDigest,
        '0x' || cs.txnHash AS txnHash,
        cp.isSlash,
        cs.blocknumber AS inc_block_number,
        l1.hash,
        l1.timestamp,
        l1.extra_data AS builder_graffiti,
        cs.bidder,
        cs.bid,
        cs.decayStartTimeStamp,
        cs.decayEndTimeStamp,
        cs.dispatchTimestamp
    FROM encrypted_stores es
    INNER JOIN commit_stores cs ON es.commitmentIndex = cs.commitmentIndex
    INNER JOIN commits_processed cp ON es.commitmentIndex = cp.commitmentIndex
    INNER JOIN l1_transactions l1 ON '0x' || cs.txnHash = l1.hash
),
commitments_final AS (
    SELECT 
        *,
        CAST(bid AS NUMERIC) / POWER(10, 18) AS bid_eth,
        TO_TIMESTAMP(timestamp / 1000) AS date,
        GREATEST(
            CASE 
                WHEN (decayEndTimeStamp - dispatchTimestamp) = 0 THEN 0
                ELSE (decayEndTimeStamp - decayStartTimeStamp)::FLOAT / (decayEndTimeStamp - dispatchTimestamp)
            END, 
            0
        ) AS decay_multiplier,
        GREATEST(
            CASE 
                WHEN (decayEndTimeStamp - dispatchTimestamp) = 0 THEN 0
                ELSE (decayEndTimeStamp - decayStartTimeStamp)::FLOAT / (decayEndTimeStamp - dispatchTimestamp)
            END, 
            0
        ) * (CAST(bid AS NUMERIC) / POWER(10, 18)) AS decayed_bid_eth
    FROM commitments_intermediate
)
SELECT 
    ROUND(SUM(decayed_bid_eth)::NUMERIC, 4) AS total_decayed_bid_eth,
    COUNT(*) AS total_commitments,
    COUNT(distinct bidder) as bidder_count,
    ROUND(AVG(decayed_bid_eth)::NUMERIC, 4) AS avg_decayed_bid_eth,
    ROUND(MAX(decayed_bid_eth)::NUMERIC, 4) AS max_decayed_bid_eth,
    ROUND(MIN(decayed_bid_eth)::NUMERIC, 4) AS min_decayed_bid_eth
FROM commitments_final;
