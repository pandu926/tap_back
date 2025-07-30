-- migrations/001_initial.sql

-- Create players table for storing current user state
CREATE TABLE players (
    user_id BIGINT PRIMARY KEY,
    score BIGINT NOT NULL DEFAULT 0,
    energy INT NOT NULL DEFAULT 1000,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create index for leaderboard queries
CREATE INDEX idx_players_score ON players (score DESC);
CREATE INDEX idx_players_last_seen ON players (last_seen);

-- Create tap_events table for raw event logging and analytics
CREATE TABLE tap_events (
    event_time TIMESTAMPTZ NOT NULL,
    user_id BIGINT NOT NULL,
    tap_count INT NOT NULL
);

-- Create hypertable if TimescaleDB is available (optional)
-- SELECT create_hypertable('tap_events', 'event_time');

-- Create indexes for tap_events
CREATE INDEX idx_tap_events_user_time ON tap_events (user_id, event_time DESC);
CREATE INDEX idx_tap_events_time ON tap_events (event_time DESC);

-- Add some useful functions

-- Function to get user rank efficiently
CREATE OR REPLACE FUNCTION get_user_rank(target_user_id BIGINT)
RETURNS BIGINT AS $$
DECLARE
    user_rank BIGINT;
BEGIN
    SELECT rank INTO user_rank FROM (
        SELECT user_id, RANK() OVER (ORDER BY score DESC) as rank
        FROM players
    ) ranked
    WHERE user_id = target_user_id;
    
    RETURN COALESCE(user_rank, 0);
END;
$$ LANGUAGE plpgsql;

-- Function to update player stats atomically
CREATE OR REPLACE FUNCTION update_player_stats(
    target_user_id BIGINT,
    score_delta BIGINT DEFAULT 0,
    energy_delta INT DEFAULT 0
)
RETURNS TABLE(new_score BIGINT, new_energy INT) AS $$
BEGIN
    INSERT INTO players (user_id, score, energy, last_seen)
    VALUES (target_user_id, score_delta, 1000 + energy_delta, NOW())
    ON CONFLICT (user_id) DO UPDATE
    SET 
        score = players.score + score_delta,
        energy = GREATEST(0, LEAST(1000, players.energy + energy_delta)),
        last_seen = NOW();
    
    RETURN QUERY
    SELECT p.score, p.energy
    FROM players p
    WHERE p.user_id = target_user_id;
END;
$$ LANGUAGE plpgsql;

-- Create a view for leaderboard with additional stats
CREATE VIEW leaderboard_view AS
SELECT 
    p.user_id,
    p.score,
    p.energy,
    p.last_seen,
    p.created_at,
    RANK() OVER (ORDER BY p.score DESC) as rank,
    COALESCE(recent_taps.total_24h, 0) as taps_last_24h
FROM players p
LEFT JOIN (
    SELECT 
        user_id,
        SUM(tap_count) as total_24h
    FROM tap_events
    WHERE event_time >= NOW() - INTERVAL '24 hours'
    GROUP BY user_id
) recent_taps ON p.user_id = recent_taps.user_id
ORDER BY p.score DESC;