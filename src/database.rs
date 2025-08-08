use anyhow::Result;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::Compression;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Clone)]
pub struct Database {
    pub session: Arc<Session>,
}

impl Database {
    pub async fn new() -> Result<Self> {
        let hosts = std::env::var("SCYLLA_HOSTS").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

        let keyspace = std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "ta_game".to_string());

        let hosts: Vec<&str> = hosts.split(',').collect();

        let session = SessionBuilder::new()
            .known_nodes(&hosts)
            .compression(Some(Compression::Lz4))
            .tcp_keepalive_interval(Duration::from_secs(30))
            .tcp_nodelay(true)
            .connection_timeout(Duration::from_secs(10))
            .build()
            .await?;

        Self::create_keyspace(&session, &keyspace).await?;
        session.use_keyspace(&keyspace, false).await?;
        Self::create_tables(&session).await?;

        info!("Connected to ScyllaDB with keyspace: {}", keyspace);

        Ok(Self {
            session: Arc::new(session),
        })
    }

    async fn create_keyspace(session: &Session, keyspace: &str) -> Result<()> {
        let query = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}} AND DURABLE_WRITES = true",
            keyspace
        );

        session.query_unpaged(query, &[]).await?;
        Ok(())
    }

    async fn create_tables(session: &Session) -> Result<()> {
        // Players table
        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS players (
                user_id bigint PRIMARY KEY,
                username text,
                first_name text,
                auth_date timestamp,
                score bigint,
                level int,
                energy int,
                max_energy int,
                energy_last_recharged timestamp,
                tap_value int,
                energy_recharge_rate int,
                referral_code text,
                referred_by_id bigint,
                last_seen timestamp
            )",
                &[],
            )
            .await?;

        // Referrals table
        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS referrals (
                referrer_id bigint,
                referred_user_id bigint,
                referral_date timestamp,
                PRIMARY KEY (referrer_id, referred_user_id)
            )",
                &[],
            )
            .await?;

        // Tasks table
        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS tasks (
                task_type text,
                task_id uuid,
                title text,
                description text,
                completion_target int,
                reward_score bigint,
                PRIMARY KEY (task_type, task_id)
            )",
                &[],
            )
            .await?;

        // Player tasks table
        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS player_tasks (
                user_id bigint,
                task_id uuid,
                progress int,
                completed_at timestamp,
                claimed_at timestamp,
                PRIMARY KEY (user_id, task_id)
            )",
                &[],
            )
            .await?;

        // Shop items table
        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS shop_items (
                category text,
                item_id int,
                name text,
                description text,
                price bigint,
                effects map<text, text>,
                PRIMARY KEY (category, item_id)
            )",
                &[],
            )
            .await?;

        // Player items table
        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS player_items (
                user_id bigint,
                item_id int,
                quantity int,
                purchase_date timestamp,
                is_active boolean,
                PRIMARY KEY (user_id, item_id)
            )",
                &[],
            )
            .await?;

        // Leaderboard table
        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS leaderboard (
                shard_id int,
                user_id bigint,
                score bigint,
                username text,
                PRIMARY KEY (shard_id, user_id)
            )",
                &[],
            )
            .await?;

        info!("All tables created successfully");
        Ok(())
    }
}
