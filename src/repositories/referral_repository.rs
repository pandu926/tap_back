use crate::{database::Database, models::referral::Referral};

use anyhow::Result;
use chrono::Utc;
use scylla::{statement::prepared::PreparedStatement, value::CqlTimestamp};
use std::sync::Arc;
#[derive(Clone)]
pub struct ReferralRepository {
    db: Database,
    create_referral_stmt: Arc<PreparedStatement>,
    get_referral_stmt: Arc<PreparedStatement>,
    get_referrals_by_referrer_stmt: Arc<PreparedStatement>,
    get_referrals_by_referred_stmt: Arc<PreparedStatement>,
    delete_referral_stmt: Arc<PreparedStatement>,
    count_referrals_stmt: Arc<PreparedStatement>,
}

impl ReferralRepository {
    pub async fn new(db: Database) -> Result<Self> {
        let create_referral_stmt = Arc::new(
            db.session
                .prepare(
                    "INSERT INTO referrals (referrer_id, referred_user_id, referral_date) 
                VALUES (?, ?, ?)",
                )
                .await?,
        );

        let get_referral_stmt = Arc::new(
            db.session
                .prepare(
                    "SELECT referrer_id, referred_user_id, referral_date 
                FROM referrals WHERE referrer_id = ? AND referred_user_id = ?",
                )
                .await?,
        );

        let get_referrals_by_referrer_stmt = Arc::new(
            db.session
                .prepare(
                    "SELECT referrer_id, referred_user_id, referral_date 
                FROM referrals WHERE referrer_id = ?",
                )
                .await?,
        );

        let get_referrals_by_referred_stmt = Arc::new(
            db.session
                .prepare(
                    "SELECT referrer_id, referred_user_id, referral_date 
                FROM referrals WHERE referred_user_id = ?",
                )
                .await?,
        );

        let delete_referral_stmt = Arc::new(
            db.session
                .prepare("DELETE FROM referrals WHERE referrer_id = ? AND referred_user_id = ?")
                .await?,
        );

        let count_referrals_stmt = Arc::new(
            db.session
                .prepare("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?")
                .await?,
        );

        Ok(Self {
            db,
            create_referral_stmt,
            get_referral_stmt,
            get_referrals_by_referrer_stmt,
            get_referrals_by_referred_stmt,
            delete_referral_stmt,
            count_referrals_stmt,
        })
    }

    pub async fn create_referral(&self, referrer_id: i64, referred_user_id: i64) -> Result<()> {
        let now = CqlTimestamp(Utc::now().timestamp_millis());

        self.db
            .session
            .execute_unpaged(
                &self.create_referral_stmt,
                (referrer_id, referred_user_id, now),
            )
            .await?;

        Ok(())
    }

    pub async fn get_referral(
        &self,
        referrer_id: i64,
        referred_user_id: i64,
    ) -> Result<Option<Referral>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_referral_stmt, (referrer_id, referred_user_id))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute get_referral_stmt: {}", e))?;

        let row = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to extract rows: {}", e))?
            .maybe_first_row::<Referral>()
            .map_err(|e| anyhow::anyhow!("Failed to parse Referral: {}", e))?;

        Ok(row)
    }

    pub async fn get_referrals_by_referrer(&self, referrer_id: i64) -> Result<Vec<Referral>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_referrals_by_referrer_stmt, (referrer_id,))
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to execute get_referrals_by_referrer_stmt: {}", e)
            })?;

        let rows = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to extract rows: {}", e))?;

        let referrals: Vec<Referral> = rows
            .rows::<Referral>()?
            .map(|row| row.map_err(|e| anyhow::anyhow!("Failed to parse Referral: {}", e)))
            .collect::<std::result::Result<_, _>>()?;

        Ok(referrals)
    }

    pub async fn get_referrals_by_referred(&self, referred_user_id: i64) -> Result<Vec<Referral>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_referrals_by_referred_stmt, (referred_user_id,))
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to execute get_referrals_by_referred_stmt: {}", e)
            })?;

        let rows = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to extract rows: {}", e))?;

        let referrals: Vec<Referral> = rows
            .rows::<Referral>()?
            .map(|row| row.map_err(|e| anyhow::anyhow!("Failed to parse Referral: {}", e)))
            .collect::<std::result::Result<_, _>>()?;

        Ok(referrals)
    }

    pub async fn delete_referral(&self, referrer_id: i64, referred_user_id: i64) -> Result<()> {
        self.db
            .session
            .execute_unpaged(&self.delete_referral_stmt, (referrer_id, referred_user_id))
            .await?;

        Ok(())
    }

    pub async fn count_referrals(&self, referrer_id: i64) -> Result<i64> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.count_referrals_stmt, (referrer_id,))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute count_referrals_stmt: {}", e))?;

        let count = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to get rows"))?
            .maybe_first_row::<(i64,)>()
            .map_err(|e| anyhow::anyhow!("Failed to parse count row: {}", e))?
            .map(|(count,)| count)
            .unwrap_or(0);

        Ok(count)
    }
}
