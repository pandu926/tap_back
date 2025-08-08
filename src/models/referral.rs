use scylla::DeserializeRow;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, DeserializeRow)]
pub struct Referral {
    pub referrer_id: i64,
    pub referred_user_id: i64,
    pub referral_date: Option<i64>,
}
