use scylla::DeserializeRow;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, DeserializeRow)]
pub struct ShopItem {
    pub category: String,
    pub item_id: i32,
    pub name: Option<String>,
    pub description: Option<String>,
    pub price: Option<i64>,
    pub effects: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, DeserializeRow)]
pub struct PlayerItem {
    pub user_id: i64,
    pub item_id: i32,
    pub quantity: Option<i32>,
    pub purchase_date: Option<i64>,
    pub is_active: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateShopItemRequest {
    pub category: String,
    pub name: String,
    pub description: Option<String>,
    pub price: i64,
    pub effects: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurchaseItemRequest {
    pub user_id: i64,
    pub item_id: i32,
    pub quantity: i32,
}
