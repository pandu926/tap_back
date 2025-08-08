use crate::database::Database;
use crate::models::shop::{CreateShopItemRequest, PlayerItem, PurchaseItemRequest, ShopItem};

use anyhow::Result;
use chrono::Utc;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;
use std::collections::HashMap;
use std::sync::Arc;
#[derive(Clone)]
pub struct ShopRepository {
    db: Database,
    create_shop_item_stmt: Arc<PreparedStatement>,
    get_shop_item_stmt: Arc<PreparedStatement>,
    get_shop_items_by_category_stmt: Arc<PreparedStatement>,
    update_shop_item_stmt: Arc<PreparedStatement>,
    delete_shop_item_stmt: Arc<PreparedStatement>,
    purchase_item_stmt: Arc<PreparedStatement>,
    get_player_item_stmt: Arc<PreparedStatement>,
    get_player_items_stmt: Arc<PreparedStatement>,
    update_player_item_stmt: Arc<PreparedStatement>,
    activate_item_stmt: Arc<PreparedStatement>,
}

impl ShopRepository {
    pub async fn new(db: Database) -> Result<Self> {
        let create_shop_item_stmt = Arc::new(
            db.session
                .prepare(
                    "INSERT INTO shop_items (category, item_id, name, description, price, effects) 
                VALUES (?, ?, ?, ?, ?, ?)",
                )
                .await?,
        );

        let get_shop_item_stmt = Arc::new(
            db.session
                .prepare(
                    "SELECT category, item_id, name, description, price, effects 
                FROM shop_items WHERE category = ? AND item_id = ?",
                )
                .await?,
        );

        let get_shop_items_by_category_stmt = Arc::new(
            db.session
                .prepare(
                    "SELECT category, item_id, name, description, price, effects 
                FROM shop_items WHERE category = ?",
                )
                .await?,
        );

        let update_shop_item_stmt = Arc::new(
            db.session
                .prepare(
                    "UPDATE shop_items SET name = ?, description = ?, price = ?, effects = ? 
                WHERE category = ? AND item_id = ?",
                )
                .await?,
        );

        let delete_shop_item_stmt = Arc::new(
            db.session
                .prepare("DELETE FROM shop_items WHERE category = ? AND item_id = ?")
                .await?,
        );

        let purchase_item_stmt = Arc::new(
            db.session.prepare(
                "INSERT INTO player_items (user_id, item_id, quantity, purchase_date, is_active) 
                VALUES (?, ?, ?, ?, ?) IF NOT EXISTS"
            ).await?
        );

        let get_player_item_stmt = Arc::new(
            db.session
                .prepare(
                    "SELECT user_id, item_id, quantity, purchase_date, is_active 
                FROM player_items WHERE user_id = ? AND item_id = ?",
                )
                .await?,
        );

        let get_player_items_stmt = Arc::new(
            db.session
                .prepare(
                    "SELECT user_id, item_id, quantity, purchase_date, is_active 
                FROM player_items WHERE user_id = ?",
                )
                .await?,
        );

        let get_quantity_stmt = Arc::new(
            db.session
                .prepare("SELECT quantity FROM player_items WHERE user_id = ? AND item_id = ?")
                .await?,
        );

        let update_player_item_stmt = Arc::new(
            db.session
                .prepare("UPDATE player_items SET quantity = ? WHERE user_id = ? AND item_id = ?")
                .await?,
        );

        let activate_item_stmt = Arc::new(
            db.session
                .prepare("UPDATE player_items SET is_active = ? WHERE user_id = ? AND item_id = ?")
                .await?,
        );

        Ok(Self {
            db,
            create_shop_item_stmt,
            get_shop_item_stmt,
            get_shop_items_by_category_stmt,
            update_shop_item_stmt,
            delete_shop_item_stmt,
            purchase_item_stmt,
            get_player_item_stmt,
            get_player_items_stmt,
            update_player_item_stmt,
            activate_item_stmt,
        })
    }

    pub async fn create_shop_item(&self, request: CreateShopItemRequest) -> Result<i32> {
        // Generate item_id (in production, use proper ID generation)
        let item_id = rand::random::<i32>().abs();

        self.db
            .session
            .execute_unpaged(
                &self.create_shop_item_stmt,
                (
                    &request.category,
                    item_id,
                    &request.name,
                    request.description.as_ref(),
                    request.price,
                    request.effects.as_ref(),
                ),
            )
            .await?;

        Ok(item_id)
    }

    pub async fn get_shop_item(&self, category: &str, item_id: i32) -> Result<Option<ShopItem>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_shop_item_stmt, (category, item_id))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute get_shop_item_stmt: {}", e))?;

        let item = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to extract rows: {}", e))?
            .maybe_first_row::<ShopItem>()
            .map_err(|e| anyhow::anyhow!("Failed to parse ShopItem: {}", e))?;

        Ok(item)
    }

    pub async fn get_shop_items_by_category(&self, category: &str) -> Result<Vec<ShopItem>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_shop_items_by_category_stmt, (category,))
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to execute get_shop_items_by_category_stmt: {}", e)
            })?;

        let rows = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to extract rows: {}", e))?;

        let items: Vec<ShopItem> = rows
            .rows::<ShopItem>()?
            .map(|row| row.map_err(|e| anyhow::anyhow!("Failed to parse ShopItem: {}", e)))
            .collect::<std::result::Result<_, _>>()?;

        Ok(items)
    }

    pub async fn update_shop_item(
        &self,
        category: &str,
        item_id: i32,
        name: &str,
        description: Option<&str>,
        price: i64,
        effects: Option<&HashMap<String, String>>,
    ) -> Result<()> {
        self.db
            .session
            .execute_unpaged(
                &self.update_shop_item_stmt,
                (name, description, price, effects, category, item_id),
            )
            .await?;

        Ok(())
    }

    pub async fn delete_shop_item(&self, category: &str, item_id: i32) -> Result<()> {
        self.db
            .session
            .execute_unpaged(&self.delete_shop_item_stmt, (category, item_id))
            .await?;

        Ok(())
    }

    pub async fn purchase_item(&self, request: &PurchaseItemRequest) -> Result<()> {
        let now = CqlTimestamp(Utc::now().timestamp_millis());

        // Check if player already has this item
        if let Some(_existing) = self
            .get_player_item(request.user_id, request.item_id)
            .await?
        {
            // Update quantity if item exists
            self.db
                .session
                .execute_unpaged(
                    &self.update_player_item_stmt,
                    (request.quantity, request.user_id, request.item_id),
                )
                .await?;
        } else {
            // Insert new item
            self.db
                .session
                .execute_unpaged(
                    &self.purchase_item_stmt,
                    (
                        request.user_id,
                        request.item_id,
                        request.quantity,
                        now,
                        false,
                    ),
                )
                .await?;
        }

        Ok(())
    }

    pub async fn get_player_item(&self, user_id: i64, item_id: i32) -> Result<Option<PlayerItem>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_player_item_stmt, (user_id, item_id))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute get_player_item_stmt: {}", e))?;

        let item = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to extract rows: {}", e))?
            .maybe_first_row::<PlayerItem>()
            .map_err(|e| anyhow::anyhow!("Failed to parse PlayerItem: {}", e))?;

        Ok(item)
    }

    pub async fn get_player_items(&self, user_id: i64) -> Result<Vec<PlayerItem>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_player_items_stmt, (user_id,))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute get_player_items_stmt: {}", e))?;

        let rows = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to extract rows: {}", e))?;

        let items: Vec<PlayerItem> = rows
            .rows::<PlayerItem>()?
            .map(|row| row.map_err(|e| anyhow::anyhow!("Failed to parse PlayerItem: {}", e)))
            .collect::<std::result::Result<_, _>>()?;

        Ok(items)
    }

    pub async fn activate_item(&self, user_id: i64, item_id: i32, is_active: bool) -> Result<()> {
        self.db
            .session
            .execute_unpaged(&self.activate_item_stmt, (is_active, user_id, item_id))
            .await?;

        Ok(())
    }
}
