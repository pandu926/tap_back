// use crate::database::Database;
// use crate::models::player::*;
// use crate::repositories::{
//     LeaderboardRepository, PlayerRepository, ReferralRepository, ShopRepository, TaskRepository,
// };
// use anyhow::Result;
// use tracing::info;
// use uuid::Uuid;

// #[derive(Clone)]
// pub struct GameService {
//     pub player_repo: PlayerRepository,
//     pub task_repo: TaskRepository,
//     pub shop_repo: ShopRepository,
//     pub referral_repo: ReferralRepository,
//     pub leaderboard_repo: LeaderboardRepository,
// }

// impl GameService {
//     pub async fn new(db: Database) -> Result<Self> {
//         info!("🔧 Initializing player_repo...");
//         let player_repo = PlayerRepository::new(db.clone())
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to init player_repo: {}", e))?;

//         info!("🔧 Initializing task_repo...");
//         let task_repo = TaskRepository::new(db.clone())
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to init task_repo: {}", e))?;

//         info!("🔧 Initializing shop_repo...");
//         let shop_repo = ShopRepository::new(db.clone())
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to init shop_repo: {}", e))?;

//         info!("🔧 Initializing referral_repo...");
//         let referral_repo = ReferralRepository::new(db.clone())
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to init referral_repo: {}", e))?;

//         info!("🔧 Initializing leaderboard_repo...");
//         let leaderboard_repo = LeaderboardRepository::new(db)
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to init leaderboard_repo: {}", e))?;

//         info!("✅ All repositories initialized");

//         Ok(Self {
//             player_repo,
//             task_repo,
//             shop_repo,
//             referral_repo,
//             leaderboard_repo,
//         })
//     }

//     // Player operations
//     pub async fn create_player(&self, request: CreatePlayerRequest) -> Result<()> {
//         // Create player
//         self.player_repo.create(request.clone()).await?;

//         // Handle referral if exists
//         if let Some(referred_by_id) = request.referred_by_id {
//             self.referral_repo
//                 .create_referral(referred_by_id, request.user_id)
//                 .await?;

//             // Award referral bonus to referrer
//             self.player_repo.update_score(referred_by_id, 1000).await?;
//             self.leaderboard_repo
//                 .update_score(referred_by_id, None, 1000)
//                 .await?;
//         }

//         Ok(())
//     }

//     pub async fn get_player(&self, user_id: i64) -> Result<Option<Player>> {
//         self.player_repo.get_user_by_id(user_id).await
//     }

//     pub async fn update_player(&self, request: UpdatePlayerRequest) -> Result<()> {
//         self.player_repo.update(request).await
//     }

//     pub async fn add_score(
//         &self,
//         user_id: i64,
//         score: i64,
//         username: Option<String>,
//     ) -> Result<()> {
//         self.player_repo.update_score(user_id, score).await?;
//         self.leaderboard_repo
//             .update_score(user_id, username, score)
//             .await?;
//         Ok(())
//     }

//     // Task operations
//     pub async fn create_task(&self, request: CreateTaskRequest) -> Result<Uuid> {
//         self.task_repo.create_task(request).await
//     }

//     pub async fn get_daily_tasks(&self) -> Result<Vec<Task>> {
//         self.task_repo.get_tasks_by_type("daily").await
//     }

//     pub async fn get_weekly_tasks(&self) -> Result<Vec<Task>> {
//         self.task_repo.get_tasks_by_type("weekly").await
//     }

//     pub async fn update_task_progress(&self, request: UpdateTaskProgressRequest) -> Result<()> {
//         // Step 1: Update progress
//         self.task_repo.update_task_progress(request.clone()).await?;

//         // Step 2: Coba ambil task dari "daily"
//         let mut task = self.task_repo.get_task("daily", request.task_id).await?;

//         // Step 3: Jika tidak ditemukan di "daily", coba "weekly"
//         if task.is_none() {
//             task = self.task_repo.get_task("weekly", request.task_id).await?;
//         }

//         // Step 4: Jika task ditemukan dan sudah mencapai target, tandai sebagai selesai
//         if let Some(task) = task {
//             if let Some(target) = task.completion_target {
//                 if request.progress >= target {
//                     self.task_repo
//                         .complete_task(request.user_id, request.task_id)
//                         .await?;
//                 }
//             }
//         }

//         Ok(())
//     }

//     pub async fn claim_task_reward(&self, user_id: i64, task_id: Uuid) -> Result<i64> {
//         // Coba ambil task dari "daily"
//         let mut task = self.task_repo.get_task("daily", task_id).await?;

//         // Jika tidak ditemukan, coba ambil dari "weekly"
//         if task.is_none() {
//             task = self.task_repo.get_task("weekly", task_id).await?;
//         }

//         // Kalau tetap tidak ditemukan, return error
//         let task = task.ok_or_else(|| anyhow::anyhow!("Task not found"))?;

//         // Ambil progress user terhadap task tsb
//         let player_task = self
//             .task_repo
//             .get_player_task(user_id, task_id)
//             .await?
//             .ok_or_else(|| anyhow::anyhow!("Player task not found"))?;

//         // Cek apakah sudah diselesaikan
//         if player_task.completed_at.is_none() {
//             return Err(anyhow::anyhow!("Task not completed"));
//         }

//         // Cek apakah sudah diklaim
//         if player_task.claimed_at.is_some() {
//             return Err(anyhow::anyhow!("Task already claimed"));
//         }

//         // Klaim reward
//         let reward = task.reward_score.unwrap_or(0);
//         self.task_repo.claim_task_reward(user_id, task_id).await?;

//         // Tambahkan skor ke player
//         self.add_score(user_id, reward, None).await?;

//         Ok(reward)
//     }

//     pub async fn get_player_tasks(&self, user_id: i64) -> Result<Vec<TaskResponse>> {
//         let player_tasks = self.task_repo.get_player_tasks(user_id).await?;
//         let mut task_responses = Vec::new();

//         for player_task in player_tasks {
//             // Ambil task dari daily dulu
//             let mut task_opt = self
//                 .task_repo
//                 .get_task("daily", player_task.task_id)
//                 .await?;

//             // Kalau tidak ketemu, coba dari weekly
//             if task_opt.is_none() {
//                 task_opt = self
//                     .task_repo
//                     .get_task("weekly", player_task.task_id)
//                     .await?;
//             }

//             if let Some(task) = task_opt {
//                 task_responses.push(TaskResponse {
//                     task_id: task.task_id,
//                     task_type: task.task_type,
//                     title: task.title.unwrap_or_default(),
//                     description: task.description,
//                     completion_target: task.completion_target.unwrap_or(0),
//                     reward_score: task.reward_score.unwrap_or(0),
//                     progress: player_task.progress,
//                     completed: player_task.completed_at.is_some(),
//                     claimed: player_task.claimed_at.is_some(),
//                 });
//             }
//         }

//         Ok(task_responses)
//     }

//     // Shop operations
//     pub async fn create_shop_item(&self, request: CreateShopItemRequest) -> Result<i32> {
//         self.shop_repo.create_shop_item(request).await
//     }

//     pub async fn get_shop_items(&self, category: &str) -> Result<Vec<ShopItem>> {
//         self.shop_repo.get_shop_items_by_category(category).await
//     }

//     pub async fn purchase_item(&self, request: PurchaseItemRequest) -> Result<()> {
//         let item = self
//             .shop_repo
//             .get_shop_item("", request.item_id)
//             .await?
//             .ok_or_else(|| anyhow::anyhow!("Item not found"))?;

//         let total_cost = item.price.unwrap_or(0) * request.quantity as i64;

//         let player = self
//             .get_player(request.user_id)
//             .await?
//             .ok_or_else(|| anyhow::anyhow!("Player not found"))?;

//         // Panggilan pakai borrow
//         self.shop_repo.purchase_item(&request).await?;

//         self.add_score(request.user_id, -total_cost, None).await?;

//         Ok(())
//     }

//     pub async fn get_player_items(&self, user_id: i64) -> Result<Vec<PlayerItem>> {
//         self.shop_repo.get_player_items(user_id).await
//     }

//     pub async fn activate_item(&self, user_id: i64, item_id: i32) -> Result<()> {
//         self.shop_repo.activate_item(user_id, item_id, true).await
//     }

//     // Referral operations
//     pub async fn get_referrals(&self, referrer_id: i64) -> Result<Vec<Referral>> {
//         self.referral_repo
//             .get_referrals_by_referrer(referrer_id)
//             .await
//     }

//     pub async fn get_referral_count(&self, referrer_id: i64) -> Result<i64> {
//         self.referral_repo.count_referrals(referrer_id).await
//     }

//     // Leaderboard operations
//     pub async fn get_leaderboard(&self, limit: i32) -> Result<Vec<LeaderboardResponse>> {
//         self.leaderboard_repo.get_leaderboard(limit).await
//     }

//     pub async fn get_player_rank(&self, user_id: i64) -> Result<Option<i32>> {
//         self.leaderboard_repo.get_player_rank(user_id).await
//     }

//     // Energy operations
//     pub async fn recharge_energy(&self, user_id: i64) -> Result<i32> {
//         // Implementation depends on energy recharge logic
//         // This is a simplified version
//         let player = self
//             .get_player(user_id)
//             .await?
//             .ok_or_else(|| anyhow::anyhow!("Player not found"))?;

//         let max_energy = player.max_energy.unwrap_or(1000);

//         self.player_repo
//             .update(UpdatePlayerRequest {
//                 user_id,
//                 username: None,
//                 first_name: None,
//                 level: None,
//                 energy: Some(max_energy),
//                 max_energy: None,
//                 tap_value: None,
//                 energy_recharge_rate: None,
//             })
//             .await?;

//         Ok(max_energy)
//     }

//     pub async fn consume_energy(&self, user_id: i64, amount: i32) -> Result<i32> {
//         let player = self
//             .get_player(user_id)
//             .await?
//             .ok_or_else(|| anyhow::anyhow!("Player not found"))?;

//         let current_energy = player.energy.unwrap_or(0);
//         let new_energy = (current_energy - amount).max(0);

//         self.player_repo
//             .update(UpdatePlayerRequest {
//                 user_id,
//                 username: None,
//                 first_name: None,
//                 level: None,
//                 energy: Some(new_energy),
//                 max_energy: None,
//                 tap_value: None,
//                 energy_recharge_rate: None,
//             })
//             .await?;

//         Ok(new_energy)
//     }
// }
