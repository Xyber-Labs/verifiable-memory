pub mod app;
pub mod crypto;
pub mod domain;
pub mod infra;
pub mod storage;
pub mod transport;

// Convenience re-exports (keeps call-sites clean)
pub use app::database_service::DatabaseService;
pub use crypto::hashing::{hash_key, hash_value};
pub use domain::commitment::RootManager;
pub use domain::model::{ModelRegistry, ProductModel, UserModel, VerifiableModel, WidgetModel};
pub use infra::solana;
