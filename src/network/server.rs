//! PyuDB Server
//!
//! gRPC server for handling client requests and cluster communication

use std::sync::Arc;
use std::net::SocketAddr;

use tokio::sync::RwLock;
use tracing::info;

use crate::storage::StorageEngine;
use crate::query::QueryExecutor;
use crate::txn::TransactionManager;
use crate::{Error, Result};

/// PyuDB server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to listen on
    pub listen_addr: SocketAddr,
    /// Node ID
    pub node_id: String,
    /// Peer addresses
    pub peers: Vec<(String, String)>,
    /// Data directory
    pub data_dir: std::path::PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:5432".parse().unwrap(),
            node_id: "node1".to_string(),
            peers: Vec::new(),
            data_dir: std::path::PathBuf::from("./data"),
        }
    }
}

/// PyuDB Server
pub struct PyuDbServer {
    config: ServerConfig,
    storage: Arc<StorageEngine>,
    txn_manager: Arc<TransactionManager>,
    executor: Arc<RwLock<QueryExecutor>>,
    // raft_handle: Option<RaftHandle>,
}

impl PyuDbServer {
    /// Create a new server
    pub fn new(config: ServerConfig) -> Result<Self> {
        let storage_config = crate::storage::StorageConfig {
            data_dir: config.data_dir.clone(),
            wal_segment_size: 64 * 1024 * 1024,
            sync_on_write: true,
        };
        
        let storage = Arc::new(StorageEngine::new(storage_config)?);
        let txn_manager = Arc::new(TransactionManager::new(Arc::clone(&storage)));
        let executor = Arc::new(RwLock::new(QueryExecutor::with_txn_manager(
            Arc::clone(&storage),
            Arc::clone(&txn_manager),
        )));

        Ok(Self {
            config,
            storage,
            txn_manager,
            executor,
        })
    }

    /// Execute a query
    pub async fn execute_query(&self, query: &str) -> Result<String> {
        let mut executor = self.executor.write().await;
        let result = executor.execute(query)?;
        
        // Format result as JSON
        serde_json::to_string_pretty(&result)
            .map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Get server status
    pub fn status(&self) -> ServerStatus {
        ServerStatus {
            node_id: self.config.node_id.clone(),
            listen_addr: self.config.listen_addr.to_string(),
            is_leader: false, // TODO: Get from Raft
        }
    }

    /// Run the server (placeholder for gRPC server)
    pub async fn run(&self) -> Result<()> {
        info!("PyuDB server starting on {}", self.config.listen_addr);
        info!("Node ID: {}", self.config.node_id);
        
        // In a full implementation, this would start the gRPC server
        // For now, we'll keep it simple
        
        info!("Server ready to accept connections");
        
        Ok(())
    }
}

/// Server status
#[derive(Debug, Clone)]
pub struct ServerStatus {
    pub node_id: String,
    pub listen_addr: String,
    pub is_leader: bool,
}

/// Simple request/response for embedded use
#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub query: String,
    pub transaction_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct QueryResponse {
    pub success: bool,
    pub result: Option<String>,
    pub error: Option<String>,
}

impl PyuDbServer {
    /// Handle a query request (for embedded use)
    pub async fn handle_query(&self, request: QueryRequest) -> QueryResponse {
        match self.execute_query(&request.query).await {
            Ok(result) => QueryResponse {
                success: true,
                result: Some(result),
                error: None,
            },
            Err(e) => QueryResponse {
                success: false,
                result: None,
                error: Some(e.to_string()),
            },
        }
    }
}
