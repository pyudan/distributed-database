//! Query Executor
//!
//! Executes parsed PyuSQL statements against the storage engine

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::ast::*;
use super::parser;
use crate::storage::StorageEngine;
use crate::txn::TransactionManager;
use crate::{Error, Result};

/// Result of query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResult {
    /// SELECT result with rows
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    /// INSERT result
    Insert { rows_affected: u64 },
    /// UPDATE result
    Update { rows_affected: u64 },
    /// DELETE result
    Delete { rows_affected: u64 },
    /// CREATE TABLE result
    CreateTable { table_name: String },
    /// DROP TABLE result
    DropTable { table_name: String },
    /// Transaction control result
    Transaction { action: String },
}

/// A value in a row
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
}

impl Value {
    /// Convert from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        // Try to deserialize as JSON, fallback to string
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(bytes) {
            match val {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::Bool(b) => Value::Bool(b),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Value::Int(i)
                    } else if let Some(f) = n.as_f64() {
                        Value::Float(f)
                    } else {
                        Value::String(n.to_string())
                    }
                }
                serde_json::Value::String(s) => Value::String(s),
                _ => Value::String(val.to_string()),
            }
        } else {
            Value::String(String::from_utf8_lossy(bytes).to_string())
        }
    }

    /// Convert to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => b"null".to_vec(),
            Value::Bool(b) => if *b { "true" } else { "false" }.as_bytes().to_vec(),
            Value::Int(i) => i.to_string().into_bytes(),
            Value::Float(f) => f.to_string().into_bytes(),
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Blob(b) => b.clone(),
        }
    }
}

/// Table schema stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

/// Query executor
pub struct QueryExecutor {
    storage: Arc<StorageEngine>,
    txn_manager: Option<Arc<TransactionManager>>,
    /// Active transaction ID for this session
    current_txn: Option<u64>,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            txn_manager: None,
            current_txn: None,
        }
    }

    /// Create a new query executor with transaction manager
    pub fn with_txn_manager(storage: Arc<StorageEngine>, txn_manager: Arc<TransactionManager>) -> Self {
        Self {
            storage,
            txn_manager: Some(txn_manager),
            current_txn: None,
        }
    }

    /// Execute a query string
    pub fn execute(&mut self, query: &str) -> Result<QueryResult> {
        let stmt = parser::parse(query)?;
        self.execute_statement(&stmt)
    }

    /// Execute a parsed statement
    pub fn execute_statement(&mut self, stmt: &Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Select(select) => self.execute_select(select),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Update(update) => self.execute_update(update),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::CreateTable(create) => self.execute_create_table(create),
            Statement::DropTable(drop) => self.execute_drop_table(drop),
            Statement::Begin => self.execute_begin(),
            Statement::Commit => self.execute_commit(),
            Statement::Rollback => self.execute_rollback(),
        }
    }

    /// Execute SELECT
    fn execute_select(&self, select: &SelectStatement) -> Result<QueryResult> {
        let table_name = &select.from.name;
        
        // Get table schema
        let schema = self.get_table_schema(table_name)?;
        
        // Scan all rows for this table
        let prefix = format!("data:{}:", table_name);
        let rows_data = self.storage.scan_prefix(prefix.as_bytes())?;
        
        // Parse rows and apply filter
        let mut result_rows = Vec::new();
        
        for (_key, value) in rows_data {
            let row: HashMap<String, Value> = serde_json::from_slice(&value)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            
            // Apply WHERE filter
            if let Some(ref where_clause) = select.where_clause {
                if !self.evaluate_expression_bool(where_clause, &row)? {
                    continue;
                }
            }
            
            result_rows.push(row);
        }
        
        // Apply ORDER BY
        if !select.order_by.is_empty() {
            result_rows.sort_by(|a, b| {
                for order in &select.order_by {
                    if let Expression::Column(col_ref) = &order.expr {
                        let val_a = a.get(&col_ref.column);
                        let val_b = b.get(&col_ref.column);
                        
                        let cmp = match (val_a, val_b) {
                            (Some(Value::Int(i1)), Some(Value::Int(i2))) => i1.cmp(i2),
                            (Some(Value::String(s1)), Some(Value::String(s2))) => s1.cmp(s2),
                            (Some(Value::Float(f1)), Some(Value::Float(f2))) => {
                                f1.partial_cmp(f2).unwrap_or(std::cmp::Ordering::Equal)
                            }
                            _ => std::cmp::Ordering::Equal,
                        };
                        
                        let cmp = if order.direction == SortDirection::Desc {
                            cmp.reverse()
                        } else {
                            cmp
                        };
                        
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        
        // Apply OFFSET
        if let Some(offset) = select.offset {
            result_rows = result_rows.into_iter().skip(offset as usize).collect();
        }
        
        // Apply LIMIT
        if let Some(limit) = select.limit {
            result_rows.truncate(limit as usize);
        }
        
        // Build column list
        let columns: Vec<String> = match &select.columns[..] {
            [SelectColumn::All] => schema.columns.iter().map(|c| c.name.clone()).collect(),
            cols => cols
                .iter()
                .filter_map(|c| match c {
                    SelectColumn::Expr { expr, alias } => {
                        alias.clone().or_else(|| {
                            if let Expression::Column(col_ref) = expr {
                                Some(col_ref.column.clone())
                            } else {
                                None
                            }
                        })
                    }
                    SelectColumn::All => None,
                })
                .collect(),
        };
        
        // Convert to output format
        let rows: Vec<Vec<Value>> = result_rows
            .iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|col| row.get(col).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();
        
        Ok(QueryResult::Select { columns, rows })
    }

    /// Execute INSERT
    fn execute_insert(&self, insert: &InsertStatement) -> Result<QueryResult> {
        let table_name = &insert.table;
        
        // Get table schema
        let schema = self.get_table_schema(table_name)?;
        
        let column_names: Vec<&str> = insert
            .columns
            .as_ref()
            .map(|c| c.iter().map(|s| s.as_str()).collect())
            .unwrap_or_else(|| schema.columns.iter().map(|c| c.name.as_str()).collect());
        
        let mut rows_affected = 0;
        
        for value_row in &insert.values {
            if value_row.len() != column_names.len() {
                return Err(Error::Parse(format!(
                    "Column count mismatch: expected {}, got {}",
                    column_names.len(),
                    value_row.len()
                )));
            }
            
            // Build row data
            let mut row: HashMap<String, Value> = HashMap::new();
            for (col_name, expr) in column_names.iter().zip(value_row) {
                let value = self.evaluate_expression(expr, &HashMap::new())?;
                row.insert(col_name.to_string(), value);
            }
            
            // Generate row key (use primary key if available, otherwise use timestamp)
            let row_key = self.generate_row_key(table_name, &row, &schema)?;
            
            // Serialize and store
            let row_bytes = serde_json::to_vec(&row)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            
            self.storage.put(row_key.into_bytes(), row_bytes)?;
            rows_affected += 1;
        }
        
        Ok(QueryResult::Insert { rows_affected })
    }

    /// Execute UPDATE
    fn execute_update(&self, update: &UpdateStatement) -> Result<QueryResult> {
        let table_name = &update.table;
        
        // Scan all rows for this table
        let prefix = format!("data:{}:", table_name);
        let rows_data = self.storage.scan_prefix(prefix.as_bytes())?;
        
        let mut rows_affected = 0;
        
        for (key, value) in rows_data {
            let mut row: HashMap<String, Value> = serde_json::from_slice(&value)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            
            // Apply WHERE filter
            if let Some(ref where_clause) = update.where_clause {
                if !self.evaluate_expression_bool(where_clause, &row)? {
                    continue;
                }
            }
            
            // Apply assignments
            for assignment in &update.assignments {
                let new_value = self.evaluate_expression(&assignment.value, &row)?;
                row.insert(assignment.column.clone(), new_value);
            }
            
            // Store updated row
            let row_bytes = serde_json::to_vec(&row)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            
            self.storage.put(key, row_bytes)?;
            rows_affected += 1;
        }
        
        Ok(QueryResult::Update { rows_affected })
    }

    /// Execute DELETE
    fn execute_delete(&self, delete: &DeleteStatement) -> Result<QueryResult> {
        let table_name = &delete.table;
        
        // Scan all rows for this table
        let prefix = format!("data:{}:", table_name);
        let rows_data = self.storage.scan_prefix(prefix.as_bytes())?;
        
        let mut rows_affected = 0;
        let mut keys_to_delete = Vec::new();
        
        for (key, value) in rows_data {
            let row: HashMap<String, Value> = serde_json::from_slice(&value)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            
            // Apply WHERE filter
            if let Some(ref where_clause) = delete.where_clause {
                if !self.evaluate_expression_bool(where_clause, &row)? {
                    continue;
                }
            }
            
            keys_to_delete.push(key);
            rows_affected += 1;
        }
        
        // Delete matching rows
        for key in keys_to_delete {
            self.storage.delete(key)?;
        }
        
        Ok(QueryResult::Delete { rows_affected })
    }

    /// Execute CREATE TABLE
    fn execute_create_table(&self, create: &CreateTableStatement) -> Result<QueryResult> {
        let schema_key = format!("schema:{}", create.name);
        
        // Check if table exists
        if self.storage.get(schema_key.as_bytes())?.is_some() {
            if create.if_not_exists {
                return Ok(QueryResult::CreateTable {
                    table_name: create.name.clone(),
                });
            }
            return Err(Error::TableNotFound(format!("Table {} already exists", create.name)));
        }
        
        // Create schema
        let schema = TableSchema {
            name: create.name.clone(),
            columns: create.columns.clone(),
        };
        
        let schema_bytes = serde_json::to_vec(&schema)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        
        self.storage.put(schema_key.into_bytes(), schema_bytes)?;
        
        Ok(QueryResult::CreateTable {
            table_name: create.name.clone(),
        })
    }

    /// Execute DROP TABLE
    fn execute_drop_table(&self, drop: &DropTableStatement) -> Result<QueryResult> {
        let schema_key = format!("schema:{}", drop.name);
        
        // Check if table exists
        if self.storage.get(schema_key.as_bytes())?.is_none() {
            if drop.if_exists {
                return Ok(QueryResult::DropTable {
                    table_name: drop.name.clone(),
                });
            }
            return Err(Error::TableNotFound(drop.name.clone()));
        }
        
        // Delete schema
        self.storage.delete(schema_key.into_bytes())?;
        
        // Delete all rows
        let prefix = format!("data:{}:", drop.name);
        let rows = self.storage.scan_prefix(prefix.as_bytes())?;
        for (key, _) in rows {
            self.storage.delete(key)?;
        }
        
        Ok(QueryResult::DropTable {
            table_name: drop.name.clone(),
        })
    }

    /// Execute BEGIN transaction
    fn execute_begin(&mut self) -> Result<QueryResult> {
        if let Some(ref txn_manager) = self.txn_manager {
            let txn_id = txn_manager.begin()?;
            self.current_txn = Some(txn_id);
            Ok(QueryResult::Transaction {
                action: format!("BEGIN (txn_id={})", txn_id),
            })
        } else {
            Ok(QueryResult::Transaction {
                action: "BEGIN (no transaction manager)".to_string(),
            })
        }
    }

    /// Execute COMMIT
    fn execute_commit(&mut self) -> Result<QueryResult> {
        if let Some(txn_id) = self.current_txn.take() {
            if let Some(ref txn_manager) = self.txn_manager {
                txn_manager.commit(txn_id)?;
            }
            Ok(QueryResult::Transaction {
                action: format!("COMMIT (txn_id={})", txn_id),
            })
        } else {
            Ok(QueryResult::Transaction {
                action: "COMMIT (no active transaction)".to_string(),
            })
        }
    }

    /// Execute ROLLBACK
    fn execute_rollback(&mut self) -> Result<QueryResult> {
        if let Some(txn_id) = self.current_txn.take() {
            if let Some(ref txn_manager) = self.txn_manager {
                txn_manager.rollback(txn_id)?;
            }
            Ok(QueryResult::Transaction {
                action: format!("ROLLBACK (txn_id={})", txn_id),
            })
        } else {
            Ok(QueryResult::Transaction {
                action: "ROLLBACK (no active transaction)".to_string(),
            })
        }
    }

    /// Get table schema
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema> {
        let schema_key = format!("schema:{}", table_name);
        let schema_bytes = self.storage.get(schema_key.as_bytes())?
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
        serde_json::from_slice(&schema_bytes)
            .map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Generate a row key
    fn generate_row_key(
        &self,
        table_name: &str,
        row: &HashMap<String, Value>,
        schema: &TableSchema,
    ) -> Result<String> {
        // Find primary key column
        let pk_col = schema.columns.iter().find(|c| c.primary_key);
        
        let key_suffix = if let Some(pk) = pk_col {
            if let Some(Value::Int(id)) = row.get(&pk.name) {
                id.to_string()
            } else if let Some(Value::String(id)) = row.get(&pk.name) {
                id.clone()
            } else {
                // Generate timestamp-based key
                self.storage.current_timestamp().to_string()
            }
        } else {
            self.storage.current_timestamp().to_string()
        };
        
        Ok(format!("data:{}:{}", table_name, key_suffix))
    }

    /// Evaluate an expression and return a Value
    fn evaluate_expression(
        &self,
        expr: &Expression,
        row: &HashMap<String, Value>,
    ) -> Result<Value> {
        match expr {
            Expression::Literal(lit) => Ok(match lit {
                Literal::Null => Value::Null,
                Literal::Bool(b) => Value::Bool(*b),
                Literal::Int(i) => Value::Int(*i),
                Literal::Float(f) => Value::Float(*f),
                Literal::String(s) => Value::String(s.clone()),
            }),
            Expression::Column(col_ref) => {
                Ok(row.get(&col_ref.column).cloned().unwrap_or(Value::Null))
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression(left, row)?;
                let right_val = self.evaluate_expression(right, row)?;
                self.evaluate_binary_op(op, &left_val, &right_val)
            }
            Expression::UnaryOp { op, expr } => {
                let val = self.evaluate_expression(expr, row)?;
                match op {
                    UnaryOperator::Not => match val {
                        Value::Bool(b) => Ok(Value::Bool(!b)),
                        _ => Ok(Value::Null),
                    },
                    UnaryOperator::Neg => match val {
                        Value::Int(i) => Ok(Value::Int(-i)),
                        Value::Float(f) => Ok(Value::Float(-f)),
                        _ => Ok(Value::Null),
                    },
                }
            }
            Expression::IsNull { expr, negated } => {
                let val = self.evaluate_expression(expr, row)?;
                let is_null = matches!(val, Value::Null);
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
            Expression::Function { name, args } => {
                // Built-in functions
                match name.to_uppercase().as_str() {
                    "COUNT" => Ok(Value::Int(1)), // Simplified
                    "UPPER" => {
                        if let Some(arg) = args.first() {
                            if let Value::String(s) = self.evaluate_expression(arg, row)? {
                                return Ok(Value::String(s.to_uppercase()));
                            }
                        }
                        Ok(Value::Null)
                    }
                    "LOWER" => {
                        if let Some(arg) = args.first() {
                            if let Value::String(s) = self.evaluate_expression(arg, row)? {
                                return Ok(Value::String(s.to_lowercase()));
                            }
                        }
                        Ok(Value::Null)
                    }
                    _ => Ok(Value::Null),
                }
            }
        }
    }

    /// Evaluate an expression as boolean
    fn evaluate_expression_bool(
        &self,
        expr: &Expression,
        row: &HashMap<String, Value>,
    ) -> Result<bool> {
        match self.evaluate_expression(expr, row)? {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            Value::Int(i) => Ok(i != 0),
            _ => Ok(true),
        }
    }

    /// Evaluate a binary operation
    fn evaluate_binary_op(
        &self,
        op: &BinaryOperator,
        left: &Value,
        right: &Value,
    ) -> Result<Value> {
        match op {
            BinaryOperator::Eq => Ok(Value::Bool(left == right)),
            BinaryOperator::Ne => Ok(Value::Bool(left != right)),
            BinaryOperator::Lt => {
                match (left, right) {
                    (Value::Int(l), Value::Int(r)) => Ok(Value::Bool(l < r)),
                    (Value::Float(l), Value::Float(r)) => Ok(Value::Bool(l < r)),
                    (Value::String(l), Value::String(r)) => Ok(Value::Bool(l < r)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOperator::Le => {
                match (left, right) {
                    (Value::Int(l), Value::Int(r)) => Ok(Value::Bool(l <= r)),
                    (Value::Float(l), Value::Float(r)) => Ok(Value::Bool(l <= r)),
                    (Value::String(l), Value::String(r)) => Ok(Value::Bool(l <= r)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOperator::Gt => {
                match (left, right) {
                    (Value::Int(l), Value::Int(r)) => Ok(Value::Bool(l > r)),
                    (Value::Float(l), Value::Float(r)) => Ok(Value::Bool(l > r)),
                    (Value::String(l), Value::String(r)) => Ok(Value::Bool(l > r)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOperator::Ge => {
                match (left, right) {
                    (Value::Int(l), Value::Int(r)) => Ok(Value::Bool(l >= r)),
                    (Value::Float(l), Value::Float(r)) => Ok(Value::Bool(l >= r)),
                    (Value::String(l), Value::String(r)) => Ok(Value::Bool(l >= r)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOperator::And => {
                match (left, right) {
                    (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l && *r)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOperator::Or => {
                match (left, right) {
                    (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l || *r)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOperator::Add => {
                match (left, right) {
                    (Value::Int(l), Value::Int(r)) => Ok(Value::Int(l + r)),
                    (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l + r)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOperator::Sub => {
                match (left, right) {
                    (Value::Int(l), Value::Int(r)) => Ok(Value::Int(l - r)),
                    (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l - r)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOperator::Mul => {
                match (left, right) {
                    (Value::Int(l), Value::Int(r)) => Ok(Value::Int(l * r)),
                    (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l * r)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOperator::Div => {
                match (left, right) {
                    (Value::Int(l), Value::Int(r)) if *r != 0 => Ok(Value::Int(l / r)),
                    (Value::Float(l), Value::Float(r)) if *r != 0.0 => Ok(Value::Float(l / r)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOperator::Mod => {
                match (left, right) {
                    (Value::Int(l), Value::Int(r)) if *r != 0 => Ok(Value::Int(l % r)),
                    _ => Ok(Value::Null),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageConfig;
    use tempfile::TempDir;

    fn create_test_executor() -> (TempDir, QueryExecutor) {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            wal_segment_size: 1024 * 1024,
            sync_on_write: false,
        };
        let storage = Arc::new(StorageEngine::new(config).unwrap());
        (temp_dir, QueryExecutor::new(storage))
    }

    #[test]
    fn test_create_and_insert() {
        let (_temp, mut executor) = create_test_executor();
        
        // Create table
        let result = executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
        assert!(matches!(result, QueryResult::CreateTable { .. }));
        
        // Insert data
        let result = executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        assert!(matches!(result, QueryResult::Insert { rows_affected: 1 }));
        
        // Select data
        let result = executor.execute("SELECT * FROM users").unwrap();
        match result {
            QueryResult::Select { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected SELECT result"),
        }
    }

    #[test]
    fn test_update() {
        let (_temp, mut executor) = create_test_executor();
        
        executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
        executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        
        let result = executor.execute("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        assert!(matches!(result, QueryResult::Update { rows_affected: 1 }));
    }

    #[test]
    fn test_delete() {
        let (_temp, mut executor) = create_test_executor();
        
        executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
        executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        
        let result = executor.execute("DELETE FROM users WHERE id = 1").unwrap();
        assert!(matches!(result, QueryResult::Delete { rows_affected: 1 }));
    }
}
