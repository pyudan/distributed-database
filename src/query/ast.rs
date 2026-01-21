//! PyuSQL Abstract Syntax Tree
//!
//! Defines the AST nodes for PyuSQL queries

use serde::{Deserialize, Serialize};

/// A complete SQL statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Statement {
    /// SELECT query
    Select(SelectStatement),
    /// INSERT statement
    Insert(InsertStatement),
    /// UPDATE statement
    Update(UpdateStatement),
    /// DELETE statement
    Delete(DeleteStatement),
    /// CREATE TABLE statement
    CreateTable(CreateTableStatement),
    /// DROP TABLE statement
    DropTable(DropTableStatement),
    /// BEGIN transaction
    Begin,
    /// COMMIT transaction
    Commit,
    /// ROLLBACK transaction
    Rollback,
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
    /// Columns to select (* for all)
    pub columns: Vec<SelectColumn>,
    /// Table to select from
    pub from: TableRef,
    /// Optional WHERE clause
    pub where_clause: Option<Expression>,
    /// ORDER BY clause
    pub order_by: Vec<OrderByClause>,
    /// LIMIT clause
    pub limit: Option<u64>,
    /// OFFSET clause
    pub offset: Option<u64>,
}

/// A column in SELECT
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectColumn {
    /// All columns (*)
    All,
    /// A specific expression with optional alias
    Expr {
        expr: Expression,
        alias: Option<String>,
    },
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
    /// Target table
    pub table: String,
    /// Column names (optional)
    pub columns: Option<Vec<String>>,
    /// Values to insert
    pub values: Vec<Vec<Expression>>,
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateStatement {
    /// Target table
    pub table: String,
    /// SET assignments
    pub assignments: Vec<Assignment>,
    /// Optional WHERE clause
    pub where_clause: Option<Expression>,
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteStatement {
    /// Target table
    pub table: String,
    /// Optional WHERE clause
    pub where_clause: Option<Expression>,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    /// Table name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// IF NOT EXISTS flag
    pub if_not_exists: bool,
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTableStatement {
    /// Table name
    pub name: String,
    /// IF EXISTS flag
    pub if_exists: bool,
}

/// Column definition in CREATE TABLE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Is primary key
    pub primary_key: bool,
    /// Is nullable
    pub nullable: bool,
}

/// Data types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Float,
    Text,
    Bool,
    Blob,
}

/// Table reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableRef {
    /// Table name
    pub name: String,
    /// Optional alias
    pub alias: Option<String>,
}

/// Assignment in UPDATE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    /// Column name
    pub column: String,
    /// Value expression
    pub value: Expression,
}

/// ORDER BY clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByClause {
    /// Expression to order by
    pub expr: Expression,
    /// Sort direction
    pub direction: SortDirection,
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl Default for SortDirection {
    fn default() -> Self {
        SortDirection::Asc
    }
}

/// Expression in PyuSQL
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// Column reference
    Column(ColumnRef),
    /// Literal value
    Literal(Literal),
    /// Binary operation
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<Expression>,
        negated: bool,
    },
    /// Function call
    Function {
        name: String,
        args: Vec<Expression>,
    },
}

/// Column reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Optional table name/alias
    pub table: Option<String>,
    /// Column name
    pub column: String,
}

/// Literal values
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOperator {
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Neg,
}

// Helper constructors
impl Expression {
    pub fn column(name: impl Into<String>) -> Self {
        Expression::Column(ColumnRef {
            table: None,
            column: name.into(),
        })
    }

    pub fn int(value: i64) -> Self {
        Expression::Literal(Literal::Int(value))
    }

    pub fn string(value: impl Into<String>) -> Self {
        Expression::Literal(Literal::String(value.into()))
    }

    pub fn bool(value: bool) -> Self {
        Expression::Literal(Literal::Bool(value))
    }

    pub fn null() -> Self {
        Expression::Literal(Literal::Null)
    }

    pub fn eq(self, other: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Eq,
            right: Box::new(other),
        }
    }

    pub fn and(self, other: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::And,
            right: Box::new(other),
        }
    }

    pub fn or(self, other: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Or,
            right: Box::new(other),
        }
    }
}

impl SelectStatement {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            columns: vec![SelectColumn::All],
            from: TableRef {
                name: table.into(),
                alias: None,
            },
            where_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }
}
