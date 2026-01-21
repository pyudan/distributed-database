//! PyuSQL Parser
//!
//! Parses tokens into an Abstract Syntax Tree

use super::ast::*;
use super::lexer::{Lexer, Token, TokenKind};
use crate::{Error, Result};

/// PyuSQL Parser
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    /// Create a new parser from a query string
    pub fn new(query: &str) -> Result<Self> {
        let mut lexer = Lexer::new(query);
        let tokens = lexer.tokenize()?;
        Ok(Self { tokens, pos: 0 })
    }

    /// Get the current token
    fn current(&self) -> &Token {
        &self.tokens[self.pos]
    }

    /// Peek at the next token
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos + 1)
    }

    /// Advance to the next token
    fn advance(&mut self) {
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
    }

    /// Check if current token matches
    fn check(&self, kind: &TokenKind) -> bool {
        std::mem::discriminant(&self.current().kind) == std::mem::discriminant(kind)
    }

    /// Expect and consume a specific token
    fn expect(&mut self, kind: TokenKind) -> Result<()> {
        if self.check(&kind) {
            self.advance();
            Ok(())
        } else {
            Err(Error::Parse(format!(
                "Expected {:?}, got {:?} at line {}, column {}",
                kind,
                self.current().kind,
                self.current().line,
                self.current().column
            )))
        }
    }

    /// Try to match and consume a token
    fn match_token(&mut self, kind: &TokenKind) -> bool {
        if self.check(kind) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Parse a statement
    pub fn parse(&mut self) -> Result<Statement> {
        let stmt = match &self.current().kind {
            TokenKind::Select => self.parse_select(),
            TokenKind::Insert => self.parse_insert(),
            TokenKind::Update => self.parse_update(),
            TokenKind::Delete => self.parse_delete(),
            TokenKind::Create => self.parse_create(),
            TokenKind::Drop => self.parse_drop(),
            TokenKind::Begin => {
                self.advance();
                Ok(Statement::Begin)
            }
            TokenKind::Commit => {
                self.advance();
                Ok(Statement::Commit)
            }
            TokenKind::Rollback => {
                self.advance();
                Ok(Statement::Rollback)
            }
            _ => Err(Error::Parse(format!(
                "Unexpected token: {:?}",
                self.current().kind
            ))),
        }?;

        // Optional semicolon
        self.match_token(&TokenKind::Semicolon);

        Ok(stmt)
    }

    /// Parse SELECT statement
    fn parse_select(&mut self) -> Result<Statement> {
        self.expect(TokenKind::Select)?;

        // Parse columns
        let columns = self.parse_select_columns()?;

        // FROM clause
        self.expect(TokenKind::From)?;
        let from = self.parse_table_ref()?;

        // WHERE clause (optional)
        let where_clause = if self.match_token(&TokenKind::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // ORDER BY clause (optional)
        let order_by = if self.match_token(&TokenKind::Order) {
            self.expect(TokenKind::By)?;
            self.parse_order_by()?
        } else {
            Vec::new()
        };

        // LIMIT clause (optional)
        let limit = if self.match_token(&TokenKind::Limit) {
            match &self.current().kind {
                TokenKind::IntegerLiteral(n) => {
                    let limit = *n as u64;
                    self.advance();
                    Some(limit)
                }
                _ => return Err(Error::Parse("Expected integer after LIMIT".into())),
            }
        } else {
            None
        };

        // OFFSET clause (optional)
        let offset = if self.match_token(&TokenKind::Offset) {
            match &self.current().kind {
                TokenKind::IntegerLiteral(n) => {
                    let offset = *n as u64;
                    self.advance();
                    Some(offset)
                }
                _ => return Err(Error::Parse("Expected integer after OFFSET".into())),
            }
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement {
            columns,
            from,
            where_clause,
            order_by,
            limit,
            offset,
        }))
    }

    /// Parse SELECT columns
    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        let mut columns = Vec::new();

        loop {
            if self.match_token(&TokenKind::Star) {
                columns.push(SelectColumn::All);
            } else {
                let expr = self.parse_expression()?;
                let alias = if self.match_token(&TokenKind::As) {
                    match &self.current().kind {
                        TokenKind::Identifier(name) => {
                            let alias = name.clone();
                            self.advance();
                            Some(alias)
                        }
                        _ => return Err(Error::Parse("Expected identifier after AS".into())),
                    }
                } else {
                    None
                };
                columns.push(SelectColumn::Expr { expr, alias });
            }

            if !self.match_token(&TokenKind::Comma) {
                break;
            }
        }

        Ok(columns)
    }

    /// Parse table reference
    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let name = match &self.current().kind {
            TokenKind::Identifier(name) => name.clone(),
            _ => return Err(Error::Parse("Expected table name".into())),
        };
        self.advance();

        let alias = if self.match_token(&TokenKind::As) {
            match &self.current().kind {
                TokenKind::Identifier(alias) => {
                    let a = alias.clone();
                    self.advance();
                    Some(a)
                }
                _ => return Err(Error::Parse("Expected alias after AS".into())),
            }
        } else {
            None
        };

        Ok(TableRef { name, alias })
    }

    /// Parse ORDER BY clauses
    fn parse_order_by(&mut self) -> Result<Vec<OrderByClause>> {
        let mut clauses = Vec::new();

        loop {
            let expr = self.parse_expression()?;
            let direction = if self.match_token(&TokenKind::Desc) {
                SortDirection::Desc
            } else {
                self.match_token(&TokenKind::Asc);
                SortDirection::Asc
            };

            clauses.push(OrderByClause { expr, direction });

            if !self.match_token(&TokenKind::Comma) {
                break;
            }
        }

        Ok(clauses)
    }

    /// Parse INSERT statement
    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(TokenKind::Insert)?;
        self.expect(TokenKind::Into)?;

        // Table name
        let table = match &self.current().kind {
            TokenKind::Identifier(name) => name.clone(),
            _ => return Err(Error::Parse("Expected table name".into())),
        };
        self.advance();

        // Column list (optional)
        let columns = if self.match_token(&TokenKind::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect(TokenKind::RParen)?;
            Some(cols)
        } else {
            None
        };

        // VALUES
        self.expect(TokenKind::Values)?;

        // Value rows
        let mut values = Vec::new();
        loop {
            self.expect(TokenKind::LParen)?;
            let row = self.parse_expression_list()?;
            self.expect(TokenKind::RParen)?;
            values.push(row);

            if !self.match_token(&TokenKind::Comma) {
                break;
            }
        }

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
        }))
    }

    /// Parse UPDATE statement
    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(TokenKind::Update)?;

        // Table name
        let table = match &self.current().kind {
            TokenKind::Identifier(name) => name.clone(),
            _ => return Err(Error::Parse("Expected table name".into())),
        };
        self.advance();

        // SET
        self.expect(TokenKind::Set)?;

        // Assignments
        let mut assignments = Vec::new();
        loop {
            let column = match &self.current().kind {
                TokenKind::Identifier(name) => name.clone(),
                _ => return Err(Error::Parse("Expected column name".into())),
            };
            self.advance();

            self.expect(TokenKind::Eq)?;

            let value = self.parse_expression()?;

            assignments.push(Assignment { column, value });

            if !self.match_token(&TokenKind::Comma) {
                break;
            }
        }

        // WHERE clause (optional)
        let where_clause = if self.match_token(&TokenKind::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
        }))
    }

    /// Parse DELETE statement
    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(TokenKind::Delete)?;
        self.expect(TokenKind::From)?;

        // Table name
        let table = match &self.current().kind {
            TokenKind::Identifier(name) => name.clone(),
            _ => return Err(Error::Parse("Expected table name".into())),
        };
        self.advance();

        // WHERE clause (optional)
        let where_clause = if self.match_token(&TokenKind::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement {
            table,
            where_clause,
        }))
    }

    /// Parse CREATE statement
    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(TokenKind::Create)?;
        self.expect(TokenKind::Table)?;

        // IF NOT EXISTS
        let if_not_exists = if self.match_token(&TokenKind::If) {
            self.expect(TokenKind::Not)?;
            self.expect(TokenKind::Exists)?;
            true
        } else {
            false
        };

        // Table name
        let name = match &self.current().kind {
            TokenKind::Identifier(name) => name.clone(),
            _ => return Err(Error::Parse("Expected table name".into())),
        };
        self.advance();

        // Column definitions
        self.expect(TokenKind::LParen)?;
        let columns = self.parse_column_defs()?;
        self.expect(TokenKind::RParen)?;

        Ok(Statement::CreateTable(CreateTableStatement {
            name,
            columns,
            if_not_exists,
        }))
    }

    /// Parse DROP statement
    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(TokenKind::Drop)?;
        self.expect(TokenKind::Table)?;

        // IF EXISTS
        let if_exists = if self.match_token(&TokenKind::If) {
            self.expect(TokenKind::Exists)?;
            true
        } else {
            false
        };

        // Table name
        let name = match &self.current().kind {
            TokenKind::Identifier(name) => name.clone(),
            _ => return Err(Error::Parse("Expected table name".into())),
        };
        self.advance();

        Ok(Statement::DropTable(DropTableStatement { name, if_exists }))
    }

    /// Parse column definitions
    fn parse_column_defs(&mut self) -> Result<Vec<ColumnDef>> {
        let mut columns = Vec::new();

        loop {
            let name = match &self.current().kind {
                TokenKind::Identifier(name) => name.clone(),
                _ => return Err(Error::Parse("Expected column name".into())),
            };
            self.advance();

            let data_type = self.parse_data_type()?;

            let mut primary_key = false;
            let mut nullable = true;

            // Parse constraints
            loop {
                if self.match_token(&TokenKind::Primary) {
                    self.expect(TokenKind::Key)?;
                    primary_key = true;
                } else if self.match_token(&TokenKind::Not) {
                    self.expect(TokenKind::Null)?;
                    nullable = false;
                } else {
                    break;
                }
            }

            columns.push(ColumnDef {
                name,
                data_type,
                primary_key,
                nullable,
            });

            if !self.match_token(&TokenKind::Comma) {
                break;
            }
        }

        Ok(columns)
    }

    /// Parse data type
    fn parse_data_type(&mut self) -> Result<DataType> {
        let dt = match &self.current().kind {
            TokenKind::Int | TokenKind::Integer => DataType::Int,
            TokenKind::Float | TokenKind::Double => DataType::Float,
            TokenKind::Text | TokenKind::Varchar => DataType::Text,
            TokenKind::Bool | TokenKind::Boolean => DataType::Bool,
            TokenKind::Blob => DataType::Blob,
            _ => return Err(Error::Parse("Expected data type".into())),
        };
        self.advance();

        // Handle VARCHAR(n) - ignore the length for now
        if self.match_token(&TokenKind::LParen) {
            while !self.check(&TokenKind::RParen) && !self.check(&TokenKind::Eof) {
                self.advance();
            }
            self.expect(TokenKind::RParen)?;
        }

        Ok(dt)
    }

    /// Parse identifier list
    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut identifiers = Vec::new();

        loop {
            match &self.current().kind {
                TokenKind::Identifier(name) => {
                    identifiers.push(name.clone());
                    self.advance();
                }
                _ => return Err(Error::Parse("Expected identifier".into())),
            }

            if !self.match_token(&TokenKind::Comma) {
                break;
            }
        }

        Ok(identifiers)
    }

    /// Parse expression list
    fn parse_expression_list(&mut self) -> Result<Vec<Expression>> {
        let mut exprs = Vec::new();

        loop {
            exprs.push(self.parse_expression()?);

            if !self.match_token(&TokenKind::Comma) {
                break;
            }
        }

        Ok(exprs)
    }

    /// Parse expression (using precedence climbing)
    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_or_expression()
    }

    /// Parse OR expression
    fn parse_or_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_and_expression()?;

        while self.match_token(&TokenKind::Or) {
            let right = self.parse_and_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse AND expression
    fn parse_and_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_comparison()?;

        while self.match_token(&TokenKind::And) {
            let right = self.parse_comparison()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse comparison expression
    fn parse_comparison(&mut self) -> Result<Expression> {
        let left = self.parse_additive()?;

        let op = match &self.current().kind {
            TokenKind::Eq => Some(BinaryOperator::Eq),
            TokenKind::Ne => Some(BinaryOperator::Ne),
            TokenKind::Lt => Some(BinaryOperator::Lt),
            TokenKind::Le => Some(BinaryOperator::Le),
            TokenKind::Gt => Some(BinaryOperator::Gt),
            TokenKind::Ge => Some(BinaryOperator::Ge),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive()?;
            Ok(Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    /// Parse additive expression
    fn parse_additive(&mut self) -> Result<Expression> {
        let mut left = self.parse_multiplicative()?;

        loop {
            let op = match &self.current().kind {
                TokenKind::Plus => Some(BinaryOperator::Add),
                TokenKind::Minus => Some(BinaryOperator::Sub),
                _ => None,
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_multiplicative()?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    /// Parse multiplicative expression
    fn parse_multiplicative(&mut self) -> Result<Expression> {
        let mut left = self.parse_unary()?;

        loop {
            let op = match &self.current().kind {
                TokenKind::Star => Some(BinaryOperator::Mul),
                TokenKind::Slash => Some(BinaryOperator::Div),
                TokenKind::Percent => Some(BinaryOperator::Mod),
                _ => None,
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_unary()?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    /// Parse unary expression
    fn parse_unary(&mut self) -> Result<Expression> {
        if self.match_token(&TokenKind::Not) {
            let expr = self.parse_unary()?;
            return Ok(Expression::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            });
        }

        if self.match_token(&TokenKind::Minus) {
            let expr = self.parse_unary()?;
            return Ok(Expression::UnaryOp {
                op: UnaryOperator::Neg,
                expr: Box::new(expr),
            });
        }

        self.parse_primary()
    }

    /// Parse primary expression
    fn parse_primary(&mut self) -> Result<Expression> {
        match &self.current().kind.clone() {
            TokenKind::IntegerLiteral(n) => {
                let val = *n;
                self.advance();
                Ok(Expression::Literal(Literal::Int(val)))
            }
            TokenKind::FloatLiteral(n) => {
                let val = *n;
                self.advance();
                Ok(Expression::Literal(Literal::Float(val)))
            }
            TokenKind::StringLiteral(s) => {
                let val = s.clone();
                self.advance();
                Ok(Expression::Literal(Literal::String(val)))
            }
            TokenKind::True => {
                self.advance();
                Ok(Expression::Literal(Literal::Bool(true)))
            }
            TokenKind::False => {
                self.advance();
                Ok(Expression::Literal(Literal::Bool(false)))
            }
            TokenKind::Null => {
                self.advance();
                Ok(Expression::Literal(Literal::Null))
            }
            TokenKind::Identifier(name) => {
                let name = name.clone();
                self.advance();

                // Check for table.column or function call
                if self.match_token(&TokenKind::Dot) {
                    match &self.current().kind {
                        TokenKind::Identifier(col) => {
                            let col = col.clone();
                            self.advance();
                            Ok(Expression::Column(ColumnRef {
                                table: Some(name),
                                column: col,
                            }))
                        }
                        _ => Err(Error::Parse("Expected column name after '.'".into())),
                    }
                } else if self.match_token(&TokenKind::LParen) {
                    // Function call
                    let args = if self.check(&TokenKind::RParen) {
                        Vec::new()
                    } else {
                        self.parse_expression_list()?
                    };
                    self.expect(TokenKind::RParen)?;
                    Ok(Expression::Function { name, args })
                } else {
                    Ok(Expression::Column(ColumnRef {
                        table: None,
                        column: name,
                    }))
                }
            }
            TokenKind::LParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(TokenKind::RParen)?;
                Ok(expr)
            }
            _ => Err(Error::Parse(format!(
                "Unexpected token in expression: {:?}",
                self.current().kind
            ))),
        }
    }
}

/// Parse a query string into a statement
pub fn parse(query: &str) -> Result<Statement> {
    let mut parser = Parser::new(query)?;
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let stmt = parse("SELECT * FROM users").unwrap();
        match stmt {
            Statement::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                assert!(matches!(select.columns[0], SelectColumn::All));
                assert_eq!(select.from.name, "users");
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let stmt = parse("SELECT id, name FROM users WHERE age > 18").unwrap();
        match stmt {
            Statement::Select(select) => {
                assert_eq!(select.columns.len(), 2);
                assert!(select.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse("INSERT INTO users (name, age) VALUES ('John', 30)").unwrap();
        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.table, "users");
                assert_eq!(insert.columns, Some(vec!["name".to_string(), "age".to_string()]));
                assert_eq!(insert.values.len(), 1);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse("UPDATE users SET name = 'Jane' WHERE id = 1").unwrap();
        match stmt {
            Statement::Update(update) => {
                assert_eq!(update.table, "users");
                assert_eq!(update.assignments.len(), 1);
                assert!(update.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse("DELETE FROM users WHERE id = 1").unwrap();
        match stmt {
            Statement::Delete(delete) => {
                assert_eq!(delete.table, "users");
                assert!(delete.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.name, "users");
                assert_eq!(create.columns.len(), 2);
                assert!(create.columns[0].primary_key);
                assert!(!create.columns[1].nullable);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_transaction() {
        assert!(matches!(parse("BEGIN").unwrap(), Statement::Begin));
        assert!(matches!(parse("COMMIT").unwrap(), Statement::Commit));
        assert!(matches!(parse("ROLLBACK").unwrap(), Statement::Rollback));
    }
}
