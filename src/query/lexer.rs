//! PyuSQL Lexer
//!
//! Tokenizes PyuSQL queries into a stream of tokens

use std::iter::Peekable;
use std::str::Chars;

use crate::{Error, Result};

/// Token kinds in PyuSQL
#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // Keywords
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Table,
    Into,
    Values,
    From,
    Where,
    And,
    Or,
    Not,
    Set,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    Join,
    Left,
    Right,
    Inner,
    On,
    As,
    Null,
    True,
    False,
    Begin,
    Commit,
    Rollback,
    If,
    Exists,
    Primary,
    Key,
    
    // Data types
    Int,
    Integer,
    Text,
    Varchar,
    Bool,
    Boolean,
    Float,
    Double,
    Blob,
    
    // Identifiers and literals
    Identifier(String),
    StringLiteral(String),
    IntegerLiteral(i64),
    FloatLiteral(f64),
    
    // Operators
    Eq,         // =
    Ne,         // != or <>
    Lt,         // <
    Le,         // <=
    Gt,         // >
    Ge,         // >=
    Plus,       // +
    Minus,      // -
    Star,       // *
    Slash,      // /
    Percent,    // %
    
    // Punctuation
    Comma,      // ,
    Semicolon,  // ;
    LParen,     // (
    RParen,     // )
    Dot,        // .
    
    // Special
    Eof,
}

/// A token with position information
#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenKind,
    pub line: usize,
    pub column: usize,
}

impl Token {
    pub fn new(kind: TokenKind, line: usize, column: usize) -> Self {
        Self { kind, line, column }
    }
}

/// PyuSQL Lexer
pub struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
    line: usize,
    column: usize,
    current_char: Option<char>,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer for the given input
    pub fn new(input: &'a str) -> Self {
        let mut lexer = Self {
            input: input.chars().peekable(),
            line: 1,
            column: 0,
            current_char: None,
        };
        lexer.advance();
        lexer
    }

    /// Advance to the next character
    fn advance(&mut self) -> Option<char> {
        self.current_char = self.input.next();
        self.column += 1;
        if self.current_char == Some('\n') {
            self.line += 1;
            self.column = 0;
        }
        self.current_char
    }

    /// Peek at the next character without consuming
    fn peek(&mut self) -> Option<&char> {
        self.input.peek()
    }

    /// Skip whitespace
    fn skip_whitespace(&mut self) {
        while let Some(c) = self.current_char {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    /// Skip comments
    fn skip_comment(&mut self) {
        // Single-line comment: -- ...
        if self.current_char == Some('-') && self.peek() == Some(&'-') {
            while let Some(c) = self.current_char {
                if c == '\n' {
                    break;
                }
                self.advance();
            }
        }
    }

    /// Read an identifier or keyword
    fn read_identifier(&mut self) -> Token {
        let start_col = self.column;
        let mut ident = String::new();
        
        while let Some(c) = self.current_char {
            if c.is_alphanumeric() || c == '_' {
                ident.push(c);
                self.advance();
            } else {
                break;
            }
        }
        
        // Check for keywords
        let kind = match ident.to_uppercase().as_str() {
            "SELECT" => TokenKind::Select,
            "INSERT" => TokenKind::Insert,
            "UPDATE" => TokenKind::Update,
            "DELETE" => TokenKind::Delete,
            "CREATE" => TokenKind::Create,
            "DROP" => TokenKind::Drop,
            "TABLE" => TokenKind::Table,
            "INTO" => TokenKind::Into,
            "VALUES" => TokenKind::Values,
            "FROM" => TokenKind::From,
            "WHERE" => TokenKind::Where,
            "AND" => TokenKind::And,
            "OR" => TokenKind::Or,
            "NOT" => TokenKind::Not,
            "SET" => TokenKind::Set,
            "ORDER" => TokenKind::Order,
            "BY" => TokenKind::By,
            "ASC" => TokenKind::Asc,
            "DESC" => TokenKind::Desc,
            "LIMIT" => TokenKind::Limit,
            "OFFSET" => TokenKind::Offset,
            "JOIN" => TokenKind::Join,
            "LEFT" => TokenKind::Left,
            "RIGHT" => TokenKind::Right,
            "INNER" => TokenKind::Inner,
            "ON" => TokenKind::On,
            "AS" => TokenKind::As,
            "NULL" => TokenKind::Null,
            "TRUE" => TokenKind::True,
            "FALSE" => TokenKind::False,
            "BEGIN" => TokenKind::Begin,
            "COMMIT" => TokenKind::Commit,
            "ROLLBACK" => TokenKind::Rollback,
            "IF" => TokenKind::If,
            "EXISTS" => TokenKind::Exists,
            "PRIMARY" => TokenKind::Primary,
            "KEY" => TokenKind::Key,
            "INT" => TokenKind::Int,
            "INTEGER" => TokenKind::Integer,
            "TEXT" => TokenKind::Text,
            "VARCHAR" => TokenKind::Varchar,
            "BOOL" => TokenKind::Bool,
            "BOOLEAN" => TokenKind::Boolean,
            "FLOAT" => TokenKind::Float,
            "DOUBLE" => TokenKind::Double,
            "BLOB" => TokenKind::Blob,
            _ => TokenKind::Identifier(ident),
        };
        
        Token::new(kind, self.line, start_col)
    }

    /// Read a number literal
    fn read_number(&mut self) -> Result<Token> {
        let start_col = self.column;
        let mut num_str = String::new();
        let mut is_float = false;
        
        while let Some(c) = self.current_char {
            if c.is_ascii_digit() {
                num_str.push(c);
                self.advance();
            } else if c == '.' && !is_float {
                is_float = true;
                num_str.push(c);
                self.advance();
            } else {
                break;
            }
        }
        
        if is_float {
            let value: f64 = num_str.parse()
                .map_err(|_| Error::Parse(format!("Invalid float: {}", num_str)))?;
            Ok(Token::new(TokenKind::FloatLiteral(value), self.line, start_col))
        } else {
            let value: i64 = num_str.parse()
                .map_err(|_| Error::Parse(format!("Invalid integer: {}", num_str)))?;
            Ok(Token::new(TokenKind::IntegerLiteral(value), self.line, start_col))
        }
    }

    /// Read a string literal
    fn read_string(&mut self) -> Result<Token> {
        let start_col = self.column;
        let quote_char = self.current_char.unwrap();
        self.advance(); // Skip opening quote
        
        let mut string = String::new();
        
        loop {
            match self.current_char {
                Some(c) if c == quote_char => {
                    self.advance(); // Skip closing quote
                    break;
                }
                Some('\\') => {
                    self.advance();
                    match self.current_char {
                        Some('n') => string.push('\n'),
                        Some('t') => string.push('\t'),
                        Some('r') => string.push('\r'),
                        Some('\\') => string.push('\\'),
                        Some(c) if c == quote_char => string.push(c),
                        _ => return Err(Error::Parse("Invalid escape sequence".into())),
                    }
                    self.advance();
                }
                Some(c) => {
                    string.push(c);
                    self.advance();
                }
                None => return Err(Error::Parse("Unterminated string".into())),
            }
        }
        
        Ok(Token::new(TokenKind::StringLiteral(string), self.line, start_col))
    }

    /// Get the next token
    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace();
        self.skip_comment();
        self.skip_whitespace();
        
        let line = self.line;
        let column = self.column;
        
        match self.current_char {
            None => Ok(Token::new(TokenKind::Eof, line, column)),
            
            Some(c) if c.is_alphabetic() || c == '_' => Ok(self.read_identifier()),
            Some(c) if c.is_ascii_digit() => self.read_number(),
            Some('\'' | '"') => self.read_string(),
            
            Some('=') => {
                self.advance();
                Ok(Token::new(TokenKind::Eq, line, column))
            }
            Some('!') => {
                self.advance();
                if self.current_char == Some('=') {
                    self.advance();
                    Ok(Token::new(TokenKind::Ne, line, column))
                } else {
                    Err(Error::Parse(format!("Unexpected character: !")))
                }
            }
            Some('<') => {
                self.advance();
                match self.current_char {
                    Some('=') => {
                        self.advance();
                        Ok(Token::new(TokenKind::Le, line, column))
                    }
                    Some('>') => {
                        self.advance();
                        Ok(Token::new(TokenKind::Ne, line, column))
                    }
                    _ => Ok(Token::new(TokenKind::Lt, line, column)),
                }
            }
            Some('>') => {
                self.advance();
                if self.current_char == Some('=') {
                    self.advance();
                    Ok(Token::new(TokenKind::Ge, line, column))
                } else {
                    Ok(Token::new(TokenKind::Gt, line, column))
                }
            }
            Some('+') => {
                self.advance();
                Ok(Token::new(TokenKind::Plus, line, column))
            }
            Some('-') => {
                self.advance();
                Ok(Token::new(TokenKind::Minus, line, column))
            }
            Some('*') => {
                self.advance();
                Ok(Token::new(TokenKind::Star, line, column))
            }
            Some('/') => {
                self.advance();
                Ok(Token::new(TokenKind::Slash, line, column))
            }
            Some('%') => {
                self.advance();
                Ok(Token::new(TokenKind::Percent, line, column))
            }
            Some(',') => {
                self.advance();
                Ok(Token::new(TokenKind::Comma, line, column))
            }
            Some(';') => {
                self.advance();
                Ok(Token::new(TokenKind::Semicolon, line, column))
            }
            Some('(') => {
                self.advance();
                Ok(Token::new(TokenKind::LParen, line, column))
            }
            Some(')') => {
                self.advance();
                Ok(Token::new(TokenKind::RParen, line, column))
            }
            Some('.') => {
                self.advance();
                Ok(Token::new(TokenKind::Dot, line, column))
            }
            
            Some(c) => Err(Error::Parse(format!("Unexpected character: {}", c))),
        }
    }

    /// Tokenize the entire input
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        
        loop {
            let token = self.next_token()?;
            let is_eof = token.kind == TokenKind::Eof;
            tokens.push(token);
            if is_eof {
                break;
            }
        }
        
        Ok(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_query() {
        let mut lexer = Lexer::new("SELECT * FROM users WHERE id = 1");
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::Star);
        assert_eq!(tokens[2].kind, TokenKind::From);
        assert!(matches!(tokens[3].kind, TokenKind::Identifier(_)));
        assert_eq!(tokens[4].kind, TokenKind::Where);
        assert!(matches!(tokens[5].kind, TokenKind::Identifier(_)));
        assert_eq!(tokens[6].kind, TokenKind::Eq);
        assert_eq!(tokens[7].kind, TokenKind::IntegerLiteral(1));
    }

    #[test]
    fn test_insert_query() {
        let mut lexer = Lexer::new("INSERT INTO users (name, age) VALUES ('John', 30)");
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens[0].kind, TokenKind::Insert);
        assert_eq!(tokens[1].kind, TokenKind::Into);
        assert!(matches!(tokens[2].kind, TokenKind::Identifier(_)));
    }

    #[test]
    fn test_string_literals() {
        let mut lexer = Lexer::new("'hello world' \"another string\"");
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens[0].kind, TokenKind::StringLiteral("hello world".to_string()));
        assert_eq!(tokens[1].kind, TokenKind::StringLiteral("another string".to_string()));
    }

    #[test]
    fn test_comparison_operators() {
        let mut lexer = Lexer::new("< <= > >= = != <>");
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens[0].kind, TokenKind::Lt);
        assert_eq!(tokens[1].kind, TokenKind::Le);
        assert_eq!(tokens[2].kind, TokenKind::Gt);
        assert_eq!(tokens[3].kind, TokenKind::Ge);
        assert_eq!(tokens[4].kind, TokenKind::Eq);
        assert_eq!(tokens[5].kind, TokenKind::Ne);
        assert_eq!(tokens[6].kind, TokenKind::Ne);
    }
}
