use pest::pratt_parser::Op;

use crate::parser::tokenizer::{self, Token, TokenKind, TokenizationError};

enum Constant {
    String(String),
    Integer(i32),
}

enum Expression {
    Field(String),
    Constant(Constant),
}

struct Term {
    left_expression: Expression,
    right_expression: Expression,
}

struct Predicate {
    term: Term,
    next_predicate: Option<Box<Predicate>>,
}

struct SelectNode {
    fields: Vec<String>,
    tables: Vec<String>,
    predicate: Predicate,
}

enum SQLNode {
    SELECT(SelectNode),
    INSERT,
    DELETE,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParserError {
    TokenizationError(TokenizationError),
    ParseError(ParseError),
    UnexpectedEOL,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseError {
    invalid_token: Token,
    expected_token: Vec<TokenKind>,
    position: usize,
}

impl From<TokenizationError> for ParserError {
    fn from(value: TokenizationError) -> Self {
        ParserError::TokenizationError(value)
    }
}

fn parse(input_text: &str) -> Result<SQLNode, ParserError> {
    let tokens = tokenizer::tokenize(input_text)?;
    let token = tokens.get(0).ok_or(ParserError::UnexpectedEOL)?;
    match token {
        Token::Select => parse_select(tokens[1..].to_vec()),
        _ => Err(ParserError::ParseError(ParseError {
            invalid_token: token.clone(),
            expected_token: vec![
                TokenKind::Select,
                TokenKind::Delete,
                TokenKind::Update,
                TokenKind::Insert,
            ],
            position: 0,
        })),
    }
}

fn parse_select(tokens: Vec<Token>) -> Result<SQLNode, ParserError> {
    let token = tokens.get(0);
    let next_token = tokens.get(1);
}
