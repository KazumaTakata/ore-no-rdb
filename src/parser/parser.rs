use pest::pratt_parser::Op;

use crate::parser::tokenizer::{self, Token, TokenKind, TokenizationError};

#[derive(Debug, PartialEq)]
enum Constant {
    String(String),
    Integer(i32),
}

#[derive(Debug, PartialEq)]
enum Expression {
    Field(String),
    Constant(Constant),
}

#[derive(Debug, PartialEq)]
struct Term {
    left_expression: Expression,
    right_expression: Expression,
}

#[derive(Debug, PartialEq)]
struct Predicate {
    term: Term,
    next_predicate: Option<Box<Predicate>>,
}

#[derive(Debug, PartialEq)]
struct SelectNode {
    fields: Vec<String>,
    tables: Vec<String>,
    predicate: Option<Predicate>,
}

#[derive(Debug, PartialEq)]
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
        Token::Select => {
            let parsed_select = parse_select(&tokens[1..])?;
            return Ok(SQLNode::SELECT(parsed_select));
        }
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

fn parse_idents(tokens: &[Token]) -> Result<(Vec<String>, usize), ParserError> {
    let mut index_1: usize = 0;

    let mut fields_vec = vec![];

    loop {
        let token = tokens.get(index_1).ok_or(ParserError::UnexpectedEOL)?;
        let next_token = tokens.get(index_1 + 1);

        match token {
            Token::IDENT(ident) => fields_vec.push(ident.clone()),
            _ => {
                return Err(ParserError::ParseError(ParseError {
                    invalid_token: token.clone(),
                    expected_token: vec![TokenKind::IDENT],
                    position: 1,
                }))
            }
        };

        index_1 = index_1 + 1;

        if let Some(token) = next_token {
            if *token == Token::COMMA {
                index_1 = index_1 + 1
            } else {
                break;
            }
        } else {
            break;
        }
    }

    return Ok((fields_vec, index_1));
}

fn parse_select(tokens: &[Token]) -> Result<SelectNode, ParserError> {
    let (fields_vec, index) = parse_idents(tokens)?;

    let token = tokens.get(index).ok_or(ParserError::UnexpectedEOL)?;

    if *token != Token::From {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.clone(),
            expected_token: vec![TokenKind::From],
            position: index,
        }));
    }

    let (table_vec, mut index_2) = parse_idents(&tokens[index + 1..])?;
    index_2 += 1;

    let where_token = tokens.get(index_2 + 1);

    if let Some(where_token) = where_token {
        if *where_token == Token::Where {
            let predicate = parse_predicate(&tokens[index_2 + 2..])?;

            return Ok(SelectNode {
                fields: fields_vec,
                tables: table_vec,
                predicate: Some(predicate),
            });
        }

        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.clone(),
            expected_token: vec![TokenKind::Where],
            position: index,
        }));
    } else {
        return Ok(SelectNode {
            fields: fields_vec,
            tables: table_vec,
            predicate: None,
        });
    }
}

fn parse_predicate(tokens: &[Token]) -> Result<Predicate, ParserError> {
    let (term, index) = parse_term(tokens)?;
    let next_token = tokens.get(index + 1);

    if let Some(token) = next_token {
        if *token == Token::AND {
            let parsed_predicate = parse_predicate(&tokens[index + 2..])?;
            return Ok(Predicate {
                term,
                next_predicate: Some(Box::new(parsed_predicate)),
            });
        } else {
            return Err(ParserError::ParseError(ParseError {
                invalid_token: token.clone(),
                expected_token: vec![TokenKind::AND],
                position: index,
            }));
        }
    } else {
        return Ok(Predicate {
            term: term,
            next_predicate: None,
        });
    }
}

fn parse_term(tokens: &[Token]) -> Result<(Term, usize), ParserError> {
    let token = tokens.get(0).ok_or(ParserError::UnexpectedEOL)?;
    let left_expression = parse_expression(token)?;

    let next_token = tokens.get(1).ok_or(ParserError::UnexpectedEOL)?;

    if *next_token == Token::Equal {
        let token = tokens.get(2).ok_or(ParserError::UnexpectedEOL)?;

        let right_expression = parse_expression(token)?;
        let term = Term {
            left_expression: left_expression,
            right_expression: right_expression,
        };
        return Ok((term, 2));
    } else {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: next_token.clone(),
            expected_token: vec![TokenKind::AND],
            position: 0,
        }));
    }
}

fn parse_expression(token: &Token) -> Result<Expression, ParserError> {
    match token {
        Token::IDENT(token_value) => Ok(Expression::Field(token_value.to_string())),
        Token::String(token_value) => Ok(Expression::Constant(Constant::String(
            token_value.to_string(),
        ))),
        Token::Number(token_value) => Ok(Expression::Constant(Constant::Integer(*token_value))),
        _ => Err(ParserError::ParseError(ParseError {
            invalid_token: token.clone(),
            expected_token: vec![TokenKind::IDENT, TokenKind::String, TokenKind::Number],
            position: 0,
        })),
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::parser::{
        parse, Constant, Expression, Predicate, SQLNode, SelectNode, Term,
    };

    #[test]
    fn test_get_token() {
        let test_data = "select field from table where a = 1 and b = 3";
        let parsed_select = parse(test_data).expect("");
        assert_eq!(
            parsed_select,
            SQLNode::SELECT(SelectNode {
                fields: vec!["field".to_string()],
                tables: vec!["table".to_string()],
                predicate: Some(Predicate {
                    term: Term {
                        left_expression: Expression::Field("a".to_string()),
                        right_expression: Expression::Constant(Constant::Integer(1))
                    },
                    next_predicate: Some(Box::new(Predicate {
                        term: Term {
                            left_expression: Expression::Field("b".to_string()),
                            right_expression: Expression::Constant(Constant::Integer(3))
                        },
                        next_predicate: None
                    }))
                })
            })
        );
    }
}
