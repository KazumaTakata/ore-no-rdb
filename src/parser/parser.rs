use std::fmt;
use std::path::Display;

use pest::pratt_parser::Op;

use crate::parser::tokenizer::{self, Token, TokenKind, TokenWithPos, TokenizationError};

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
struct InsertNode {
    fields: Vec<String>,
    table: String,
    constants: Vec<Constant>,
}

#[derive(Debug, PartialEq)]
struct DeleteNode {
    table: String,
    predicate: Predicate,
}

#[derive(Debug, PartialEq)]
struct UpdateNode {
    table: String,
    field: String,
    value: Expression,
    predicate: Option<Predicate>,
}

#[derive(Debug, PartialEq)]
struct FieldDefNode {
    field_type: FieldType,
    field_length: usize,
    field_name: String,
}

#[derive(Debug, PartialEq)]
struct CreateTableNode {
    table_name: String,
    field_defs: Vec<FieldDefNode>,
}

#[derive(Debug, PartialEq)]
struct CreateIndexNode {
    table_name: String,
    field_name: String,
    index_name: String,
}

#[derive(Debug, PartialEq)]
struct CreateViewNode {
    view_name: String,
    query: SelectNode,
}

#[derive(Debug, PartialEq)]
enum FieldType {
    INTEGER,
    VARCHAR,
}

#[derive(Debug, PartialEq)]
enum SQLNode {
    SELECT(SelectNode),
    INSERT(InsertNode),
    DELETE(DeleteNode),
    UPDATE(UpdateNode),
    CreateTable(CreateTableNode),
    CreateIndexNode(CreateIndexNode),
    CreateViewNode(CreateViewNode),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParserError {
    TokenizationError(TokenizationError),
    ParseError(ParseError),
    UnexpectedEOL,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ErrorResport<'a> {
    err: &'a ParserError,
    src: &'a str,
}

impl fmt::Display for ErrorResport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.err {
            ParserError::ParseError(x) => {
                writeln!(f, "error: expected {:?}", x.expected_token);
                writeln!(f, "{}", self.src);
                write!(f, "{:w$}^", "", w = x.position);
                return Ok(());
            }
            _ => write!(f, ""),
        }
    }
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
    match token.token {
        Token::Select => {
            let parsed_select = parse_select(&tokens[1..])?;
            return Ok(SQLNode::SELECT(parsed_select));
        }
        Token::Insert => {
            let parsed_insert = parse_insert(&tokens[1..])?;
            return Ok(SQLNode::INSERT(parsed_insert));
        }
        Token::Delete => {
            let parsed_delete = parse_delete(&tokens[1..])?;
            return Ok(SQLNode::DELETE(parsed_delete));
        }
        Token::Update => {
            let parsed_delete = parse_update(&tokens[1..])?;
            return Ok(SQLNode::UPDATE(parsed_delete));
        }

        Token::Create => {
            let token = tokens.get(1).ok_or(ParserError::UnexpectedEOL)?;
            match token.token {
                Token::Table => {
                    let parsed_create_table = parse_create_table(&tokens[2..])?;
                    return Ok(SQLNode::CreateTable(parsed_create_table));
                }
                Token::Index => {
                    let parsed_index = parse_create_index(&tokens[2..])?;
                    return Ok(SQLNode::CreateIndexNode(parsed_index));
                }
                Token::View => {
                    let parsed_delete = parse_create_view(&tokens[2..])?;
                    return Ok(SQLNode::CreateViewNode(parsed_delete));
                }
                _ => Err(ParserError::ParseError(ParseError {
                    invalid_token: token.token.clone(),
                    expected_token: vec![TokenKind::Table, TokenKind::Index, TokenKind::View],
                    position: token.pos,
                })),
            }
        }

        _ => Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![
                TokenKind::Select,
                TokenKind::Delete,
                TokenKind::Update,
                TokenKind::Insert,
            ],
            position: token.pos,
        })),
    }
}

fn parse_idents(tokens: &[TokenWithPos]) -> Result<(Vec<String>, usize), ParserError> {
    let mut index_1: usize = 0;

    let mut fields_vec = vec![];

    loop {
        let token = tokens.get(index_1).ok_or(ParserError::UnexpectedEOL)?;
        let token = &token.token;

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
            let token = &token.token;
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

fn parse_constants(tokens: &[TokenWithPos]) -> Result<(Vec<Constant>, usize), ParserError> {
    let mut index: usize = 0;

    let mut constants = vec![];

    loop {
        let token = tokens.get(index).ok_or(ParserError::UnexpectedEOL)?;

        let next_token = tokens.get(index + 1);

        let constant = match token.token.clone() {
            Token::Number(ident) => Constant::Integer(ident),
            Token::String(ident) => Constant::String(ident.to_string()),
            _ => {
                return Err(ParserError::ParseError(ParseError {
                    invalid_token: token.token.clone(),
                    expected_token: vec![TokenKind::IDENT],
                    position: token.pos,
                }))
            }
        };

        constants.push(constant);

        index = index + 1;

        if let Some(token) = next_token {
            let token = &token.token;
            if *token == Token::COMMA {
                index = index + 1
            } else {
                break;
            }
        } else {
            break;
        }
    }

    return Ok((constants, index));
}

fn parse_select(tokens: &[TokenWithPos]) -> Result<SelectNode, ParserError> {
    let (fields_vec, index) = parse_idents(tokens)?;

    let token = tokens.get(index).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::From {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::From],
            position: token.pos,
        }));
    }

    let (table_vec, mut index_2) = parse_idents(&tokens[index + 1..])?;
    index_2 += index;

    let where_token = tokens.get(index_2 + 1);

    if let Some(where_token) = where_token {
        let where_token = &where_token.token;

        if *where_token == Token::Where {
            let predicate = parse_predicate(&tokens[index_2 + 2..])?;

            return Ok(SelectNode {
                fields: fields_vec,
                tables: table_vec,
                predicate: Some(predicate),
            });
        }

        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::Where],
            position: token.pos,
        }));
    } else {
        return Ok(SelectNode {
            fields: fields_vec,
            tables: table_vec,
            predicate: None,
        });
    }
}

fn parse_insert(tokens: &[TokenWithPos]) -> Result<InsertNode, ParserError> {
    let token = tokens.get(0).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::Into {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::Insert],
            position: token.pos,
        }));
    }

    let token = tokens.get(1).ok_or(ParserError::UnexpectedEOL)?;

    let table_name = if let Token::IDENT(ident_name) = token.token.clone() {
        Ok(ident_name.clone())
    } else {
        Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::IDENT],
            position: token.pos,
        }))
    }?;

    let (fields_vec, index) = parse_idents(&tokens[2..])?;

    let token = tokens.get(index + 2).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::Values {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::Values],
            position: token.pos,
        }));
    }

    let (constants_vec, index) = parse_constants(&tokens[index + 3..])?;

    return Ok(InsertNode {
        constants: constants_vec,
        table: table_name,
        fields: fields_vec,
    });
}

fn parse_delete(tokens: &[TokenWithPos]) -> Result<DeleteNode, ParserError> {
    let token = tokens.get(0).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::From {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::From],
            position: token.pos,
        }));
    }

    let token = tokens.get(1).ok_or(ParserError::UnexpectedEOL)?;

    let table_name = if let Token::IDENT(ident_name) = token.token.clone() {
        Ok(ident_name.clone())
    } else {
        Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::IDENT],
            position: token.pos,
        }))
    }?;

    let predicate = parse_predicate(&tokens[2..])?;

    return Ok(DeleteNode {
        predicate: predicate,
        table: table_name,
    });
}

fn parse_update(tokens: &[TokenWithPos]) -> Result<UpdateNode, ParserError> {
    let token = tokens.get(0).ok_or(ParserError::UnexpectedEOL)?;

    let table_name = if let Token::IDENT(ident_name) = token.token.clone() {
        Ok(ident_name.clone())
    } else {
        Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::IDENT],
            position: token.pos,
        }))
    }?;

    let token = tokens.get(1).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::Set {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::Set],
            position: token.pos,
        }));
    }

    let token = tokens.get(2).ok_or(ParserError::UnexpectedEOL)?;

    let field_name = if let Token::IDENT(ident_name) = token.token.clone() {
        Ok(ident_name.clone())
    } else {
        Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::IDENT],
            position: token.pos,
        }))
    }?;

    let token = tokens.get(3).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::Equal {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::Equal],
            position: token.pos,
        }));
    }

    let token = tokens.get(4).ok_or(ParserError::UnexpectedEOL)?;

    let expression = parse_expression(&token.token)?;

    let where_token = tokens.get(5).ok_or(ParserError::UnexpectedEOL)?;

    if where_token.token == Token::Where {
        let predicate = parse_predicate(&tokens[6..])?;
        return Ok(UpdateNode {
            table: table_name,
            field: field_name,
            value: expression,
            predicate: Some(predicate),
        });
    } else {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::Equal],
            position: token.pos,
        }));
    }
}

fn parse_field_defs(tokens: &[TokenWithPos]) -> Result<Vec<FieldDefNode>, ParserError> {
    let mut field_defs = vec![];

    let mut index = 0;

    loop {
        let (field_def, field_def_index) = parse_field_def(&tokens[index..])?;
        field_defs.push(field_def);

        let token = tokens
            .get(field_def_index)
            .ok_or(ParserError::UnexpectedEOL)?;

        if token.token != Token::COMMA {
            break;
        }

        index += field_def_index + 1
    }

    return Ok(field_defs);
}

fn parse_field_def(tokens: &[TokenWithPos]) -> Result<(FieldDefNode, usize), ParserError> {
    let token = tokens.get(0).ok_or(ParserError::UnexpectedEOL)?;

    let field_name = if let Token::IDENT(ident_name) = token.token.clone() {
        Ok(ident_name.clone())
    } else {
        Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::IDENT],
            position: token.pos,
        }))
    }?;

    let token = tokens.get(1).ok_or(ParserError::UnexpectedEOL)?;

    let field_type = if token.token == Token::INT {
        FieldType::INTEGER
    } else if token.token != Token::VARCHAR {
        FieldType::VARCHAR
    } else {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::INT, TokenKind::VARCHAR],
            position: token.pos,
        }));
    };

    let token = tokens.get(2).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::LeftParen {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::LeftParen],
            position: token.pos,
        }));
    }

    let token = tokens.get(3).ok_or(ParserError::UnexpectedEOL)?;

    if let Token::PositiveInteger(int_token) = token.token {
        let token = tokens.get(4).ok_or(ParserError::UnexpectedEOL)?;

        if token.token != Token::RightParen {
            return Err(ParserError::ParseError(ParseError {
                invalid_token: token.token.clone(),
                expected_token: vec![TokenKind::RightParen],
                position: token.pos,
            }));
        }

        return Ok((
            FieldDefNode {
                field_length: int_token,
                field_name: field_name,
                field_type: field_type,
            },
            5,
        ));
    } else {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::INT, TokenKind::VARCHAR],
            position: token.pos,
        }));
    }
}

fn parse_create_view(tokens: &[TokenWithPos]) -> Result<CreateViewNode, ParserError> {
    let token = tokens.get(0).ok_or(ParserError::UnexpectedEOL)?;

    let view_name = if let Token::IDENT(ident_name) = token.token.clone() {
        Ok(ident_name.clone())
    } else {
        Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::IDENT],
            position: token.pos,
        }))
    }?;

    let token = tokens.get(1).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::As {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::As],
            position: token.pos,
        }));
    }

    let parsed_query = parse_select(&tokens[2..])?;

    return Ok(CreateViewNode {
        view_name: view_name,
        query: parsed_query,
    });
}

fn parse_create_index(tokens: &[TokenWithPos]) -> Result<CreateIndexNode, ParserError> {
    let token = tokens.get(0).ok_or(ParserError::UnexpectedEOL)?;

    let index_name = if let Token::IDENT(ident_name) = token.token.clone() {
        Ok(ident_name.clone())
    } else {
        Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::IDENT],
            position: token.pos,
        }))
    }?;

    let token = tokens.get(1).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::On {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::On],
            position: token.pos,
        }));
    }

    let token = tokens.get(2).ok_or(ParserError::UnexpectedEOL)?;

    let table_name = if let Token::IDENT(ident_name) = token.token.clone() {
        Ok(ident_name.clone())
    } else {
        Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::IDENT],
            position: token.pos,
        }))
    }?;

    let token = tokens.get(3).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::LeftParen {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::LeftParen],
            position: token.pos,
        }));
    }

    let token = tokens.get(4).ok_or(ParserError::UnexpectedEOL)?;

    let field_name = if let Token::IDENT(ident_name) = token.token.clone() {
        Ok(ident_name.clone())
    } else {
        Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::IDENT],
            position: token.pos,
        }))
    }?;

    let token = tokens.get(5).ok_or(ParserError::UnexpectedEOL)?;

    if token.token != Token::RightParen {
        return Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::RightParen],
            position: token.pos,
        }));
    }

    return Ok(CreateIndexNode {
        index_name: index_name,
        table_name: table_name,
        field_name: field_name,
    });
}

fn parse_create_table(tokens: &[TokenWithPos]) -> Result<CreateTableNode, ParserError> {
    let token = tokens.get(0).ok_or(ParserError::UnexpectedEOL)?;

    let table_name = if let Token::IDENT(ident_name) = token.token.clone() {
        Ok(ident_name.clone())
    } else {
        Err(ParserError::ParseError(ParseError {
            invalid_token: token.token.clone(),
            expected_token: vec![TokenKind::IDENT],
            position: token.pos,
        }))
    }?;

    let field_defs = parse_field_defs(tokens)?;
    return Ok(CreateTableNode {
        table_name: table_name,
        field_defs: field_defs,
    });
}

fn parse_predicate(tokens: &[TokenWithPos]) -> Result<Predicate, ParserError> {
    let (term, index) = parse_term(tokens)?;
    let next_token = tokens.get(index + 1);

    if let Some(token) = next_token {
        let token = &token.token;
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

fn parse_term(tokens: &[TokenWithPos]) -> Result<(Term, usize), ParserError> {
    let token = tokens.get(0).ok_or(ParserError::UnexpectedEOL)?;
    let token = &token.token;
    let left_expression = parse_expression(token)?;

    let next_token = tokens.get(1).ok_or(ParserError::UnexpectedEOL)?;

    let next_token = &next_token.token;

    if *next_token == Token::Equal {
        let token = tokens.get(2).ok_or(ParserError::UnexpectedEOL)?;
        let token = &token.token;
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
    use crate::parser::{
        parser::{
            parse, Constant, ErrorResport, Expression, InsertNode, ParseError, Predicate, SQLNode,
            SelectNode, Term,
        },
        tokenizer::{Token, TokenKind},
    };

    #[test]
    fn test_select_sql_parse() {
        let test_data = "select field, field2 from table1, table2 where a = 1 and b = 3";
        let parsed_select = parse(test_data).expect("");
        assert_eq!(
            parsed_select,
            SQLNode::SELECT(SelectNode {
                fields: vec!["field".to_string(), "field2".to_string()],
                tables: vec!["table1".to_string(), "table2".to_string()],
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

    #[test]
    fn test_insert_sql_parse() {
        let test_data = "insert into table1 field1, field2 values 'const1', 11";
        let parsed_select = parse(test_data).expect("");
        assert_eq!(
            parsed_select,
            SQLNode::INSERT(InsertNode {
                fields: vec!["field1".to_string(), "field2".to_string()],
                table: "table1".to_string(),
                constants: vec![
                    Constant::String("const1".to_string()),
                    Constant::Integer(11)
                ]
            })
        );
    }

    #[test]
    fn test_select_sql_parse_error() {
        let test_data = "select field   field2 from table where a = 1 and b = 3";
        let parsed_select = parse(test_data);
        assert_eq!(
            parsed_select,
            Err(crate::parser::parser::ParserError::ParseError(ParseError {
                invalid_token: Token::IDENT("field2".to_string()),
                expected_token: vec![TokenKind::From],
                position: 15
            }))
        )
    }

    #[test]
    fn test_select_sql_parse_error_2() {
        let test_data = "select field, field2 from table where a = ";
        let parsed_select = parse(test_data);
        println!("{:?}", parsed_select);
        assert_eq!(
            parsed_select,
            Err(crate::parser::parser::ParserError::UnexpectedEOL)
        )
    }
}
