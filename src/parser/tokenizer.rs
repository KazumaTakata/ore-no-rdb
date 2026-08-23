use regex::Regex;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::fmt::Pointer;
use std::sync::LazyLock;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TokenWithPos {
    pub token: Token,
    pub pos: usize,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Token {
    Select,
    From,
    Insert,
    Into,
    Values,
    Update,
    Create,
    Table,
    View,
    Index,
    On,
    As,
    Delete,
    Equal,
    Where,
    Set,
    INT,
    VARCHAR,
    COMMA,
    AND,
    LeftParen,
    RightParen,

    IDENT(String),
    Number(i32),
    PositiveInteger(usize),
    String(String),
}

static KEYWORD_MAP: LazyLock<HashMap<&str, Token>> = LazyLock::new(|| {
    HashMap::from([
        ("select", Token::Select),
        ("from", Token::From),
        ("insert", Token::Insert),
        ("into", Token::Into),
        ("values", Token::Values),
        ("and", Token::AND),
        ("update", Token::Update),
        ("where", Token::Where),
        ("create", Token::Create),
        ("table", Token::Table),
        ("view", Token::View),
        ("index", Token::Index),
        ("on", Token::On),
        ("as", Token::As),
        ("delete", Token::Delete),
        ("equal", Token::Equal),
        ("where", Token::Where),
        ("set", Token::Set),
        ("int", Token::INT),
        ("varchar", Token::VARCHAR),
    ])
});

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TokenKind {
    Select,
    From,
    Insert,
    Into,
    Values,
    Update,
    Delete,
    Create,
    Table,
    View,
    Index,
    INT,
    VARCHAR,
    PositiveInteger,
    On,
    As,
    Equal,
    IDENT,
    Number,
    Set,
    String,
    Where,
    COMMA,
    LeftParen,
    RightParen,
    AND,
}

impl Token {
    pub fn kind(&self) -> TokenKind {
        match self {
            Token::Select => TokenKind::Select,
            Token::From => TokenKind::From,
            Token::Insert => TokenKind::Insert,
            Token::Into => TokenKind::Into,
            Token::Values => TokenKind::Values,
            Token::Create => TokenKind::Create,
            Token::Table => TokenKind::Table,
            Token::View => TokenKind::View,
            Token::Index => TokenKind::Index,
            Token::PositiveInteger(_) => TokenKind::PositiveInteger,
            Token::INT => TokenKind::INT,
            Token::VARCHAR => TokenKind::VARCHAR,
            Token::On => TokenKind::On,
            Token::As => TokenKind::As,
            Token::Delete => TokenKind::Delete,
            Token::Update => TokenKind::Update,
            Token::Equal => TokenKind::Equal,
            Token::IDENT(_) => TokenKind::IDENT,
            Token::Number(_) => TokenKind::Number,
            Token::String(_) => TokenKind::String,
            Token::Where => TokenKind::Where,
            Token::COMMA => TokenKind::COMMA,
            Token::LeftParen => TokenKind::LeftParen,
            Token::RightParen => TokenKind::RightParen,
            Token::AND => TokenKind::AND,
            Token::Set => TokenKind::Set,
        }
    }
}

#[derive(Debug)]
struct TokenResponse {
    token: Token,
    position: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct InvalidCharacterError {
    position: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TokenizationError {
    InvalidCharacter(InvalidCharacterError),
}

impl fmt::Display for TokenizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenizationError::InvalidCharacter(x) => {
                write!(f, "不適切な文字列がありました. pos:{}", x.position)
            }
        }
    }
}

impl error::Error for TokenizationError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        return None;
    }
}

static COMMA_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^,").unwrap());
static EQUAL_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^=").unwrap());

fn get_token(input_text: &str, position: usize) -> Result<TokenResponse, TokenizationError> {
    let ident_regex = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]{0,62}\s*").unwrap();
    if let Some(value) = ident_regex.find(input_text) {
        let keyword = KEYWORD_MAP.get(value.as_str().trim().to_lowercase().as_str());

        if let Some(keyword) = keyword {
            return Ok(TokenResponse {
                token: keyword.clone(),
                position: value.end(),
            });
        }

        return Ok(TokenResponse {
            token: Token::IDENT(value.as_str().trim().to_string()),
            position: value.end(),
        });
    }

    let string_regex = Regex::new(r"^'[A-Za-z_][A-Za-z0-9_]{0,62}'").unwrap();
    if let Some(value) = string_regex.find(input_text) {
        return Ok(TokenResponse {
            token: Token::String(value.as_str()[1..value.len() - 1].to_string()),
            position: value.end(),
        });
    }

    let number_regex = Regex::new(r"^[0-9]+").unwrap();
    if let Some(value) = number_regex.find(input_text) {
        return Ok(TokenResponse {
            token: Token::Number(value.as_str().to_string().parse().unwrap()),
            position: value.end(),
        });
    }

    if let Some(value) = COMMA_REGEX.find(input_text) {
        return Ok(TokenResponse {
            token: Token::COMMA,
            position: value.end(),
        });
    }

    if let Some(value) = EQUAL_REGEX.find(input_text) {
        return Ok(TokenResponse {
            token: Token::Equal,
            position: value.end(),
        });
    }

    let left_paren_regex = Regex::new(r"^\(").unwrap();
    if let Some(value) = left_paren_regex.find(input_text) {
        return Ok(TokenResponse {
            token: Token::LeftParen,
            position: value.end(),
        });
    }

    let right_paren_regex = Regex::new(r"^\)").unwrap();
    if let Some(value) = right_paren_regex.find(input_text) {
        return Ok(TokenResponse {
            token: Token::RightParen,
            position: value.end(),
        });
    }

    return Err(TokenizationError::InvalidCharacter(InvalidCharacterError {
        position: position,
    }));
}

pub fn tokenize(input_text: &str) -> Result<Vec<TokenWithPos>, TokenizationError> {
    let mut position = 0;
    let mut result: Vec<TokenWithPos> = vec![];
    let input_date_without_white_space = input_text[position..].trim_start();
    position += input_text[position..].len() - input_date_without_white_space.len();

    while input_text[position..].len() > 0 {
        let token = get_token(&input_text[position..], position)?;
        result.push(TokenWithPos {
            token: token.token,
            pos: position,
        });

        position += token.position;

        let input_date_without_white_space = input_text[position..].trim_start();
        position += input_text[position..].len() - input_date_without_white_space.len();
    }
    return Ok(result);
}

#[cfg(test)]
mod tests {
    use crate::parser::tokenizer::{
        get_token, tokenize, InvalidCharacterError, Token, TokenWithPos, TokenizationError,
    };

    #[test]
    fn test_tokenize() {
        let test_data = "  select field from table  ";
        let token_vec = tokenize(test_data).expect("Okであることを期待");
        assert_eq!(
            token_vec,
            vec![
                TokenWithPos {
                    token: Token::Select,
                    pos: 2
                },
                TokenWithPos {
                    token: Token::IDENT("field".to_string()),
                    pos: 9
                },
                TokenWithPos {
                    token: Token::From,
                    pos: 15
                },
                TokenWithPos {
                    token: Token::Table,
                    pos: 20
                },
            ]
        );
    }

    #[test]
    fn test_tokenize_2() {
        let test_data = "insert into table1 (field1, field2) values ('value1', 200)";
        let token_vec = tokenize(test_data).expect("Okであることを期待");
        assert_eq!(
            token_vec,
            vec![
                TokenWithPos {
                    token: Token::Insert,
                    pos: 0
                },
                TokenWithPos {
                    token: Token::Into,
                    pos: 7
                },
                TokenWithPos {
                    token: Token::IDENT("table1".to_string()),
                    pos: 12
                },
                TokenWithPos {
                    token: Token::LeftParen,
                    pos: 19
                },
                TokenWithPos {
                    token: Token::IDENT("field1".to_string()),
                    pos: 20
                },
                TokenWithPos {
                    token: Token::COMMA,
                    pos: 26
                },
                TokenWithPos {
                    token: Token::IDENT("field2".to_string()),
                    pos: 28
                },
                TokenWithPos {
                    token: Token::RightParen,
                    pos: 34
                },
                TokenWithPos {
                    token: Token::Values,
                    pos: 36
                },
                TokenWithPos {
                    token: Token::LeftParen,
                    pos: 43
                },
                TokenWithPos {
                    token: Token::String("value1".to_string()),
                    pos: 44
                },
                TokenWithPos {
                    token: Token::COMMA,
                    pos: 52
                },
                TokenWithPos {
                    token: Token::Number(200),
                    pos: 54
                },
                TokenWithPos {
                    token: Token::RightParen,
                    pos: 57
                },
            ]
        );
    }

    #[test]
    fn test_tokenize_4() {
        let test_data = "select あいうえお from table where field1 = 200";
        let result = tokenize(test_data);
        assert_eq!(
            result.unwrap_err(),
            TokenizationError::InvalidCharacter(InvalidCharacterError { position: 7 })
        );
    }
}
