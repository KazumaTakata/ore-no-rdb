use regex::Regex;
use std::error;
use std::fmt;
use std::sync::LazyLock;

#[derive(Debug, PartialEq, Eq)]
enum Token {
    Select,
    From,
    Insert,
    Into,
    Values,
    Update,
    Delete,
    Equal,
    IDENT(String),
    Number(i32),
    String(String),
    Where,
    COMMA,
    LeftParen,
    RightParen,
}

#[derive(Debug)]
struct TokenResponse {
    token: Token,
    position: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct InvalidCharacterError {
    character: String,
}

#[derive(Debug, PartialEq, Eq)]
enum TokenizationError {
    InvalidCharacter(InvalidCharacterError),
}

impl fmt::Display for TokenizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenizationError::InvalidCharacter(x) => write!(
                f,
                "不適切な文字列がありました. pos: character:{}",
                x.character
            ),
        }
    }
}

impl error::Error for TokenizationError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        return None;
    }
}

static SELECT_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(?i)select\s+").unwrap());
static INSERT_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(?i)insert\s+").unwrap());
static FROM_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(?i)from\s+").unwrap());
static INTO_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(?i)into\s+").unwrap());
static VALUES_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(?i)values\s+").unwrap());
static UPDATE_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(?i)updates\s+").unwrap());
static WHERE_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(?i)where\s+").unwrap());
static LEADING_WHITESPACE_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\s+").unwrap());
static COMMA_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^,").unwrap());
static EQUAL_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^=").unwrap());

fn get_token(input_text: &str) -> Result<TokenResponse, TokenizationError> {
    let input_text = input_text.trim_start();

    if let Some(value) = SELECT_REGEX.find(input_text) {
        return Ok(TokenResponse {
            token: Token::Select,
            position: value.end(),
        });
    }

    if let Some(value) = INSERT_REGEX.find(input_text) {
        return Ok(TokenResponse {
            token: Token::Insert,
            position: value.end(),
        });
    }

    if let Some(value) = FROM_REGEX.find(input_text) {
        return Ok(TokenResponse {
            token: Token::From,
            position: value.end(),
        });
    }

    if let Some(value) = INTO_REGEX.find(input_text) {
        return Ok(TokenResponse {
            token: Token::Into,
            position: value.end(),
        });
    }

    if let Some(value) = VALUES_REGEX.find(input_text) {
        return Ok(TokenResponse {
            token: Token::Values,
            position: value.end(),
        });
    }

    if let Some(value) = UPDATE_REGEX.find(input_text) {
        return Ok(TokenResponse {
            token: Token::Update,
            position: value.end(),
        });
    }

    let delete_regex = Regex::new(r"^(?i)delete\s+").unwrap();
    if let Some(value) = delete_regex.find(input_text) {
        return Ok(TokenResponse {
            token: Token::Delete,
            position: value.end(),
        });
    }

    if let Some(value) = WHERE_REGEX.find(input_text) {
        return Ok(TokenResponse {
            token: Token::Where,
            position: value.end(),
        });
    }

    let indent_regex = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]{0,62}\s*").unwrap();
    if let Some(value) = indent_regex.find(input_text) {
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

    let left_paren_regex = Regex::new(r"^\(").unwrap();
    if let Some(value) = left_paren_regex.find(input_text) {
        return Ok(TokenResponse {
            token: Token::LeftParen,
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

    let right_paren_regex = Regex::new(r"^\)").unwrap();
    if let Some(value) = right_paren_regex.find(input_text) {
        return Ok(TokenResponse {
            token: Token::RightParen,
            position: value.end(),
        });
    }
    return Err(TokenizationError::InvalidCharacter(InvalidCharacterError {
        character: input_text.to_string(),
    }));
}

fn tokenize(input_text: &str) -> Result<Vec<Token>, TokenizationError> {
    let mut input_data = input_text;
    let mut result: Vec<Token> = vec![];
    while input_data.len() > 0 {
        input_data = input_data.trim_start();
        let token = get_token(input_data)?;
        input_data = &input_data[token.position..];
        result.push(token.token);
    }
    return Ok(result);
}

#[cfg(test)]
mod tests {
    use crate::parser::tokenizer::{
        get_token, tokenize, InvalidCharacterError, Token, TokenizationError,
    };

    #[test]
    fn test_get_token() {
        let test_data = "select field from table";
        let token = get_token(test_data).expect("Okであることを期待");
        assert_eq!(&test_data[token.position..], "field from table");
        assert_eq!(token.token, Token::Select)
    }

    #[test]
    fn test_from_token() {
        let test_data = "from table1";
        let token = get_token(test_data).expect("Okであることを期待");
        assert_eq!(&test_data[token.position..], "table1");
        assert_eq!(token.token, Token::From)
    }

    #[test]
    fn test_get_token_left_paren() {
        let test_data = "(var1, var2)";
        let token = get_token(test_data).expect("Okであることを期待");
        assert_eq!(&test_data[token.position..], "var1, var2)");
        assert_eq!(token.token, Token::LeftParen)
    }

    #[test]
    fn test_tokenize() {
        let test_data = "  select field from table  ";
        let token_vec = tokenize(test_data).expect("Okであることを期待");
        assert_eq!(
            token_vec,
            vec![
                Token::Select,
                Token::IDENT("field".to_string()),
                Token::From,
                Token::IDENT("table".to_string())
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
                Token::Insert,
                Token::Into,
                Token::IDENT("table1".to_string()),
                Token::LeftParen,
                Token::IDENT("field1".to_string()),
                Token::COMMA,
                Token::IDENT("field2".to_string()),
                Token::RightParen,
                Token::Values,
                Token::LeftParen,
                Token::String("value1".to_string()),
                Token::COMMA,
                Token::Number(200),
                Token::RightParen,
            ]
        );
    }

    #[test]
    fn test_tokenize_3() {
        let test_data = "select field1 from table where field1 = 200";
        let token_vec = tokenize(test_data).expect("Okであることを期待");
        assert_eq!(
            token_vec,
            vec![
                Token::Select,
                Token::IDENT("field1".to_string()),
                Token::From,
                Token::IDENT("table".to_string()),
                Token::Where,
                Token::IDENT("field1".to_string()),
                Token::Equal,
                Token::Number(200),
            ]
        );
    }

    #[test]
    fn test_tokenize_4() {
        let test_data = "select あいうえお from table where field1 = 200";
        let result = tokenize(test_data);
        assert_eq!(
            result.unwrap_err(),
            TokenizationError::InvalidCharacter(InvalidCharacterError {
                character: "あいうえお from table where field1 = 200".to_string()
            })
        );
    }
}
