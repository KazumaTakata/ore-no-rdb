use std::{
    cell::RefCell,
    io::{BufReader, BufWriter, Error, ErrorKind, Read, Result, Write},
    net::{TcpListener, TcpStream},
    os::unix::io,
    rc::Rc,
};

use crate::{
    database::Database,
    metadata::metadata_manager::MetadataManager,
    parser::parser::{parse, FieldType},
    parser_mapping::sql_node_to_parsed_sql,
    query::{
        index_update_planner::{self, IndexUpdatePlanner},
        parser::ParsedSQL,
        plan_v2::execute_create_table,
        predicate::ConstantValue,
        query_handler::handle_select_query,
    },
};

pub fn handle_serve(database: &Database) -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, database);
    }
    return Ok(());
}

const MAX_MESSAGE_BYTES: i32 = 300;
const SSL_REQUEST_CODE: i32 = 80877103;
const PROTOCOL_VERSION_3: i32 = 196608;

struct PostgresTypeInfo {
    type_oid: i32,
    type_size: i16,
    type_modifier: i32,
}

fn pg_type_info(t: &FieldType) -> PostgresTypeInfo {
    match t {
        FieldType::INTEGER => PostgresTypeInfo {
            type_oid: 23,
            type_size: 4,
            type_modifier: -1,
        },
        FieldType::VARCHAR(n) => PostgresTypeInfo {
            type_oid: 1043,
            type_size: -1,
            type_modifier: *n as i32 + 4,
        },
    }
}

fn row_description_body(fields: &[(&str, PostgresTypeInfo)]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(fields.len() as i16).to_be_bytes());
    for (name, postgres_type_info) in fields {
        body.extend_from_slice(name.as_bytes());
        body.push(0); // NUL終端
        body.extend_from_slice(&0i32.to_be_bytes()); // テーブルOID
        body.extend_from_slice(&0i16.to_be_bytes()); // 属性番号
        body.extend_from_slice(&postgres_type_info.type_oid.to_be_bytes());
        body.extend_from_slice(&postgres_type_info.type_size.to_be_bytes());
        body.extend_from_slice(&postgres_type_info.type_modifier.to_be_bytes()); // 型修飾子
        body.extend_from_slice(&0i16.to_be_bytes()); // テキスト形式
    }
    body
}

fn data_row_body(values: &Vec<Option<ConstantValue>>) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for v in values {
        match v {
            Some(v) => match v {
                ConstantValue::Null => body.extend_from_slice(&(-1i32).to_be_bytes()),
                ConstantValue::Number(n) => push_value(&mut body, n.to_string().as_bytes()),
                ConstantValue::String(s) => push_value(&mut body, s.as_bytes()),
            },
            None => {}
        }
    }
    body
}

fn push_value(body: &mut Vec<u8>, bytes: &[u8]) {
    body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    body.extend_from_slice(bytes);
}

// 'Q' を受け取ったときの応答
fn send_select_result(
    writer: &mut impl Write,
    row_desc_body: &[u8],
    rows: &[Vec<Option<ConstantValue>>],
) -> Result<()> {
    write_message(writer, b'T', row_desc_body)?;
    for row in rows {
        write_message(writer, b'D', &data_row_body(row))?;
    }
    write_message(writer, b'C', format!("SELECT {}\0", rows.len()).as_bytes())?;
    write_message(writer, b'Z', b"I")?;
    writer.flush()
}

// [長さ: i32（自分自身の4バイトを含む）][本体]
fn read_body(reader: &mut impl Read) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = i32::from_be_bytes(len_buf);
    if len < 4 || len - 4 > MAX_MESSAGE_BYTES {
        return Err(Error::new(ErrorKind::InvalidData, "invalid message length"));
    }
    let mut body = vec![0u8; len as usize - 4];
    reader.read_exact(&mut body)?;
    Ok(body)
}

// [種類: 1バイト][長さ: i32（自分自身の4バイトを含む）][本体]
fn write_message(writer: &mut impl Write, message_type: u8, body: &[u8]) -> Result<()> {
    writer.write_all(&[message_type])?;
    writer.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    writer.write_all(body)
}

fn startup(reader: &mut impl Read, writer: &mut impl Write) -> Result<bool> {
    loop {
        // 起動時のメッセージだけは、先頭に種類の1バイトが無い
        let body = read_body(reader)?;
        let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        println!("code: {}", code);

        match code {
            SSL_REQUEST_CODE => {
                writer.write_all(b"N")?; // 暗号化は断る。クライアントは平文で送り直す
                writer.flush()?;
            }
            //
            // CANCEL_REQUEST_CODE => return Ok(false),
            PROTOCOL_VERSION_3 => break,

            //
            _ => { /* FATAL エラーを返して切る */ }
        }
    }
    write_message(writer, b'R', &0i32.to_be_bytes())?;
    write_message(writer, b'Z', b"I")?;
    writer.flush()?;
    Ok(true)
}

fn send_error(writer: &mut impl Write, message: &str) -> std::io::Result<()> {
    let mut body = Vec::new();
    for (code, value) in [
        (b'S', "ERROR"),
        (b'V', "ERROR"),
        (b'C', "42601"),
        (b'M', message),
    ] {
        body.push(code);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0); // フィールドの終わり
    write_message(writer, b'E', &body)?;
    write_message(writer, b'Z', b"I")?;
    writer.flush()
}

fn handle_connection(stream: TcpStream, database: &Database) -> Result<()> {
    let transaction = database.new_transaction(1);
    let metadata_manager = Rc::new(RefCell::new(
        MetadataManager::new(transaction.clone()).unwrap(),
    ));

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);
    if !startup(&mut reader, &mut writer)? {
        return Ok(());
    }

    loop {
        let mut message_type = [0u8; 1];
        match reader.read_exact(&mut message_type) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }
        let body = read_body(&mut reader)?;
        match message_type[0] {
            b'Q' => {
                let sql = String::from_utf8_lossy(&body);
                // ;の後に謎のcharが含まれているため -2
                let sql_node = match parse(&sql[..sql.len() - 2]) {
                    Ok(sql_node) => sql_node,
                    Err(err) => {
                        send_error(&mut writer, &format!("{:?}", err));
                        continue;
                    }
                };

                let parsed_sql = sql_node_to_parsed_sql(sql_node);
                match parsed_sql {
                    ParsedSQL::Query(select_query) => {
                        let (headers, result_vec) = match handle_select_query(
                            &select_query,
                            &mut metadata_manager.borrow_mut(),
                            transaction.clone(),
                        ) {
                            Ok(sql_node) => sql_node,
                            Err(err) => {
                                send_error(&mut writer, &format!("{:?}", err));
                                continue;
                            }
                        };

                        transaction.borrow_mut().commit();

                        // let query = read_cstring(&body)?;
                        let row_desc_fields = headers
                            .iter()
                            .map(|header| (header.as_str(), pg_type_info(&FieldType::VARCHAR(100))))
                            .into_iter();

                        let mut desc_body = Vec::new();
                        desc_body.extend_from_slice(&(row_desc_fields.len() as i16).to_be_bytes());
                        for (name, postgres_type_info) in row_desc_fields {
                            desc_body.extend_from_slice(name.as_bytes());
                            desc_body.push(0); // NUL終端
                            desc_body.extend_from_slice(&0i32.to_be_bytes()); // テーブルOID
                            desc_body.extend_from_slice(&0i16.to_be_bytes()); // 属性番号
                            desc_body.extend_from_slice(&postgres_type_info.type_oid.to_be_bytes());
                            desc_body
                                .extend_from_slice(&postgres_type_info.type_size.to_be_bytes());
                            desc_body
                                .extend_from_slice(&postgres_type_info.type_modifier.to_be_bytes()); // 型修飾子
                            desc_body.extend_from_slice(&0i16.to_be_bytes()); // テキスト形式
                        }
                        //
                        //                         let rows = [
                        //                             vec![ConstantValue::String("takata".to_string())],
                        //                             vec![ConstantValue::String("satuo".to_string())],
                        //                         ];
                        //
                        let _ = send_select_result(&mut writer, &desc_body, &result_vec);
                        writer.flush()?;
                    }
                    ParsedSQL::CreateTable(create_table_query) => {
                        let result = execute_create_table(
                            transaction.clone(),
                            &mut metadata_manager.borrow_mut(),
                            create_table_query.clone(),
                        );
                        if let Ok(()) = result {
                            write_message(&mut writer, b'C', "CREATE TABLE\0".as_bytes())?;
                            write_message(&mut writer, b'Z', b"I")?;
                            writer.flush()?;
                        } else {
                            eprintln!("Error executing create table: {:?}", result.err());
                        };
                    }
                    ParsedSQL::Insert(insert_query) => {
                        // execute_insert(transaction.clone(), metadata_manager, insert_data.clone());
                        let index_update_planner = IndexUpdatePlanner::new();
                        let result = index_update_planner.execute_insert(
                            insert_query.clone(),
                            transaction.clone(),
                            &mut metadata_manager.borrow_mut(),
                        );
                        if let Ok(()) = result {
                            write_message(&mut writer, b'C', "INSERT 0 1\0".as_bytes())?;
                            write_message(&mut writer, b'Z', b"I")?;
                            writer.flush()?;
                        } else {
                            eprintln!("Error executing create table: {:?}", result.err());
                        };

                        transaction.borrow_mut().commit();
                    }

                    _ => {}
                }
                //
            }
            b'X' => return Ok(()), // Terminate
            other => { /* 拡張クエリなどは非対応として FATAL で切る */ }
        }
    }
}
