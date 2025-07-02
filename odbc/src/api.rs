// ODBC API implementation will go here.

use arrow::{
    array::{Int8Array, RecordBatch},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use odbc_sys as sql;
use sf_core::{
    api_client,
    handle_manager::{Handle, HandleManager},
    thrift_gen::database_driver_v1::{
        ConnectionHandle as TConnectionHandle, DatabaseHandle as TDatabaseHandle, ExecuteResult,
        StatementHandle, TDatabaseDriverSyncClient,
    },
};
use std::{collections::HashMap, slice, str, sync::Mutex};

struct Environment {
    odbc_version: sql::Integer,
    driver_version: i32,
}

enum ConnectionState {
    Disconnected,
    Connected {
        client: Box<dyn TDatabaseDriverSyncClient + Send>,
        db_handle: TDatabaseHandle,
        conn_handle: TConnectionHandle,
    },
}

struct Connection {
    state: ConnectionState,
}

enum StatementState {
    Created,
    Executed {
        result: ExecuteResult,
    },
    Fetched {
        reader: ArrowArrayStreamReader,
        record_batch: RecordBatch,
    },
    Done,
}

struct Statement<'a> {
    conn: &'a mut Connection,
    stmt_handle: StatementHandle,
    state: StatementState,
}

pub unsafe extern "C" fn SQLAllocEnv(output_handle: *mut sql::Handle) -> i16 {
    eprintln!("RUST: SQLAllocEnv");
    SQLAllocHandle(sql::HandleType::Env, 0 as sql::Handle, output_handle)
}

pub unsafe extern "C" fn SQLAllocConnect(
    environment_handle: sql::Handle,
    output_handle: *mut sql::Handle,
) -> i16 {
    eprintln!("RUST: SQLAllocConnect");
    return SQLAllocHandle(sql::HandleType::Dbc, environment_handle, output_handle);
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLAllocHandle(
    handle_type: sql::HandleType,
    input_handle: sql::Handle,
    output_handle: *mut sql::Handle,
) -> i16 {
    eprintln!("RUST: SQLAllocHandle: {:?}", handle_type);
    match handle_type {
        sql::HandleType::Env => {
            eprintln!("Allocating new env: SQLAllocHandle: {:?}", handle_type);
            let env = Box::new(Environment {
                odbc_version: 3,
                driver_version: 1,
            });
            let handle = Box::into_raw(env);
            std::ptr::write(output_handle, handle as sql::Handle);
            return sql::SqlReturn::SUCCESS.0;
        }
        sql::HandleType::Dbc => {
            eprintln!("Allocating new dbc: SQLAllocHandle: {:?}", handle_type);
            let dbc = Box::new(Connection {
                state: ConnectionState::Disconnected,
            });
            // eprintln!("RUST: SQLAllocHandle: dbc: {:?}", dbc);
            let handle = Box::into_raw(dbc);
            // eprintln!("RUST: SQLAllocHandle: dbc: {:?}", handle);
            *output_handle = handle as sql::Handle;
            eprintln!("RUST: SQLAllocHandle: dbc: {:?}", handle);
            return sql::SqlReturn::SUCCESS.0;
        }
        sql::HandleType::Stmt => {
            eprintln!("Allocating new stmt: SQLAllocHandle: {:?}", handle_type);
            let conn_ptr = input_handle as *mut Connection;
            let conn = conn_ptr.as_mut().unwrap();
            match &mut conn.state {
                ConnectionState::Connected {
                    client,
                    db_handle,
                    conn_handle,
                } => {
                    let stmt_handle = client.statement_new(conn_handle.clone()).unwrap();
                    let stmt = Box::new(Statement {
                        conn: conn,
                        stmt_handle: stmt_handle,
                        state: StatementState::Created,
                    });
                    let handle = Box::into_raw(stmt);
                    std::ptr::write(output_handle, handle as sql::Handle);
                    return sql::SqlReturn::SUCCESS.0;
                }
                ConnectionState::Disconnected => {
                    return sql::SqlReturn::ERROR.0;
                }
            };
        }
        sql::HandleType::Desc => {
            // Not implemented yet
            eprintln!("RUST: SQLAllocHandle: Desc: {:?}", handle_type);
            return sql::SqlReturn::ERROR.0;
        }
        _ => {
            eprintln!(
                "RUST: SQLAllocHandle: unknown handle type: {:?}",
                handle_type
            );
            return sql::SqlReturn::ERROR.0;
        }
    }
}

unsafe fn text_to_string(text: *const sql::Char, length: sql::Integer) -> String {
    if length == sql::NTS as i32 {
        return unsafe {
            std::ffi::CStr::from_ptr(text as *const i8)
                .to_str()
                .unwrap()
                .to_string()
        };
    } else {
        let text_slice = unsafe { std::slice::from_raw_parts(text, length as usize) };
        return String::from_utf8(text_slice.to_vec()).unwrap();
    }
}

/// # Safety
///
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLExecDirect(
    statement_handle: sql::Handle,
    statement_text: *const sql::Char,
    text_length: sql::Integer,
) -> sql::RetCode {
    let stmt_ptr = statement_handle as *mut Statement;
    let stmt = stmt_ptr.as_mut().unwrap();
    eprintln!("RUST: SQLExecDirect: {:?}", statement_handle);
    match &mut stmt.conn.state {
        ConnectionState::Connected {
            client,
            db_handle,
            conn_handle,
        } => {
            let query = text_to_string(statement_text, text_length);
            client
                .statement_set_sql_query(stmt.stmt_handle.clone(), query)
                .unwrap();
            let result = client
                .statement_execute_query(stmt.stmt_handle.clone())
                .unwrap();
            stmt.state = StatementState::Executed { result: result };
            return sql::SqlReturn::SUCCESS.0;
        }
        ConnectionState::Disconnected => {
            eprintln!("RUST: SQLExecDirect: disconnected");
            return sql::SqlReturn::ERROR.0;
        }
    }
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLFreeHandle(
    handle_type: sql::HandleType,
    handle: sql::Handle,
) -> sql::SqlReturn {
    if handle.is_null() {
        return sql::SqlReturn::INVALID_HANDLE;
    }

    match handle_type {
        sql::HandleType::Env => {
            eprintln!("Freeing env: SQLFreeHandle: {:?}", handle_type);
            drop(Box::from_raw(handle as *mut Environment));
            sql::SqlReturn::SUCCESS
        }
        sql::HandleType::Dbc => {
            eprintln!("Freeing dbc: SQLFreeHandle: {:?}", handle_type);
            drop(Box::from_raw(handle as *mut Connection));
            sql::SqlReturn::SUCCESS
        }
        sql::HandleType::Stmt => {
            eprintln!("Freeing stmt: SQLFreeHandle: {:?}", handle_type);
            drop(Box::from_raw(handle as *mut Statement));
            sql::SqlReturn::SUCCESS
        }
        sql::HandleType::Desc => {
            // Not implemented yet
            sql::SqlReturn::ERROR
        }
        _ => sql::SqlReturn::ERROR,
    }
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLConnect(
    connection_handle: sql::Handle,
    server_name: *const sql::Char,
    name_length1: sql::SmallInt,
    _user_name: *const sql::Char,
    _name_length2: sql::SmallInt,
    _authentication: *const sql::Char,
    _name_length3: sql::SmallInt,
) -> sql::SqlReturn {
    eprintln!("RUST: SQLConnect: {:?}", connection_handle);
    // todo!()

    // let dbc_handle = *(connection_handle as *mut Handle);

    // let conn_mutex = match DBC_HANDLE_MANAGER.get_obj(dbc_handle) {
    //     Some(c) => c,
    //     None => return sql::SqlReturn::INVALID_HANDLE,
    // };

    // let mut conn = conn_mutex.lock().unwrap();

    // let mut client = api_client::new_database_driver_v1_client();

    // let db_handle = match client.database_new() {
    //     Ok(h) => h,
    //     Err(_) => return sql::SqlReturn::ERROR,
    // };

    // let server_name_str =
    //     str::from_utf8(slice::from_raw_parts(server_name, name_length1 as usize)).unwrap();

    // if client
    //     .database_set_option_string(
    //         db_handle.clone(),
    //         "uri".to_string(),
    //         server_name_str.to_string(),
    //     )
    //     .is_err()
    // {
    //     return sql::SqlReturn::ERROR;
    // }

    // // You can also set username and password if the driver supports it
    // // let user_name_str = ...
    // // let authentication_str = ...
    // // client.database_set_option_string(db_handle.clone(), "username", user_name_str.to_string())
    // // client.database_set_option_string(db_handle.clone(), "password", authentication_str.to_string())

    // if client.database_init(db_handle.clone()).is_err() {
    //     return sql::SqlReturn::ERROR;
    // }

    // let conn_handle = match client.connection_new() {
    //     Ok(h) => h,
    //     Err(_) => return sql::SqlReturn::ERROR,
    // };

    // // The thrift definition for connection_init takes a string for db_handle,
    // // but the current server implementation doesn't use it.
    // // We will pass an empty string.
    // if client
    //     .connection_init(conn_handle.clone(), "".to_string())
    //     .is_err()
    // {
    //     return sql::SqlReturn::ERROR;
    // }

    // conn.client = Some(client);
    // conn.db_handle = Some(db_handle);
    // conn.conn_handle = Some(conn_handle);

    sql::SqlReturn::SUCCESS
}

fn to_env_attr(attribute: i32) -> Option<sql::EnvironmentAttribute> {
    match attribute {
        200 => Some(sql::EnvironmentAttribute::OdbcVersion),
        201 => Some(sql::EnvironmentAttribute::ConnectionPooling),
        202 => Some(sql::EnvironmentAttribute::CpMatch),
        10001 => Some(sql::EnvironmentAttribute::OutputNts),
        _ => todo!(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLSetEnvAttr(
    environment_handle: sql::Handle,
    attribute: sql::Integer,
    value: sql::Pointer,
    _string_length: sql::SmallInt,
) -> i16 {
    eprintln!("RUST: SQLSetEnvAttr: {:?}", environment_handle);
    let env_ptr = environment_handle as *mut Environment;
    let env = env_ptr.as_mut().unwrap();
    let attr = to_env_attr(attribute);
    if attr.is_none() {
        eprintln!("RUST: SQLSetEnvAttr: unknown attribute: {:?}", attribute);
        return sql::SqlReturn::ERROR.0;
    }

    match attr.unwrap() {
        sql::EnvironmentAttribute::OdbcVersion => {
            eprintln!("RUST: SQLSetEnvAttr: OdbcVersion: {:?}", value);
            let int = value as sql::Integer;
            env.odbc_version = int;
            return sql::SqlReturn::SUCCESS.0;
        }
        _ => {
            eprintln!("RUST: SQLSetEnvAttr: unknown attribute: {:?}", attribute);
            return sql::SqlReturn::ERROR.0;
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLGetEnvAttr(
    environment_handle: sql::Handle,
    attribute: sql::Integer,
    value: sql::Pointer,
    _string_length: sql::SmallInt,
) -> i16 {
    eprintln!("RUST: SQLGetEnvAttr: {:?}", environment_handle);
    let env_ptr = environment_handle as *mut Environment;
    let env = env_ptr.as_mut().unwrap();
    let attr = to_env_attr(attribute);
    if attr.is_none() {
        eprintln!("RUST: SQLGetEnvAttr: unknown attribute: {:?}", attribute);
        return sql::SqlReturn::ERROR.0;
    }
    match attr.unwrap() {
        sql::EnvironmentAttribute::OdbcVersion => {
            eprintln!("RUST: SQLGetEnvAttr: OdbcVersion: {:?}", value);
            let int_ptr = value as *mut sql::Integer;
            std::ptr::write(int_ptr, env.odbc_version);
            eprintln!("RUST: SQLGetEnvAttr: OdbcVersion: {:?}", env.odbc_version);
            return sql::SqlReturn::SUCCESS.0;
        }
        _ => {
            eprintln!("RUST: SQLGetEnvAttr: unknown attribute: {:?}", attribute);
            return sql::SqlReturn::ERROR.0;
        }
    }
}

fn parse_connection_string(connection_string: &String) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in connection_string.split(';') {
        let parts: Vec<&str> = pair.splitn(2, '=').collect();
        if parts.len() == 2 {
            map.insert(parts[0].to_string(), parts[1].to_string());
        }
    }
    map
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLDriverConnect(
    connection_handle: sql::Handle,
    window_handle: sql::Handle,
    in_connection_string: *const sql::Char,
    in_string_length: sql::SmallInt,
    _out_connection_string: *mut sql::Char,
    _out_string_length: *mut sql::SmallInt,
    _driver_completion: sql::SmallInt,
) -> sql::RetCode {
    // Parse the connection string
    let connection_string = text_to_string(in_connection_string, in_string_length as i32);
    let connection_string_map = parse_connection_string(&connection_string);
    eprintln!("RUST: SQLDriverConnect: {:?}", connection_string_map);

    let connection_ptr = connection_handle as *mut Connection;
    let connection = connection_ptr.as_mut().unwrap();
    let mut client = api_client::new_database_driver_v1_client();
    let db_handle = match client.database_new() {
        Ok(h) => h,
        Err(_) => return sql::SqlReturn::ERROR.0,
    };
    let conn_handle = match client.connection_new() {
        Ok(h) => h,
        Err(_) => return sql::SqlReturn::ERROR.0,
    };

    for (key, value) in connection_string_map {
        match key.as_str() {
            "DRIVER" => {
                // ignore
            }
            "ACCOUNT" => {
                client
                    .connection_set_option_string(conn_handle.clone(), "account".to_owned(), value)
                    .unwrap();
            }
            "SERVER" => {
                client
                    .connection_set_option_string(conn_handle.clone(), "server".to_owned(), value)
                    .unwrap();
            }
            "PWD" => {
                client
                    .connection_set_option_string(conn_handle.clone(), "password".to_owned(), value)
                    .unwrap();
            }
            "UID" => {
                client
                    .connection_set_option_string(conn_handle.clone(), "user".to_owned(), value)
                    .unwrap();
            }
            _ => {
                eprintln!("RUST: SQLDriverConnect: unknown key: {:?}", key);
            }
        }
    }

    match client.connection_init(conn_handle.clone(), "".to_owned()) {
        Ok(_) => {
            connection.state = ConnectionState::Connected {
                client: client,
                db_handle: db_handle,
                conn_handle: conn_handle,
            };
            sql::SqlReturn::SUCCESS.0
        }
        Err(e) => {
            eprintln!("RUST: SQLDriverConnect: error: {:?}", e);
            sql::SqlReturn::ERROR.0
        }
    }
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLDisconnect(_connection_handle: sql::Handle) -> sql::SqlReturn {
    sql::SqlReturn::SUCCESS

    // let dbc_handle = *(connection_handle as *mut Handle);

    // let conn_mutex = match DBC_HANDLE_MANAGER.get_obj(dbc_handle) {
    //     Some(c) => c,
    //     None => return sql::SqlReturn::INVALID_HANDLE,
    // };

    // let mut conn = conn_mutex.lock().unwrap();

    // if let Some(mut client) = conn.client.take() {
    //     if let Some(conn_handle) = conn.conn_handle.take() {
    //         if client.connection_release(conn_handle).is_err() {
    //             return sql::SqlReturn::ERROR;
    //         }
    //     }
    // if let Some(db_handle) = conn.db_handle.take() {
    //     if client.database_release(db_handle).is_err() {
    //         return sql::SqlReturn::ERROR;
    //     }
    // }
    // }

    // sql::SqlReturn::SUCCESS
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLFetch(_statement_handle: sql::Handle) -> i16 {
    eprintln!("RUST: SQLFetch");
    let stmt_ptr = _statement_handle as *mut Statement;
    let stmt = stmt_ptr.as_mut().unwrap();
    match &mut stmt.state {
        StatementState::Executed { result } => {
            let stream_ptr: *mut FFI_ArrowArrayStream = result.stream.clone().into();
            let stream: FFI_ArrowArrayStream =
                unsafe { FFI_ArrowArrayStream::from_raw(stream_ptr) };
            let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();
            match reader.next() {
                Some(Ok(record_batch)) => {
                    eprintln!("RUST: SQLFetch: fetched: {:?}", record_batch);
                    stmt.state = StatementState::Fetched {
                        reader: reader,
                        record_batch: record_batch,
                    };
                    sql::SqlReturn::SUCCESS.0
                }
                Some(Err(e)) => {
                    eprintln!("RUST: SQLFetch: error: {:?}", e);
                    sql::SqlReturn::ERROR.0
                }
                None => {
                    eprintln!("RUST: SQLFetch: no data");
                    stmt.state = StatementState::Done;
                    sql::SqlReturn::NO_DATA.0
                }
            }
        }
        _ => {
            eprintln!("RUST: SQLFetch: not executed");
            sql::SqlReturn::ERROR.0
        }
    }
    // match &mut stmt.state {
    //     StatementState::Executed { result } => {
    //         result.stream.unwrap().next();
    //     }
    //     _ => {
    //         eprintln!("RUST: SQLFetch: not executed");
    //     }
    // }
    // stmt.number_of_rows = 1;
    // if stmt.number_of_rows < 0 {
    //     eprintln!("RUST: SQLFetch: NO_DATA");
    //     return sql::SqlReturn::NO_DATA.0;
    // }

    // eprintln!("RUST: SQLFetch: {:?}", stmt.number_of_rows);
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLGetData(
    statement_handle: sql::Handle,
    col_or_param_num: sql::USmallInt,
    _target_type: sql::SqlDataType,
    target_value_ptr: sql::Pointer,
    _buffer_length: sql::Len,
    _str_len_or_ind_ptr: *mut sql::Len,
) -> sql::RetCode {
    eprintln!("RUST: SQLGetData: {:?}", statement_handle);
    let stmt_ptr = statement_handle as *mut Statement;
    let stmt = stmt_ptr.as_mut().unwrap();
    match &mut stmt.state {
        StatementState::Fetched {
            reader: _,
            record_batch,
        } => {
            eprintln!("RUST: SQLGetData: fetched: {:?}", record_batch);
            let array_ref = record_batch.column((col_or_param_num - 1) as usize);
            // TODO: handle other types
            let array = array_ref.as_any().downcast_ref::<Int8Array>().unwrap();
            eprintln!("RUST: SQLGetData: array: {:?}", array);
            let value = array.value(0);
            std::ptr::write(target_value_ptr as *mut i8, value as i8);
            sql::SqlReturn::SUCCESS.0
        }
        _ => {
            eprintln!("RUST: SQLGetData: not fetched");
            sql::SqlReturn::ERROR.0
        }
    }
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLNumResultCols(
    _statement_handle: sql::Handle,
    _column_count_ptr: *mut sql::SmallInt,
) -> sql::SqlReturn {
    eprintln!("RUST: SQLNumResultCols");
    let int_ptr = _column_count_ptr as *mut i32;
    std::ptr::write(int_ptr, 1);
    sql::SqlReturn::SUCCESS
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SQLRowCount(
    statement_handle: sql::Handle,
    row_count_ptr: *mut sql::Len,
) -> sql::SqlReturn {
    eprintln!("RUST: SQLRowCount");
    let stmt_ptr = statement_handle as *mut Statement;
    let stmt = stmt_ptr.as_mut().unwrap();
    let row_count_ptr = row_count_ptr as *mut i32;
    match &mut stmt.state {
        StatementState::Executed { result } => {
            std::ptr::write(row_count_ptr, result.rows_affected as i32 as i32);
        }
        _ => {
            std::ptr::write(row_count_ptr, 0);
        }
    }
    sql::SqlReturn::SUCCESS
}
