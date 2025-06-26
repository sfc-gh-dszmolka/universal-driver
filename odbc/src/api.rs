// ODBC API implementation will go here.

use odbc_sys as sql;
use sf_core::{
    api_client,
    handle_manager::{Handle, HandleManager},
    thrift_gen::database_driver_v1::{
        ConnectionHandle as TConnectionHandle, DatabaseHandle as TDatabaseHandle,
        TDatabaseDriverSyncClient,
    },
};
use std::{slice, str, sync::Mutex};

struct Environment {}

struct Connection {
    client: Option<Box<dyn TDatabaseDriverSyncClient + Send>>,
    db_handle: Option<TDatabaseHandle>,
    conn_handle: Option<TConnectionHandle>,
}

struct Statement {
}

lazy_static! {
    static ref ENV_HANDLE_MANAGER: HandleManager<Mutex<Environment>> = HandleManager::new();
    static ref DBC_HANDLE_MANAGER: HandleManager<Mutex<Connection>> = HandleManager::new();
    static ref STMT_HANDLE_MANAGER: HandleManager<Mutex<Statement>> = HandleManager::new();
}
/// # Safety
/// This function is called by the ODBC driver manager.
#[no_mangle]
pub unsafe extern "C" fn SQLAllocHandle(
    handle_type: sql::HandleType,
    input_handle: sql::Handle,
    output_handle: *mut sql::Handle,
) -> sql::SqlReturn {
    match handle_type {
        sql::HandleType::Env => {
            let env = Mutex::new(Environment {});
            let handle = ENV_HANDLE_MANAGER.add_handle(env);
            let handle_ptr = Box::into_raw(Box::new(handle));
            *output_handle = handle_ptr as sql::Handle;
            sql::SqlReturn::SUCCESS
        }
        sql::HandleType::Dbc => {
            if input_handle.is_null() {
                return sql::SqlReturn::INVALID_HANDLE;
            }
            let env_handle = *(input_handle as *mut Handle);
            if ENV_HANDLE_MANAGER.get_obj(env_handle).is_some() {
                let conn = Mutex::new(Connection {
                    client: None,
                    db_handle: None,
                    conn_handle: None,
                });
                let dbc_handle = DBC_HANDLE_MANAGER.add_handle(conn);
                let handle_ptr = Box::into_raw(Box::new(dbc_handle));
                *output_handle = handle_ptr as sql::Handle;
                sql::SqlReturn::SUCCESS
            } else {
                sql::SqlReturn::INVALID_HANDLE
            }
        }
        sql::HandleType::Stmt => {
            if input_handle.is_null() {
                return sql::SqlReturn::INVALID_HANDLE;
            }
            let dbc_handle = *(input_handle as *mut Handle);
            if DBC_HANDLE_MANAGER.get_obj(dbc_handle).is_some() {
                let stmt = Mutex::new(Statement {});
                let new_stmt_handle = STMT_HANDLE_MANAGER.add_handle(stmt);
                let handle_ptr = Box::into_raw(Box::new(new_stmt_handle));
                *output_handle = handle_ptr as sql::Handle;
                sql::SqlReturn::SUCCESS
            } else {
                sql::SqlReturn::INVALID_HANDLE
            }
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
#[no_mangle]
pub unsafe extern "C" fn SQLFreeHandle(
    handle_type: sql::HandleType,
    handle: sql::Handle,
) -> sql::SqlReturn {
    if handle.is_null() {
        return sql::SqlReturn::INVALID_HANDLE;
    }

    let odbc_handle = *(handle as *mut Handle);

    match handle_type {
        sql::HandleType::Env => {
            if ENV_HANDLE_MANAGER.delete_handle(odbc_handle) {
                sql::SqlReturn::SUCCESS
            } else {
                sql::SqlReturn::INVALID_HANDLE
            }
        }
        sql::HandleType::Dbc => {
            if DBC_HANDLE_MANAGER.delete_handle(odbc_handle) {
                sql::SqlReturn::SUCCESS
            } else {
                sql::SqlReturn::INVALID_HANDLE
            }
        }
        sql::HandleType::Stmt => {
            if STMT_HANDLE_MANAGER.delete_handle(odbc_handle) {
                sql::SqlReturn::SUCCESS
            } else {
                sql::SqlReturn::INVALID_HANDLE
            }
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
#[no_mangle]
pub unsafe extern "C" fn SQLConnect(
    connection_handle: sql::Handle,
    server_name: *const sql::Char,
    name_length1: sql::SmallInt,
    _user_name: *const sql::Char,
    _name_length2: sql::SmallInt,
    _authentication: *const sql::Char,
    _name_length3: sql::SmallInt,
) -> sql::SqlReturn {
    let dbc_handle = *(connection_handle as *mut Handle);

    let conn_mutex = match DBC_HANDLE_MANAGER.get_obj(dbc_handle) {
        Some(c) => c,
        None => return sql::SqlReturn::INVALID_HANDLE,
    };

    let mut conn = conn_mutex.lock().unwrap();

    let mut client = api_client::new_database_driver_v1_client();

    let db_handle = match client.database_new() {
        Ok(h) => h,
        Err(_) => return sql::SqlReturn::ERROR,
    };

    let server_name_str =
        str::from_utf8(slice::from_raw_parts(server_name, name_length1 as usize)).unwrap();

    if client
        .database_set_option_string(
            db_handle.clone(),
            "uri".to_string(),
            server_name_str.to_string(),
        )
        .is_err()
    {
        return sql::SqlReturn::ERROR;
    }

    // You can also set username and password if the driver supports it
    // let user_name_str = ...
    // let authentication_str = ...
    // client.database_set_option_string(db_handle.clone(), "username", user_name_str.to_string())
    // client.database_set_option_string(db_handle.clone(), "password", authentication_str.to_string())

    if client.database_init(db_handle.clone()).is_err() {
        return sql::SqlReturn::ERROR;
    }

    let conn_handle = match client.connection_new() {
        Ok(h) => h,
        Err(_) => return sql::SqlReturn::ERROR,
    };

    // The thrift definition for connection_init takes a string for db_handle,
    // but the current server implementation doesn't use it.
    // We will pass an empty string.
    if client
        .connection_init(conn_handle.clone(), "".to_string())
        .is_err()
    {
        return sql::SqlReturn::ERROR;
    }

    conn.client = Some(client);
    conn.db_handle = Some(db_handle);
    conn.conn_handle = Some(conn_handle);

    sql::SqlReturn::SUCCESS
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[no_mangle]
pub unsafe extern "C" fn SQLDisconnect(connection_handle: sql::Handle) -> sql::SqlReturn {
    let dbc_handle = *(connection_handle as *mut Handle);

    let conn_mutex = match DBC_HANDLE_MANAGER.get_obj(dbc_handle) {
        Some(c) => c,
        None => return sql::SqlReturn::INVALID_HANDLE,
    };

    let mut conn = conn_mutex.lock().unwrap();

    if let Some(mut client) = conn.client.take() {
        if let Some(conn_handle) = conn.conn_handle.take() {
            if client.connection_release(conn_handle).is_err() {
                return sql::SqlReturn::ERROR;
            }
        }
        if let Some(db_handle) = conn.db_handle.take() {
            if client.database_release(db_handle).is_err() {
                return sql::SqlReturn::ERROR;
            }
        }
    }

    sql::SqlReturn::SUCCESS
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[no_mangle]
pub unsafe extern "C" fn SQLExecDirect(
    _statement_handle: sql::Handle,
    _statement_text: *const sql::Char,
    _text_length: sql::Integer,
) -> sql::SqlReturn {
    sql::SqlReturn::ERROR
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[no_mangle]
pub unsafe extern "C" fn SQLFetch(_statement_handle: sql::Handle) -> sql::SqlReturn {
    sql::SqlReturn::NO_DATA
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[no_mangle]
pub unsafe extern "C" fn SQLGetData(
    _statement_handle: sql::Handle,
    _col_or_param_num: sql::USmallInt,
    _target_type: sql::SqlDataType,
    _target_value_ptr: sql::Pointer,
    _buffer_length: sql::Len,
    _str_len_or_ind_ptr: *mut sql::Len,
) -> sql::SqlReturn {
    sql::SqlReturn::ERROR
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[no_mangle]
pub unsafe extern "C" fn SQLNumResultCols(
    _statement_handle: sql::Handle,
    _column_count_ptr: *mut sql::SmallInt,
) -> sql::SqlReturn {
    sql::SqlReturn::ERROR
}

/// # Safety
/// This function is called by the ODBC driver manager.
#[no_mangle]
pub unsafe extern "C" fn SQLRowCount(
    _statement_handle: sql::Handle,
    _row_count_ptr: *mut sql::Len,
) -> sql::SqlReturn {
    sql::SqlReturn::ERROR
}
