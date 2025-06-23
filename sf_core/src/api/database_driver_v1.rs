use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use thrift::{Error, OrderedFloat};
use thrift::server::TProcessor;
use driver::{Database, Connection, Statement, Setting};
use handle_manager::{Handle, HandleManager};
use thrift_gen::database_driver_v1::{ArrowSchemaPtr, ConnectionHandle, DatabaseDriverSyncHandler, DatabaseDriverSyncProcessor, DatabaseHandle, DriverException, ExecuteResult, InfoCode, PartitionedResult, StatementHandle, StatusCode, TDatabaseDriverSyncClient};

impl From<Handle> for DatabaseHandle {
    fn from(handle: Handle) -> Self {
        DatabaseHandle {
            id: handle.id as i64,
            magic: handle.magic as i64,
        }
    }
}

impl Into<Handle> for DatabaseHandle {
    fn into(self) -> Handle {
        Handle {
            id: self.id as u64,
            magic: self.magic as u64,
        }
    }
}

impl From<Handle> for ConnectionHandle {
    fn from(handle: Handle) -> Self {
        ConnectionHandle {
            id: handle.id as i64,
            magic: handle.magic as i64,
        }
    }
}

impl Into<Handle> for ConnectionHandle {
    fn into(self) -> Handle {
        Handle {
            id: self.id as u64,
            magic: self.magic as u64,
        }
    }
}

impl From<Handle> for StatementHandle {
    fn from(handle: Handle) -> Self {
        StatementHandle {
            id: handle.id as i64,
            magic: handle.magic as i64,
        }
    }
}

impl Into<Handle> for StatementHandle {
    fn into(self) -> Handle {
        Handle {
            id: self.id as u64,
            magic: self.magic as u64,
        }
    }
}

pub struct DatabaseDriverV1 {
    db_handle_manager: HandleManager<Mutex<Database>>,
    conn_handle_manager: HandleManager<Mutex<Connection>>,
    stmt_handle_manager: HandleManager<Mutex<Statement>>,
}

impl DatabaseDriverV1 {
    pub fn new() -> DatabaseDriverV1 {
        DatabaseDriverV1 { 
            db_handle_manager: HandleManager::new(), 
            conn_handle_manager: HandleManager::new(),
            stmt_handle_manager: HandleManager::new()
        }
    }

    pub fn processor() -> Box<dyn TProcessor + Send + Sync> {
        Box::new(DatabaseDriverSyncProcessor::new(DatabaseDriverV1::new()))
    }
    
    pub fn database_set_option(&self, db_handle: DatabaseHandle, key: String, value: Setting) -> thrift::Result<()> {
        let handle = db_handle.into();
        match self.db_handle_manager.get_obj(handle) {
            Some(db_ptr) => {
                let mut db = db_ptr.lock().unwrap();
                db.settings.insert(key, value);
                Ok(())
            },
            None => {
                Err(Error::from(DriverException::new(String::from("Database handle not found"), StatusCode::INVALID_ARGUMENT, None, None, None)))
            }
        }
    }

    fn connection_set_option(&self, handle: ConnectionHandle, key: String, value: Setting) -> thrift::Result<()> {
        let handle = handle.into();
        match self.conn_handle_manager.get_obj(handle) {
            Some(conn_ptr) => {
                let mut conn = conn_ptr.lock().unwrap();
                conn.settings.insert(key, value);
                Ok(())
            },
            None => {
                Err(Error::from(DriverException::new(String::from("Connection handle not found"), StatusCode::INVALID_ARGUMENT, None, None, None)))
            }
        }
    }

    fn statement_set_option(&self, handle: StatementHandle, key: String, value: Setting) -> thrift::Result<()> {
        let handle = handle.into();
        match self.stmt_handle_manager.get_obj(handle) {
            Some(stmt_ptr) => {
                let mut stmt = stmt_ptr.lock().unwrap();
                stmt.settings.insert(key, value);
                Ok(())
            },
            None => {
                Err(Error::from(DriverException::new(String::from("Statement handle not found"), StatusCode::INVALID_ARGUMENT, None, None, None)))
            }
        }
    }

}
impl DatabaseDriverSyncHandler for DatabaseDriverV1 {
    fn handle_database_new(&self) -> thrift::Result<DatabaseHandle> {
        let handle = self.db_handle_manager.add_handle(Mutex::new(Database::new()));
        Ok(DatabaseHandle::from(handle))
    }

    fn handle_database_set_option_string(&self, db_handle: DatabaseHandle, key: String, value: String) -> thrift::Result<()> {
        self.database_set_option(db_handle, key, Setting::String(value))
    }

    fn handle_database_set_option_bytes(&self, db_handle: DatabaseHandle, key: String, value: Vec<u8>) -> thrift::Result<()> {
        self.database_set_option(db_handle, key, Setting::Bytes(value))
    }

    fn handle_database_set_option_int(&self, db_handle: DatabaseHandle, key: String, value: i64) -> thrift::Result<()> {
        self.database_set_option(db_handle, key, Setting::Int(value))
    }

    fn handle_database_set_option_double(&self, db_handle: DatabaseHandle, key: String, value: OrderedFloat<f64>) -> thrift::Result<()> {
        self.database_set_option(db_handle, key, Setting::Double(value.into_inner()))
    }

    fn handle_database_init(&self, db_handle: DatabaseHandle) -> thrift::Result<()> {
        let handle = db_handle.into();
        match self.db_handle_manager.get_obj(handle) {
            Some(db_ptr) => {
                Ok(())
            },
            None => Err(Error::from(DriverException::new(String::from("Database handle not found"), StatusCode::INVALID_ARGUMENT, None, None, None))),
        }
    }

    fn handle_database_release(&self, db_handle: DatabaseHandle) -> thrift::Result<()> {
        match self.db_handle_manager.delete_handle(db_handle.into()) {
            true => Ok(()),
            false => Err(Error::from(DriverException::new(String::from("Failed to release database handle"), StatusCode::INVALID_ARGUMENT, None, None, None))),
        }
    }


    fn handle_connection_new(&self) -> thrift::Result<ConnectionHandle> {
        let handle = self.conn_handle_manager.add_handle(Mutex::new(Connection::new()));
        Ok(ConnectionHandle::from(handle))
    }

    fn handle_connection_set_option_string(&self, conn_handle: ConnectionHandle, key: String, value: String) -> thrift::Result<()> {
        self.connection_set_option(conn_handle, key, Setting::String(value))
    }

    fn handle_connection_set_option_bytes(&self, conn_handle: ConnectionHandle, key: String, value: Vec<u8>) -> thrift::Result<()> {
        self.connection_set_option(conn_handle, key, Setting::Bytes(value))
    }

    fn handle_connection_set_option_int(&self, conn_handle: ConnectionHandle, key: String, value: i64) -> thrift::Result<()> {
        self.connection_set_option(conn_handle, key, Setting::Int(value))
    }

    fn handle_connection_set_option_double(&self, conn_handle: ConnectionHandle, key: String, value: OrderedFloat<f64>) -> thrift::Result<()> {
        self.connection_set_option(conn_handle, key, Setting::Double(value.into_inner()))
    }

    fn handle_connection_init(&self, conn_handle: ConnectionHandle, db_handle: String) -> thrift::Result<()> {
        let handle = conn_handle.into();
        match self.conn_handle_manager.get_obj(handle) {
            Some(conn_ptr) => {
                Ok(())
            },
            None => Err(Error::from(DriverException::new(String::from("Connection handle not found"), StatusCode::INVALID_ARGUMENT, None, None, None))),
        }
    }

    fn handle_connection_release(&self, conn_handle: ConnectionHandle) -> thrift::Result<()> {
        match self.conn_handle_manager.delete_handle(conn_handle.into()) {
            true => Ok(()),
            false => Err(DriverException::new(String::from("Failed to release connection handle"), StatusCode::INVALID_ARGUMENT, None, None, None).into()),
        }
    }

    fn handle_connection_get_info(&self, conn_handle: ConnectionHandle, info_codes: Vec<InfoCode>) -> thrift::Result<Vec<u8>> {
        todo!()
    }

    fn handle_connection_get_objects(&self, conn_handle: ConnectionHandle, depth: i32, catalog: String, db_schema: String, table_name: String, table_type: Vec<String>, column_name: String) -> thrift::Result<Vec<u8>> {
        todo!()
    }

    fn handle_connection_get_table_schema(&self, conn_handle: ConnectionHandle, catalog: String, db_schema: String, table_name: String) -> thrift::Result<Vec<u8>> {
        todo!()
    }

    fn handle_connection_get_table_types(&self, conn_handle: ConnectionHandle) -> thrift::Result<Vec<u8>> {
        todo!()
    }

    fn handle_connection_commit(&self, conn_handle: ConnectionHandle) -> thrift::Result<()> {
        todo!()
    }

    fn handle_connection_rollback(&self, conn_handle: ConnectionHandle) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_new(&self, conn_handle: ConnectionHandle) -> thrift::Result<StatementHandle> {
        let handle = conn_handle.into();
        match self.conn_handle_manager.get_obj(handle) {
            Some(_conn_ptr) => {
                let stmt = Mutex::new(Statement::new());
                let handle = self.stmt_handle_manager.add_handle(stmt);
                Ok(handle.into())
            },
            None => Err(Error::from(DriverException::new(
                String::from("Connection handle not found"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None
            ))),
        }
    }

    fn handle_statement_release(&self, stmt_handle: StatementHandle) -> thrift::Result<()> {
        match self.stmt_handle_manager.delete_handle(stmt_handle.into()) {
            true => Ok(()),
            false => Err(DriverException::new(
                String::from("Failed to release statement handle"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None
            ).into()),
        }
    }

    fn handle_statement_set_sql_query(&self, stmt_handle: StatementHandle, query: String) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_set_substrait_plan(&self, stmt_handle: StatementHandle, plan: Vec<u8>) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_prepare(&self, stmt_handle: StatementHandle) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_set_option_string(&self, stmt_handle: StatementHandle, key: String, value: String) -> thrift::Result<()> {
        self.statement_set_option(stmt_handle, key, Setting::String(value))
    }

    fn handle_statement_set_option_bytes(&self, stmt_handle: StatementHandle, key: String, value: Vec<u8>) -> thrift::Result<()> {
        self.statement_set_option(stmt_handle, key, Setting::Bytes(value))
    }

    fn handle_statement_set_option_int(&self, stmt_handle: StatementHandle, key: String, value: i64) -> thrift::Result<()> {
        self.statement_set_option(stmt_handle, key, Setting::Int(value))
    }

    fn handle_statement_set_option_double(&self, stmt_handle: StatementHandle, key: String, value: OrderedFloat<f64>) -> thrift::Result<()> {
        self.statement_set_option(stmt_handle, key, Setting::Double(value.into_inner()))
    }

    fn handle_statement_get_parameter_schema(&self, stmt_handle: StatementHandle) -> thrift::Result<ArrowSchemaPtr> {
        todo!()
    }

    fn handle_statement_bind(&self, stmt_handle: StatementHandle, values: Vec<u8>) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_bind_stream(&self, stmt_handle: StatementHandle, stream: Vec<u8>) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_execute_query(&self, stmt_handle: StatementHandle) -> thrift::Result<ExecuteResult> {
        todo!()
    }

    fn handle_statement_execute_partitions(&self, stmt_handle: StatementHandle) -> thrift::Result<PartitionedResult> {
        todo!()
    }

    fn handle_statement_read_partition(&self, stmt_handle: StatementHandle, partition_descriptor: Vec<u8>) -> thrift::Result<i64> {
        todo!()
    }
}