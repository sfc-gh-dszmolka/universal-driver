use crate::driver::{
    ClientEnvironment, Connection, Database, Setting, Settings, SnowflakeLoginData,
    SnowflakeLoginRequest, SnowflakeLoginResponse, Statement,
};
use crate::handle_manager::{Handle, HandleManager};
use crate::thrift_gen::database_driver_v1::{
    ArrowSchemaPtr, ConnectionHandle, DatabaseDriverSyncHandler, DatabaseDriverSyncProcessor,
    DatabaseHandle, DriverException, ExecuteResult, InfoCode, PartitionedResult, StatementHandle,
    StatusCode,
};
use std::collections::HashMap;
use std::sync::Mutex;
use thrift::server::TProcessor;
use thrift::{Error, OrderedFloat};

use crate::driver::StatementState;
use crate::thrift_gen::database_driver_v1::ArrowArrayStreamPtr;

struct LoginParameters {
    account_name: String,
    login_name: String,
    password: String,
    server_url: String,
}

struct ClientInfo {
    application: String,
    version: String,
    os: String,
    os_version: String,
    ocsp_mode: Option<String>,
}

impl From<Handle> for DatabaseHandle {
    fn from(handle: Handle) -> Self {
        DatabaseHandle {
            id: handle.id as i64,
            magic: handle.magic as i64,
        }
    }
}

impl From<DatabaseHandle> for Handle {
    fn from(val: DatabaseHandle) -> Self {
        Handle {
            id: val.id as u64,
            magic: val.magic as u64,
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

impl From<ConnectionHandle> for Handle {
    fn from(val: ConnectionHandle) -> Self {
        Handle {
            id: val.id as u64,
            magic: val.magic as u64,
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

impl From<StatementHandle> for Handle {
    fn from(val: StatementHandle) -> Self {
        Handle {
            id: val.id as u64,
            magic: val.magic as u64,
        }
    }
}

pub struct DatabaseDriverV1 {
    db_handle_manager: HandleManager<Mutex<Database>>,
    conn_handle_manager: HandleManager<Mutex<Connection>>,
    stmt_handle_manager: HandleManager<Mutex<Statement>>,
}

impl Default for DatabaseDriverV1 {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseDriverV1 {
    pub fn new() -> DatabaseDriverV1 {
        DatabaseDriverV1 {
            db_handle_manager: HandleManager::new(),
            conn_handle_manager: HandleManager::new(),
            stmt_handle_manager: HandleManager::new(),
        }
    }

    pub fn processor() -> Box<dyn TProcessor + Send + Sync> {
        Box::new(DatabaseDriverSyncProcessor::new(DatabaseDriverV1::new()))
    }

    pub fn database_set_option(
        &self,
        db_handle: DatabaseHandle,
        key: String,
        value: Setting,
    ) -> thrift::Result<()> {
        let handle = db_handle.into();
        match self.db_handle_manager.get_obj(handle) {
            Some(db_ptr) => {
                let mut db = db_ptr.lock().unwrap();
                db.settings.insert(key, value);
                Ok(())
            }
            None => Err(Error::from(DriverException::new(
                String::from("Database handle not found"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None,
            ))),
        }
    }

    fn connection_set_option(
        &self,
        handle: ConnectionHandle,
        key: String,
        value: Setting,
    ) -> thrift::Result<()> {
        let handle = handle.into();
        match self.conn_handle_manager.get_obj(handle) {
            Some(conn_ptr) => {
                let mut conn = conn_ptr.lock().unwrap();
                conn.settings.insert(key, value);
                Ok(())
            }
            None => Err(Error::from(DriverException::new(
                String::from("Connection handle not found"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None,
            ))),
        }
    }

    fn statement_set_option(
        &self,
        handle: StatementHandle,
        key: String,
        value: Setting,
    ) -> thrift::Result<()> {
        let handle = handle.into();
        match self.stmt_handle_manager.get_obj(handle) {
            Some(stmt_ptr) => {
                let mut stmt = stmt_ptr.lock().unwrap();
                stmt.settings.insert(key, value);
                Ok(())
            }
            None => Err(Error::from(DriverException::new(
                String::from("Statement handle not found"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None,
            ))),
        }
    }

    fn client_info(&self, _conn: &Connection) -> Result<ClientInfo, Error> {
        let client_info = ClientInfo {
            application: "PythonConnector".to_string(),
            version: "3.15.0".to_string(),
            os: "Darwin".to_string(),
            os_version: "macOS-15.5-arm64-arm-64bit".to_string(),
            ocsp_mode: Some("FAIL_OPEN".to_string()),
        };
        Ok(client_info)
    }

    fn get_login_parameters(&self, conn: &Connection) -> Result<LoginParameters, Error> {
        let params = LoginParameters {
            account_name: {
                tracing::debug!("Getting account name from connection settings");
                tracing::debug!("Connection settings: {:?}", conn.settings);
                if let Some(value) = conn.settings.get_string("account") {
                    value
                } else {
                    return Err(Error::from(DriverException::new(
                        "Account name not found".to_string(),
                        StatusCode::INVALID_ARGUMENT,
                        None,
                        None,
                        None,
                    )));
                }
            },
            login_name: {
                if let Some(Setting::String(value)) = conn.settings.get("user") {
                    value.clone()
                } else {
                    return Err(Error::from(DriverException::new(
                        "User not found".to_string(),
                        StatusCode::INVALID_ARGUMENT,
                        None,
                        None,
                        None,
                    )));
                }
            },
            password: {
                if let Some(Setting::String(value)) = conn.settings.get("password") {
                    value.clone()
                } else {
                    return Err(Error::from(DriverException::new(
                        "Password not found".to_string(),
                        StatusCode::INVALID_ARGUMENT,
                        None,
                        None,
                        None,
                    )));
                }
            },
            server_url: {
                if let Some(Setting::String(value)) = conn.settings.get("server_url") {
                    value.clone()
                } else if let Some(Setting::String(account_name)) = conn.settings.get("account") {
                    let protocol = conn
                        .settings
                        .get_string("protocol")
                        .unwrap_or("https".to_string());
                    if protocol != "https" && protocol != "http" {
                        tracing::warn!(
                            "Unexpected protocol specified during server url construction: {}",
                            protocol
                        );
                    }
                    format!("{}://{}.snowflakecomputing.com", protocol, account_name)
                } else {
                    return Err(Error::from(DriverException::new(
                        "Server URL not found".to_string(),
                        StatusCode::INVALID_ARGUMENT,
                        None,
                        None,
                        None,
                    )));
                }
            },
        };
        Ok(params)
    }

    #[tracing::instrument(skip(self, conn_ptr), fields(account_name, login_name))]
    async fn snowflake_login(
        &self,
        conn_ptr: std::sync::Arc<Mutex<Connection>>,
    ) -> thrift::Result<()> {
        tracing::info!("Starting Snowflake login process");

        // Extract required settings from connection - scope the lock to avoid holding across await
        let (login_parameters, client_info) = {
            let conn = conn_ptr.lock().unwrap();

            // Extract required settings from connection
            tracing::debug!("Extracting connection settings");
            let login_parameters = self.get_login_parameters(&conn)?;
            let client_info = self.client_info(&conn)?;

            (login_parameters, client_info)
        };

        // Record key fields in the span
        tracing::Span::current().record("account_name", &login_parameters.account_name);
        tracing::Span::current().record("login_name", &login_parameters.login_name);

        // Optional settings
        tracing::debug!(
            account_name = %login_parameters.account_name,
            login_name = %login_parameters.login_name,
            server_url = %login_parameters.server_url,
            "Extracted connection settings"
        );

        // Build the login request
        let login_request = SnowflakeLoginRequest {
            data: SnowflakeLoginData {
                client_app_id: client_info.application.clone(),
                client_app_version: client_info.version.clone(),
                account_name: login_parameters.account_name,
                login_name: login_parameters.login_name,
                browser_mode_redirect_port: None,
                proof_key: None,
                client_environment: ClientEnvironment {
                    application: client_info.application.clone(),
                    os: client_info.os.clone(),
                    os_version: client_info.os_version.clone(),
                    ocsp_mode: client_info.ocsp_mode,
                    python_version: Some("3.11.6".to_string()),
                    python_runtime: Some("CPython".to_string()),
                    python_compiler: Some("Clang 13.0.0 (clang-1300.0.29.30)".to_string()),
                },
                password: Some(login_parameters.password),
                session_parameters: Some(HashMap::new()),
                authenticator: Some("snowflake".to_string()),
                database_name: None,
                schema_name: None,
                warehouse_name: None,
                role_name: None,
                token: None,
            },
        };

        tracing::debug!(
            "Login request: {}",
            serde_json::to_string_pretty(&login_request).unwrap()
        );

        // Create HTTP client
        tracing::debug!("Creating HTTP client and preparing login request");
        let client = reqwest::Client::new();
        let login_url = format!("{}/session/v1/login-request", login_parameters.server_url);

        tracing::info!(login_url = %login_url, "Making Snowflake login request");
        let request = client
            .post(&login_url)
            .json(&login_request)
            .header("accept", "application/snowflake")
            .header(
                "User-Agent",
                format!(
                    "{}/{} ({}) CPython/3.11.6",
                    client_info.application,
                    client_info.version.clone(),
                    client_info.os.clone()
                ),
            )
            .header("Authorization", "Snowflake Token=\"None\"");
        let request = request.build().unwrap();
        tracing::info!("Request url: {:?}", request.url());
        tracing::info!("Request body: {:?}", request.body().unwrap());
        tracing::info!("Request method: {:?}", request.method());
        tracing::info!("Request version: {:?}", request.version());
        tracing::info!("Request timeout: {:?}", request.timeout());
        tracing::info!("Request headers: {:?}", request.headers());
        // Make the login request
        let response = client.execute(request).await.map_err(|e| {
            tracing::error!(error = %e, "HTTP request failed");
            Error::from(DriverException::new(
                format!("HTTP request failed: {}", e),
                StatusCode::IO,
                None,
                None,
                None,
            ))
        })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received login response");

        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            tracing::error!(status = %status, error_text = %error_text, "Login request failed");
            return Err(Error::from(DriverException::new(
                format!("Login failed with status {}: {}", status, error_text),
                StatusCode::UNAUTHENTICATED,
                None,
                None,
                None,
            )));
        }

        // Parse the response
        tracing::debug!("Parsing login response");
        let login_response: SnowflakeLoginResponse = response.json().await.map_err(|e| {
            tracing::error!(error = %e, "Failed to parse login response");
            Error::from(DriverException::new(
                format!("Failed to parse login response: {}", e),
                StatusCode::INVALID_DATA,
                None,
                None,
                None,
            ))
        })?;

        if !login_response.success {
            let message = login_response
                .message
                .unwrap_or_else(|| "Unknown error".to_string());
            tracing::error!(message = %message, "Snowflake login failed");
            return Err(Error::from(DriverException::new(
                format!("Login failed: {}", message),
                StatusCode::UNAUTHENTICATED,
                None,
                None,
                None,
            )));
        }

        // Extract and store the session token
        tracing::debug!("Login successful, extracting session token");
        if let Some(data) = login_response.data {
            let mut conn = conn_ptr.lock().unwrap();
            conn.session_token = data.token;
            tracing::info!("Snowflake login completed successfully");
            Ok(())
        } else {
            tracing::error!("Login response missing token data");
            Err(Error::from(DriverException::new(
                "Login response missing token data".to_string(),
                StatusCode::INVALID_DATA,
                None,
                None,
                None,
            )))
        }
    }
}

impl DatabaseDriverSyncHandler for DatabaseDriverV1 {
    fn handle_database_new(&self) -> thrift::Result<DatabaseHandle> {
        let handle = self
            .db_handle_manager
            .add_handle(Mutex::new(Database::new()));
        Ok(DatabaseHandle::from(handle))
    }

    fn handle_database_set_option_string(
        &self,
        db_handle: DatabaseHandle,
        key: String,
        value: String,
    ) -> thrift::Result<()> {
        self.database_set_option(db_handle, key, Setting::String(value))
    }

    fn handle_database_set_option_bytes(
        &self,
        db_handle: DatabaseHandle,
        key: String,
        value: Vec<u8>,
    ) -> thrift::Result<()> {
        self.database_set_option(db_handle, key, Setting::Bytes(value))
    }

    fn handle_database_set_option_int(
        &self,
        db_handle: DatabaseHandle,
        key: String,
        value: i64,
    ) -> thrift::Result<()> {
        self.database_set_option(db_handle, key, Setting::Int(value))
    }

    fn handle_database_set_option_double(
        &self,
        db_handle: DatabaseHandle,
        key: String,
        value: OrderedFloat<f64>,
    ) -> thrift::Result<()> {
        self.database_set_option(db_handle, key, Setting::Double(value.into_inner()))
    }

    fn handle_database_init(&self, db_handle: DatabaseHandle) -> thrift::Result<()> {
        let handle = db_handle.into();
        match self.db_handle_manager.get_obj(handle) {
            Some(_db_ptr) => Ok(()),
            None => Err(Error::from(DriverException::new(
                String::from("Database handle not found"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None,
            ))),
        }
    }

    fn handle_database_release(&self, db_handle: DatabaseHandle) -> thrift::Result<()> {
        match self.db_handle_manager.delete_handle(db_handle.into()) {
            true => Ok(()),
            false => Err(Error::from(DriverException::new(
                String::from("Failed to release database handle"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None,
            ))),
        }
    }

    fn handle_connection_new(&self) -> thrift::Result<ConnectionHandle> {
        let handle = self
            .conn_handle_manager
            .add_handle(Mutex::new(Connection::new()));
        Ok(ConnectionHandle::from(handle))
    }

    fn handle_connection_set_option_string(
        &self,
        conn_handle: ConnectionHandle,
        key: String,
        value: String,
    ) -> thrift::Result<()> {
        self.connection_set_option(conn_handle, key, Setting::String(value))
    }

    fn handle_connection_set_option_bytes(
        &self,
        conn_handle: ConnectionHandle,
        key: String,
        value: Vec<u8>,
    ) -> thrift::Result<()> {
        self.connection_set_option(conn_handle, key, Setting::Bytes(value))
    }

    fn handle_connection_set_option_int(
        &self,
        conn_handle: ConnectionHandle,
        key: String,
        value: i64,
    ) -> thrift::Result<()> {
        self.connection_set_option(conn_handle, key, Setting::Int(value))
    }

    fn handle_connection_set_option_double(
        &self,
        conn_handle: ConnectionHandle,
        key: String,
        value: OrderedFloat<f64>,
    ) -> thrift::Result<()> {
        self.connection_set_option(conn_handle, key, Setting::Double(value.into_inner()))
    }

    fn handle_connection_init(
        &self,
        conn_handle: ConnectionHandle,
        _db_handle: String,
    ) -> thrift::Result<()> {
        let handle = conn_handle.into();
        match self.conn_handle_manager.get_obj(handle) {
            Some(conn_ptr) => {
                // Create a blocking runtime for the login process
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    Error::from(DriverException::new(
                        format!("Failed to create runtime: {}", e),
                        StatusCode::UNKNOWN,
                        None,
                        None,
                        None,
                    ))
                })?;

                let login_result =
                    rt.block_on(async { self.snowflake_login(conn_ptr.clone()).await });

                match login_result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            None => Err(Error::from(DriverException::new(
                String::from("Connection handle not found"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None,
            ))),
        }
    }

    fn handle_connection_release(&self, conn_handle: ConnectionHandle) -> thrift::Result<()> {
        match self.conn_handle_manager.delete_handle(conn_handle.into()) {
            true => Ok(()),
            false => Err(DriverException::new(
                String::from("Failed to release connection handle"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None,
            )
            .into()),
        }
    }

    fn handle_connection_get_info(
        &self,
        _conn_handle: ConnectionHandle,
        _info_codes: Vec<InfoCode>,
    ) -> thrift::Result<Vec<u8>> {
        todo!()
    }

    fn handle_connection_get_objects(
        &self,
        _conn_handle: ConnectionHandle,
        _depth: i32,
        _catalog: String,
        _db_schema: String,
        _table_name: String,
        _table_type: Vec<String>,
        _column_name: String,
    ) -> thrift::Result<Vec<u8>> {
        todo!()
    }

    fn handle_connection_get_table_schema(
        &self,
        _conn_handle: ConnectionHandle,
        _catalog: String,
        _db_schema: String,
        _table_name: String,
    ) -> thrift::Result<Vec<u8>> {
        todo!()
    }

    fn handle_connection_get_table_types(
        &self,
        _conn_handle: ConnectionHandle,
    ) -> thrift::Result<Vec<u8>> {
        todo!()
    }

    fn handle_connection_commit(&self, _conn_handle: ConnectionHandle) -> thrift::Result<()> {
        todo!()
    }

    fn handle_connection_rollback(&self, _conn_handle: ConnectionHandle) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_new(
        &self,
        conn_handle: ConnectionHandle,
    ) -> thrift::Result<StatementHandle> {
        let handle = conn_handle.into();
        match self.conn_handle_manager.get_obj(handle) {
            Some(conn_ptr) => {
                let stmt = Mutex::new(Statement::new(conn_ptr));
                let handle = self.stmt_handle_manager.add_handle(stmt);
                Ok(handle.into())
            }
            None => Err(Error::from(DriverException::new(
                String::from("Connection handle not found"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None,
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
                None,
            )
            .into()),
        }
    }

    fn handle_statement_set_sql_query(
        &self,
        stmt_handle: StatementHandle,
        query: String,
    ) -> thrift::Result<()> {
        let handle = stmt_handle.into();
        match self.stmt_handle_manager.get_obj(handle) {
            Some(stmt_ptr) => {
                let mut stmt = stmt_ptr.lock().unwrap();
                stmt.query = Some(query);
                Ok(())
            }
            None => Err(Error::from(DriverException::new(
                String::from("Statement handle not found"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None,
            ))),
        }
    }

    fn handle_statement_set_substrait_plan(
        &self,
        _stmt_handle: StatementHandle,
        _plan: Vec<u8>,
    ) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_prepare(&self, _stmt_handle: StatementHandle) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_set_option_string(
        &self,
        stmt_handle: StatementHandle,
        key: String,
        value: String,
    ) -> thrift::Result<()> {
        self.statement_set_option(stmt_handle, key, Setting::String(value))
    }

    fn handle_statement_set_option_bytes(
        &self,
        stmt_handle: StatementHandle,
        key: String,
        value: Vec<u8>,
    ) -> thrift::Result<()> {
        self.statement_set_option(stmt_handle, key, Setting::Bytes(value))
    }

    fn handle_statement_set_option_int(
        &self,
        stmt_handle: StatementHandle,
        key: String,
        value: i64,
    ) -> thrift::Result<()> {
        self.statement_set_option(stmt_handle, key, Setting::Int(value))
    }

    fn handle_statement_set_option_double(
        &self,
        stmt_handle: StatementHandle,
        key: String,
        value: OrderedFloat<f64>,
    ) -> thrift::Result<()> {
        self.statement_set_option(stmt_handle, key, Setting::Double(value.into_inner()))
    }

    fn handle_statement_get_parameter_schema(
        &self,
        _stmt_handle: StatementHandle,
    ) -> thrift::Result<ArrowSchemaPtr> {
        todo!()
    }

    fn handle_statement_bind(
        &self,
        _stmt_handle: StatementHandle,
        _values: Vec<u8>,
    ) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_bind_stream(
        &self,
        _stmt_handle: StatementHandle,
        _stream: Vec<u8>,
    ) -> thrift::Result<()> {
        todo!()
    }

    fn handle_statement_execute_query(
        &self,
        stmt_handle: StatementHandle,
    ) -> thrift::Result<ExecuteResult> {
        let handle = stmt_handle.into();
        match self.stmt_handle_manager.get_obj(handle) {
            Some(stmt_ptr) => {
                let mut stmt = stmt_ptr.lock().unwrap();
                stmt.state = StatementState::Executed;
                Ok(ExecuteResult::new(Box::new(ArrowArrayStreamPtr::new(0)), 0))
            }
            None => Err(Error::from(DriverException::new(
                String::from("Statement handle not found"),
                StatusCode::INVALID_ARGUMENT,
                None,
                None,
                None,
            ))),
        }
    }

    fn handle_statement_execute_partitions(
        &self,
        _stmt_handle: StatementHandle,
    ) -> thrift::Result<PartitionedResult> {
        todo!()
    }

    fn handle_statement_read_partition(
        &self,
        _stmt_handle: StatementHandle,
        _partition_descriptor: Vec<u8>,
    ) -> thrift::Result<i64> {
        todo!()
    }
}
