use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug)]
pub enum Setting {
    String(String),
    Bytes(Vec<u8>),
    Int(i64),
    Double(f64),
}

impl Setting {
    fn as_string(&self) -> Option<&String> {
        if let Setting::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn as_int(&self) -> Option<&i64> {
        if let Setting::Int(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn as_double(&self) -> Option<&f64> {
        if let Setting::Double(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn as_bytes(&self) -> Option<&Vec<u8>> {
        if let Setting::Bytes(value) = self {
            Some(value)
        } else {
            None
        }
    }
}

pub trait Settings {
    fn get(&self, key: &str) -> Option<Setting>;
    fn get_string(&self, key: &str) -> Option<String> {
        let setting = self.get(key)?;
        setting.as_string().cloned()
    }
    fn get_int(&self, key: &str) -> Option<i64> {
        let setting = self.get(key)?;
        setting.as_int().cloned()
    }
    fn get_double(&self, key: &str) -> Option<f64> {
        let setting = self.get(key)?;
        setting.as_double().cloned()
    }
    fn get_bytes(&self, key: &str) -> Option<Vec<u8>> {
        let setting = self.get(key)?;
        setting.as_bytes().cloned()
    }
    fn set(&mut self, key: &str, value: Setting);
    fn set_string(&mut self, key: &str, value: String) {
        self.set(key, Setting::String(value));
    }
    fn set_int(&mut self, key: &str, value: i64) {
        self.set(key, Setting::Int(value));
    }
    fn set_double(&mut self, key: &str, value: f64) {
        self.set(key, Setting::Double(value));
    }
    fn set_bytes(&mut self, key: &str, value: Vec<u8>) {
        self.set(key, Setting::Bytes(value));
    }
}

impl Settings for HashMap<String, Setting> {
    fn get(&self, key: &str) -> Option<Setting> {
        self.get(key).cloned()
    }

    fn set(&mut self, key: &str, value: Setting) {
        self.insert(key.to_string(), value);
    }
}

pub struct Connection {
    pub settings: HashMap<String, Setting>,
    pub session_token: Option<String>,
}

impl Default for Connection {
    fn default() -> Self {
        Self::new()
    }
}

impl Connection {
    pub fn new() -> Self {
        Connection {
            settings: HashMap::new(),
            session_token: None,
        }
    }
}

pub struct Database {
    pub settings: HashMap<String, Setting>,
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    pub fn new() -> Self {
        Database {
            settings: HashMap::new(),
        }
    }
}

pub enum StatementState {
    Initialized,
    Executed,
}

pub struct Statement {
    pub state: StatementState,
    pub settings: HashMap<String, Setting>,
    pub query: Option<String>,
    pub conn: Arc<Mutex<Connection>>,
}

impl Statement {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Statement {
            settings: HashMap::new(),
            state: StatementState::Initialized,
            query: None,
            conn,
        }
    }
}

// Snowflake login request structures
#[derive(Serialize)]
pub struct SnowflakeLoginRequest {
    pub data: SnowflakeLoginData,
}

#[derive(Serialize)]
pub struct SnowflakeLoginData {
    #[serde(rename = "LOGIN_NAME")]
    pub login_name: String,
    #[serde(rename = "PASSWORD", skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(rename = "ACCOUNT_NAME")]
    pub account_name: String,
    #[serde(rename = "CLIENT_APP_ID")]
    pub client_app_id: String,
    #[serde(rename = "CLIENT_APP_VERSION")]
    pub client_app_version: String,
    #[serde(rename = "AUTHENTICATOR", skip_serializing_if = "Option::is_none")]
    pub authenticator: Option<String>,
    #[serde(
        rename = "BROWSER_MODE_REDIRECT_PORT",
        skip_serializing_if = "Option::is_none"
    )]
    pub browser_mode_redirect_port: Option<String>,
    #[serde(rename = "PROOF_KEY", skip_serializing_if = "Option::is_none")]
    pub proof_key: Option<String>,
    #[serde(rename = "CLIENT_ENVIRONMENT")]
    pub client_environment: ClientEnvironment,
    #[serde(rename = "SESSION_PARAMETERS", skip_serializing_if = "Option::is_none")]
    pub session_parameters: Option<HashMap<String, serde_json::Value>>,
    #[serde(rename = "DATABASE_NAME", skip_serializing_if = "Option::is_none")]
    pub database_name: Option<String>,
    #[serde(rename = "SCHEMA_NAME", skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    #[serde(rename = "WAREHOUSE_NAME", skip_serializing_if = "Option::is_none")]
    pub warehouse_name: Option<String>,
    #[serde(rename = "ROLE_NAME", skip_serializing_if = "Option::is_none")]
    pub role_name: Option<String>,
    #[serde(rename = "TOKEN", skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Serialize)]
pub struct ClientEnvironment {
    #[serde(rename = "APPLICATION")]
    pub application: String,
    #[serde(rename = "OS")]
    pub os: String,
    #[serde(rename = "OS_VERSION")]
    pub os_version: String,
    #[serde(rename = "OCSP_MODE", skip_serializing_if = "Option::is_none")]
    pub ocsp_mode: Option<String>,
    #[serde(rename = "PYTHON_VERSION", skip_serializing_if = "Option::is_none")]
    pub python_version: Option<String>,
    #[serde(rename = "PYTHON_RUNTIME", skip_serializing_if = "Option::is_none")]
    pub python_runtime: Option<String>,
    #[serde(rename = "PYTHON_COMPILER", skip_serializing_if = "Option::is_none")]
    pub python_compiler: Option<String>,
}

#[derive(Deserialize)]
pub struct SnowflakeLoginResponse {
    pub success: bool,
    pub message: Option<String>,
    pub data: Option<SnowflakeLoginResponseData>,
}

#[derive(Deserialize)]
pub struct SnowflakeLoginResponseData {
    pub token: Option<String>,
    #[serde(rename = "masterToken")]
    pub master_token: Option<String>,
    #[serde(rename = "validityInSeconds")]
    pub validity_in_seconds: Option<i64>,
}
