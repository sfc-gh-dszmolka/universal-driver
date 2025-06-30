use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
}

#[derive(Serialize)]
pub struct SnowflakeQueryRequest {
    #[serde(rename = "sqlText")]
    pub sql_text: String,
    #[serde(rename = "asyncExec")]
    pub async_exec: bool,
    #[serde(rename = "sequenceId")]
    pub sequence_id: i64,
    #[serde(rename = "querySubmissionTime")]
    pub query_submission_time: i64,
    #[serde(rename = "parameters")]
    pub parameters: HashMap<String, String>,
    #[serde(rename = "queryContextDTO")]
    pub query_context_dto: HashMap<String, String>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct SnowflakeQueryResponse {
    pub data: SnowflakeQueryResponseData,
    pub code: Option<String>,
    pub message: Option<String>,
    pub success: bool,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct SnowflakeQueryResponseData {
    pub parameters: Vec<HashMap<String, serde_json::Value>>,
    pub rowtype: Vec<HashMap<String, serde_json::Value>>,
    #[serde(rename = "rowsetBase64")]
    pub rowset_base64: String,
    pub total: i64,
    pub returned: i64,
    #[serde(rename = "queryId")]
    pub query_id: String,
    #[serde(rename = "databaseProvider")]
    pub database_provider: Option<String>,
    #[serde(rename = "finalDatabaseName")]
    pub final_database_name: String,
    #[serde(rename = "finalSchemaName")]
    pub final_schema_name: String,
    #[serde(rename = "finalWarehouseName")]
    pub final_warehouse_name: String,
    #[serde(rename = "finalRoleName")]
    pub final_role_name: String,
    #[serde(rename = "numberOfBinds")]
    pub number_of_binds: i64,
    #[serde(rename = "arrayBindSupported")]
    pub array_bind_supported: bool,
    #[serde(rename = "statementTypeId")]
    pub statement_type_id: i64,
    pub version: i64,
    #[serde(rename = "sendResultTime")]
    pub send_result_time: i64,
    #[serde(rename = "queryResultFormat")]
    pub query_result_format: String,
    #[serde(rename = "queryContext")]
    pub query_context: HashMap<String, serde_json::Value>,
}
