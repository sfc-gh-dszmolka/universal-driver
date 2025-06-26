mod data;
mod payload;

use crate::driver::{Connection, Setting, Settings};
use crate::rest::error::RestError;
use crate::rest::snowflake::data::{ClientInfo, LoginParameters};
use crate::rest::snowflake::payload::{
    ClientEnvironment, SnowflakeLoginData, SnowflakeLoginRequest, SnowflakeLoginResponse,
};
use reqwest;
use serde_json;
use std::collections::HashMap;
use std::sync::Mutex;
use tracing;

fn client_info(_conn: &Connection) -> Result<ClientInfo, RestError> {
    let client_info = ClientInfo {
        application: "PythonConnector".to_string(),
        version: "3.15.0".to_string(),
        os: "Darwin".to_string(),
        os_version: "macOS-15.5-arm64-arm-64bit".to_string(),
        ocsp_mode: Some("FAIL_OPEN".to_string()),
    };
    Ok(client_info)
}

fn get_login_parameters(conn: &Connection) -> Result<LoginParameters, RestError> {
    let params = LoginParameters {
        account_name: {
            tracing::debug!("Getting account name from connection settings");
            tracing::debug!("Connection settings: {:?}", conn.settings);
            if let Some(value) = conn.settings.get_string("account") {
                value
            } else {
                return Err(RestError::MissingParameter("account".to_string()));
            }
        },
        login_name: {
            if let Some(Setting::String(value)) = conn.settings.get("user") {
                value.clone()
            } else {
                return Err(RestError::MissingParameter("user".to_string()));
            }
        },
        password: {
            if let Some(Setting::String(value)) = conn.settings.get("password") {
                value.clone()
            } else {
                return Err(RestError::MissingParameter("password".to_string()));
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
                return Err(RestError::MissingParameter(
                    "server_url or account".to_string(),
                ));
            }
        },
    };
    Ok(params)
}

#[tracing::instrument(skip(conn_ptr), fields(account_name, login_name))]
pub async fn snowflake_login(
    conn_ptr: &std::sync::Arc<Mutex<Connection>>,
) -> Result<(), RestError> {
    tracing::info!("Starting Snowflake login process");

    // Extract required settings from connection
    tracing::debug!("Extracting connection settings");
    let (login_parameters, client_info) = {
        let conn = conn_ptr.lock().unwrap();
        (get_login_parameters(&conn)?, client_info(&conn)?)
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
    let response = client.execute(request).await.map_err(|e| {
        tracing::error!(error = %e, "HTTP request failed");
        RestError::Internal(format!("HTTP request failed: {}", e))
    })?;

    let status = response.status();
    tracing::debug!(status = %status, "Received login response");

    if !status.is_success() {
        let error_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        tracing::error!(status = %status, error_text = %error_text, "Login request failed");
        return Err(RestError::Internal(format!(
            "Login failed with status {}: {}",
            status, error_text
        )));
    }

    // Parse the response
    tracing::debug!("Parsing login response");
    let login_response: SnowflakeLoginResponse = response.json().await.map_err(|e| {
        tracing::error!(error = %e, "Failed to parse login response");
        RestError::Internal(format!("Failed to parse login response: {}", e))
    })?;

    if !login_response.success {
        let message = login_response
            .message
            .unwrap_or_else(|| "Unknown error".to_string());
        tracing::error!(message = %message, "Snowflake login failed");
        return Err(RestError::Status(status));
    }

    // Extract and store the session token
    tracing::debug!("Login successful, extracting session token");
    if let Some(data) = login_response.data {
        conn_ptr.lock().unwrap().session_token = data.token;
        tracing::info!("Snowflake login completed successfully");
        Ok(())
    } else {
        tracing::error!("Login response missing token data");
        return Err(RestError::Internal(
            "Login response missing token data".to_string(),
        ));
    }
}
