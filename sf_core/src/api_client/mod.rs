use crate::c_api::SfCoreApi;
use thrift_gen::database_driver_v1::{DatabaseDriverSyncClient, TDatabaseDriverSyncClient};

mod handle_transport;

pub fn new_database_driver_v1_client() -> Box<dyn TDatabaseDriverSyncClient + Send> {
    let span = tracing::info_span!(target: "database_driver", "DatabaseDriverV1Client");
    let _guard = span.enter();
    let api_handle = crate::c_api::sf_core_api_init(SfCoreApi::DatabaseDriverApiV1);
    tracing::debug!(api_handle=?api_handle, "Api handle created");
    let input_handle_transport = handle_transport::HandleTransport::new(api_handle);
    let output_handle_transport = handle_transport::HandleTransport::new(api_handle);
    let input_protocol = thrift::protocol::TCompactInputProtocol::new(input_handle_transport);
    let output_protocol = thrift::protocol::TCompactOutputProtocol::new(output_handle_transport);
    Box::new(
        DatabaseDriverSyncClient::new(
            input_protocol,
            output_protocol, 
        )
    )
}
