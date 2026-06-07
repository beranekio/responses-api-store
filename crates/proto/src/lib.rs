pub mod v1 {
    tonic::include_proto!("responsesapistore.v1");
}

pub use v1::responses_api_store_client::ResponsesApiStoreClient;
pub use v1::responses_api_store_server::{ResponsesApiStore, ResponsesApiStoreServer};
