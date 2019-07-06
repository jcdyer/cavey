use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all="snake_case")]
pub(crate) enum ClientMessage {
    Get { key: String },
    Put { key: String, value: String },
    Remove { key: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all="snake_case")]
pub(crate) enum ServerMessage {
    Success { value: Option<String> },
    Error { err: String },
}