use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all="snake_case")]
pub(crate) enum ClientMessage<'a> {
    Get { key: &'a str },
    Put { key: &'a str, value: &'a str },
    Remove { key: &'a str },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all="snake_case")]
pub(crate) enum ServerMessage<'a> {
    Success { value: Option<&'a str> },
    Error { err: &'a str },
}