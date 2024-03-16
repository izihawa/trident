use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::fmt::Display;

/// An Error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("author_error: {description}")]
    Author { description: String },
    #[error("doc_error: {description}")]
    Doc { description: String },
    #[error("file_shard: {description}")]
    FileShard { description: String },
    #[error("node_create_error: {description}")]
    NodeCreate { description: String },
    #[error("table_error: {description}")]
    Table { description: String },
    #[error("table_ticket_error: {description}")]
    TableTicket { description: String },
    #[error("blobs: {description}")]
    Blobs { description: String },
    #[error("entry_error: {description}")]
    Entry { description: String },
    #[error("io_error: {description}")]
    Io { description: String },
    #[error("sink: {description}")]
    Sink { description: String },
    #[error("missing_table: {description}")]
    MissingTable { description: String },
    #[error("missing_key: {description}")]
    MissingKey { description: String },
    #[error("missing_sink: {description}")]
    MissingSink { description: String },
    #[error("existing_sink: {description}")]
    ExistingSink { description: String },
    #[error("existing_table: {description}")]
    ExistingTable { description: String },
    #[error("storage: {description}")]
    Storage { description: String },
    #[error("key: {description}")]
    IncorrectKey { description: String },
    #[error("network: {description}")]
    Network { description: String },
    #[error("failed_download: {description}")]
    FailedDownload { description: String },
}

impl Error {
    pub fn author(error: impl Display) -> Self {
        Error::Author {
            description: error.to_string(),
        }
    }

    pub fn doc(error: impl Display) -> Self {
        Error::Doc {
            description: error.to_string(),
        }
    }

    pub fn node_create(error: impl Display) -> Self {
        Error::NodeCreate {
            description: error.to_string(),
        }
    }

    pub fn table(error: impl Display) -> Self {
        Error::Table {
            description: error.to_string(),
        }
    }

    pub fn table_ticket(error: impl Display) -> Self {
        Error::TableTicket {
            description: error.to_string(),
        }
    }

    pub fn blobs(error: impl Display) -> Self {
        Error::Blobs {
            description: error.to_string(),
        }
    }

    pub fn entry(error: impl Display) -> Self {
        Error::Entry {
            description: error.to_string(),
        }
    }

    pub fn io_error(error: impl Display) -> Self {
        Error::Io {
            description: error.to_string(),
        }
    }

    pub fn sink(error: impl Display) -> Self {
        Error::Sink {
            description: error.to_string(),
        }
    }

    pub fn missing_table(error: impl Display) -> Self {
        Error::MissingTable {
            description: error.to_string(),
        }
    }

    pub fn missing_key(error: impl Display) -> Self {
        Error::MissingKey {
            description: error.to_string(),
        }
    }

    pub fn missing_sink(error: impl Display) -> Self {
        Error::MissingSink {
            description: error.to_string(),
        }
    }
    pub fn existing_sink(error: impl Display) -> Self {
        Error::ExistingSink {
            description: error.to_string(),
        }
    }

    pub fn existing_table(error: impl Display) -> Self {
        Error::ExistingTable {
            description: error.to_string(),
        }
    }

    pub fn storage(error: impl Display) -> Self {
        Error::Storage {
            description: error.to_string(),
        }
    }

    pub fn incorrect_key(error: impl Display) -> Self {
        Error::IncorrectKey {
            description: error.to_string(),
        }
    }

    pub fn network(error: impl Display) -> Self {
        Error::Network {
            description: error.to_string(),
        }
    }

    pub fn failed_download(error: impl Display) -> Self {
        Error::FailedDownload {
            description: error.to_string(),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let code = match self {
            Error::MissingKey { .. } | Error::MissingTable { .. } => StatusCode::NOT_FOUND,
            Error::ExistingTable { .. } => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (code, self.to_string()).into_response()
    }
}

pub type Result<T> = std::result::Result<T, Error>;
