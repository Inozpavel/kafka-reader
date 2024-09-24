use tonic::Status;
use tracing::error;

#[derive(Debug)]
pub enum ApplicationError {
    InvalidArgument(anyhow::Error),
}
impl From<ApplicationError> for Status {
    fn from(value: ApplicationError) -> Self {
        match value {
            ApplicationError::InvalidArgument(e) => {
                error!("{e:?}");
                Status::invalid_argument(format!("{e}"))
            }
        }
    }
}
