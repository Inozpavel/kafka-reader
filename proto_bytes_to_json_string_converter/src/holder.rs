use crate::ProtoDescriptorPreparer;
use anyhow::Context;

type PreparerPath = std::path::PathBuf;

pub struct ProtoDescriptorHolder {
    preparer: ProtoDescriptorPreparer<PreparerPath>,
    temp_dir: tempfile::TempDir,
}

impl ProtoDescriptorHolder {
    pub async fn from_single_file(file_content: &[u8]) -> Result<Self, anyhow::Error> {
        let temp_dir = tempfile::Builder::new()
            .prefix("kafka-reader")
            .tempdir()
            .context("While creating temp directory for proto")?;
        let path = temp_dir.path().join("message.proto");
        tokio::fs::write(&path, file_content)
            .await
            .context("While filling proto temp file")?;

        let preparer = ProtoDescriptorPreparer::new(path, vec![]);
        Ok(Self { temp_dir, preparer })
    }

    pub fn preparer(&mut self) -> &mut ProtoDescriptorPreparer<PreparerPath> {
        &mut self.preparer
    }
}

impl Drop for ProtoDescriptorHolder {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir(self.temp_dir.path()) {
            eprintln!("Error while deleting temp folder for messages proto files. {e:?}")
        }
    }
}
