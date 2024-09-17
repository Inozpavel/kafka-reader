use anyhow::{anyhow, Context};
use protobuf::reflect::FileDescriptor;
use std::fs;

pub struct ProtoDescriptorPreparer {
    created_dir: Option<tempfile::TempDir>,
    proto_file: String,
    _proto_file_includes: Vec<String>,
    file_descriptor: Option<FileDescriptor>,
}

impl ProtoDescriptorPreparer {
    pub fn new(proto_file: String, proto_file_includes: Vec<String>) -> Self {
        Self {
            proto_file,
            _proto_file_includes: proto_file_includes,
            created_dir: None,
            file_descriptor: None,
        }
    }

    pub fn file_descriptor(&self) -> Option<&FileDescriptor> {
        self.file_descriptor.as_ref()
    }

    pub async fn prepare(&mut self) -> Result<(), anyhow::Error> {
        if self.created_dir.is_some() {
            return Ok(());
        }
        let temp_dir = tempfile::TempDir::new().context("While creating temp file")?;
        let dir_path = temp_dir.path().to_path_buf();
        let file_path = temp_dir.path().join("message.proto");

        self.created_dir = Some(temp_dir);

        tokio::fs::write(&file_path, self.proto_file.as_bytes())
            .await
            .context("While filling temp file with proto")?;

        let mut file_descriptor_protos = protobuf_parse::Parser::new()
            .pure()
            .includes(&dir_path)
            .input(&file_path)
            .parse_and_typecheck()
            .context("While building file descriptors")?
            .file_descriptors;

        if file_descriptor_protos.is_empty() || file_descriptor_protos.len() > 1 {
            return Err(anyhow!(
                "Incorrect files count parsed from proto. Expected 1, got {}",
                file_descriptor_protos.len()
            ));
        }
        let file_descriptor_proto = file_descriptor_protos.pop().unwrap();

        let file_descriptor = FileDescriptor::new_dynamic(file_descriptor_proto, &[])
            .context("While building file_descriptor")?;

        self.file_descriptor = Some(file_descriptor);

        Ok(())
    }
}

impl Drop for ProtoDescriptorPreparer {
    fn drop(&mut self) {
        if let Some(dir) = &self.created_dir {
            if let Err(e) = fs::remove_dir_all(dir.path()) {
                eprintln!(
                    "Error while deleting descriptor temp dir. Path {:?}. {:?}",
                    dir.path(),
                    e
                )
            }
        }
    }
}
