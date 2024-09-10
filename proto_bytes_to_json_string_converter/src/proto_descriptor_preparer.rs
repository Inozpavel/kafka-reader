use anyhow::{bail, Context};
use protobuf::descriptor::{FileDescriptorProto, FileDescriptorSet};
use protobuf::Message;
use std::fs;
use std::path::Path;
use tempfile::NamedTempFile;

pub struct ProtoDescriptorPreparer<T: AsRef<Path>> {
    created_file: Option<NamedTempFile>,
    proto_path: T,
    includes: Vec<T>,
    file_descriptor_set: Option<FileDescriptorProto>,
}

impl<T: AsRef<Path>> ProtoDescriptorPreparer<T> {
    pub fn new(proto_path: T, includes: Vec<T>) -> Self {
        Self {
            proto_path,
            includes,
            created_file: None,
            file_descriptor_set: None,
        }
    }

    pub fn file_descriptor_set(&self) -> Option<&FileDescriptorProto> {
        self.file_descriptor_set.as_ref()
    }

    pub async fn prepare(&mut self) -> Result<(), anyhow::Error> {
        if self.file_descriptor_set.is_some() {
            return Ok(());
        }
        let temp_file = NamedTempFile::new().context("While creating temp file")?;
        self.created_file = Some(temp_file);

        let file = self.created_file.as_ref().unwrap();
        tonic_build::configure()
            .file_descriptor_set_path(file.path())
            .compile(&[&self.proto_path], &self.includes)?;

        let descriptor_set_bytes = tokio::fs::read(file.path())
            .await
            .context("While reading generated descriptor")?;

        let file_descriptor_set = Self::parse_descriptor(&descriptor_set_bytes)
            .context("While parsing descriptor set bytes")?;
        self.file_descriptor_set = Some(file_descriptor_set);

        Ok(())
    }

    fn parse_descriptor(descriptor_bytes: &[u8]) -> Result<FileDescriptorProto, anyhow::Error> {
        let descriptor = FileDescriptorSet::parse_from_bytes(descriptor_bytes)?;

        let FileDescriptorSet { file, .. } = descriptor;

        if file.is_empty() {
            bail!("No files files in descriptor set");
        } else if file.len() > 1 {
            bail!(
                "More than 1 file found in descriptor set. Count: {}",
                file.len()
            );
        }

        let file_descriptor = file.into_iter().next().unwrap();
        Ok(file_descriptor)
    }
}

impl<T: AsRef<Path>> Drop for ProtoDescriptorPreparer<T> {
    fn drop(&mut self) {
        if let Some(file) = &self.created_file {
            if let Err(e) = fs::remove_file(file.path()) {
                eprint!(
                    "Error while deleting temp file. Path {:?}. {:?}",
                    file.path(),
                    e
                )
            }
        }
    }
}
