use anyhow::{bail, Context};
use protobuf::descriptor::{FileDescriptorProto, FileDescriptorSet};
use protobuf::Message;
use std::fs;
use std::path::{Path, PathBuf};

pub struct ProtoDescriptorPreparer<T: AsRef<Path>> {
    created_dir: Option<tempfile::TempDir>,
    proto_path: T,
    includes: Vec<T>,
    file_descriptor_set: Option<FileDescriptorProto>,
}

impl<T: AsRef<Path>> ProtoDescriptorPreparer<T> {
    pub fn new(proto_path: T, includes: Vec<T>) -> Self {
        Self {
            proto_path,
            includes,
            created_dir: None,
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
        let descriptor_dir = tempfile::TempDir::new().context("While creating temp file")?;
        let file_path = descriptor_dir.path().join("descriptor.bin");

        self.created_dir = Some(descriptor_dir);

        tokio::fs::File::create(&file_path)
            .await
            .context("While creating empty descriptor file")?;

        tonic_build::configure()
            .file_descriptor_set_path(&file_path)
            .protoc_arg("-I")
            .protoc_arg("/")
            .build_client(false)
            .build_server(false)
            .compile(
                &[&self.proto_path.as_ref()],
                Vec::<PathBuf>::new().as_slice(),
            )
            .context("While building descriptor set")?;

        let descriptor_set_bytes = tokio::fs::read(file_path)
            .await
            .context("While reading generated descriptor")?;

        let file_descriptor_set = Self::parse_descriptor(&descriptor_set_bytes)
            .context("While parsing descriptor set bytes")?;
        self.file_descriptor_set = Some(file_descriptor_set);

        Ok(())
    }

    fn parse_descriptor(descriptor_bytes: &[u8]) -> Result<FileDescriptorProto, anyhow::Error> {
        let descriptor = FileDescriptorSet::parse_from_bytes(descriptor_bytes)
            .context("While parsing generated descriptor")?;

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
        if let Some(dir) = &self.created_dir {
            if let Err(e) = fs::remove_dir(dir.path()) {
                eprintln!(
                    "Error while deleting descriptor temp dir. Path {:?}. {:?}",
                    dir.path(),
                    e
                )
            }
        }
    }
}
