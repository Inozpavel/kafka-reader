use anyhow::{bail, Context};
use protobuf::reflect::FileDescriptor;
use std::fs;
use std::path::PathBuf;
use tracing::{error, trace};
use uuid::Uuid;

pub struct ProtoDescriptorPreparer {
    created_dir: Option<PathBuf>,
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
        let id = Uuid::new_v4();
        let dir_path = format!("messages/{}", id);
        let file_path = format!("messages/{}/message.proto", id);
        let relative_to_dir_file_name = format!("{}/message.proto", id);
        tokio::fs::create_dir_all(&dir_path)
            .await
            .context("While creating directory for messages")?;

        self.created_dir = Some(PathBuf::from(dir_path.clone()));

        tokio::fs::write(&file_path, self.proto_file.as_bytes())
            .await
            .context("While filling temp file with proto")?;

        let file_descriptor_protos = protobuf_parse::Parser::new()
            .pure()
            .includes(&PathBuf::from(dir_path))
            .input(&file_path)
            .parse_and_typecheck()
            .inspect_err(|e| error!("Proto parsing error {:?}", e))
            .context("While building file descriptors")?
            .file_descriptors;

        trace!("Descriptors: {:#?} ", file_descriptor_protos);

        let Some(file_descriptor_proto) = file_descriptor_protos.iter().find(|x| {
            x.name
                .as_ref()
                .is_some_and(|n| n.as_str() == relative_to_dir_file_name)
        }) else {
            bail!("Internal error, generated file not found")
        };
        let file_descriptor_proto = file_descriptor_proto.clone();

        let includes = vec![
            protobuf::well_known_types::timestamp::file_descriptor().clone(),
            protobuf::well_known_types::wrappers::file_descriptor().clone(),
            protobuf::well_known_types::empty::file_descriptor().clone(),
            protobuf::well_known_types::duration::file_descriptor().clone(),
            protobuf::well_known_types::api::file_descriptor().clone(),
            protobuf::well_known_types::source_context::file_descriptor().clone(),
        ];

        let files = file_descriptor_protos
            .into_iter()
            .map(|x| FileDescriptor::new_dynamic(x, &includes.clone()))
            .collect::<Result<Vec<_>, _>>()
            .context("While mapping files")?;

        let file_descriptor = FileDescriptor::new_dynamic(file_descriptor_proto, &files)
            .context("While building file_descriptor")?;

        self.file_descriptor = Some(file_descriptor);

        Ok(())
    }
}

impl Drop for ProtoDescriptorPreparer {
    fn drop(&mut self) {
        if let Some(directory_path) = &self.created_dir {
            if let Err(e) = fs::remove_dir_all(directory_path) {
                eprintln!(
                    "Error while deleting descriptor temp dir. Path {:?}. {:?}",
                    directory_path, e
                )
            }
        }
    }
}
