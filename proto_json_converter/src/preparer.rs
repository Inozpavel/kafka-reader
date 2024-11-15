use anyhow::{bail, Context};
use protobuf::reflect::FileDescriptor;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use flate2::read::GzDecoder;
use tar::Archive;
use tracing::{debug, error, trace};
use uuid::Uuid;

pub struct ProtoDescriptorPreparer {
    created_dir: Option<PathBuf>,
    input_files: InputProtoFiles,
    file_descriptor: Option<FileDescriptor>,
}

pub enum InputProtoFiles {
    SingleFile(String),
    TarArchive(InputTarArchive),
}
pub struct InputTarArchive {
    pub archive_bytes: Vec<u8>,
    pub target_archive_file_path: String,
    pub decompression: Option<ArchiveDecompression>,
}

#[derive(Debug, Copy, Clone)]
pub enum ArchiveDecompression {
    Gzip
}

impl ProtoDescriptorPreparer {
    pub fn new(input_files: InputProtoFiles) -> Self {
        Self {
            input_files,
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

        tokio::fs::create_dir_all(&dir_path)
            .await
            .context("While creating directory for messages")?;

        self.created_dir = Some(PathBuf::from(dir_path.clone()));

        let file_path: PathBuf = match &self.input_files {
            InputProtoFiles::SingleFile(file) => {
                let single_file_path = format!("{}/message.proto", &dir_path);
                tokio::fs::write(&single_file_path, file.as_bytes())
                    .await
                    .context("While filling temp file with proto")?;

                PathBuf::from(&single_file_path)
            }
            InputProtoFiles::TarArchive(input_archive) => {
                let decompression = input_archive.decompression;
                let cursor = Cursor::new(input_archive.archive_bytes.as_slice());
                let reader: Box<dyn Read> = match decompression {
                    None => Box::new(cursor),
                    Some(ArchiveDecompression::Gzip) => Box::new(GzDecoder::new(cursor))
                };
                let mut archive = Archive::new(reader);
                let entries = archive.entries().context("While getting archive entries")?;

                let mut requested_file_path = None;

                let target_file_path = Path::new(&input_archive.target_archive_file_path);
                for entry in entries {
                    let mut entry = entry.context("while getting file for archive")?;

                    let mut bytes = vec![];

                    entry
                        .read_to_end(&mut bytes)
                        .context("While reading file bytes")?;

                    let archive_file_path = entry.path().context("While getting entry path")?;
                    let result_file_path = PathBuf::from(format!(
                        "{}/{}",
                        &dir_path,
                        archive_file_path
                            .as_os_str()
                            .to_str()
                            .context("Invalid utf-8 file path")?
                    ));
                    let mut result_dir_path = result_file_path.clone();
                    result_dir_path.pop();

                    debug!("Path: {:?}", &result_file_path);
                    std::fs::create_dir_all(&result_dir_path).context("While creating dir for certain archive file")?;
                    let mut file = std::fs::File::create(&result_file_path).context("While creating file from archive")?;
                    file.write_all(&bytes).context("While writing file bytes")?;

                    if archive_file_path == target_file_path {
                        requested_file_path = Some(result_file_path);
                    }
                }

                let Some(requested_file_path) = requested_file_path else {
                    bail!("Requested file wasn't found in archive");
                };

                requested_file_path
            }
        };
        let path_in_dir = file_path.strip_prefix("messages/")?;
        let file_descriptor_protos = protobuf_parse::Parser::new()
            .pure()
            .includes(&PathBuf::from(dir_path))
            .input(&file_path)
            .parse_and_typecheck()
            .inspect_err(|e| error!("Proto parsing error {:?}", e))
            .context("While building file descriptors")?
            .file_descriptors;

        trace!("Descriptors: {:#?} ", file_descriptor_protos);

        let Some(file_descriptor_proto) = file_descriptor_protos
            .iter()
            .find(|x| x.name.as_ref().is_some_and(|n| Path::new(n) == path_in_dir))
        else {
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
            .map(|x| FileDescriptor::new_dynamic(x, &includes))
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
            if let Err(e) = std::fs::remove_dir_all(directory_path) {
                eprintln!(
                    "Error while deleting descriptor temp dir. Path {:?}. {:?}",
                    directory_path, e
                )
            }
        }
    }
}
