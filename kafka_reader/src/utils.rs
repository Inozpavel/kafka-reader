use crate::queries::read_messages::{ProtoTarArchive, SingleProtoFile};
use anyhow::Context;
use proto_json_converter::{InputProtoFiles, InputTarArchive, ProtoDescriptorPreparer};
use tokio::sync::RwLock;

pub async fn create_holder(
    holder: &RwLock<Option<ProtoDescriptorPreparer>>,
    single_proto_file: &SingleProtoFile,
) -> Result<(), anyhow::Error> {
    if holder.read().await.is_some() {
        return Ok(());
    }
    let mut guard = holder.write().await;

    if guard.is_some() {
        return Ok(());
    }
    let input_files = InputProtoFiles::SingleFile(single_proto_file.file.clone());
    let mut new_descriptor_holder = ProtoDescriptorPreparer::new(input_files);
    new_descriptor_holder
        .prepare()
        .await
        .context("While initializing ProtoDescriptorPreparer")?;
    *guard = Some(new_descriptor_holder);

    Ok(())
}

pub async fn create_holder_from_tar(
    holder: &RwLock<Option<ProtoDescriptorPreparer>>,
    archive: &ProtoTarArchive,
) -> Result<(), anyhow::Error> {
    if holder.read().await.is_some() {
        return Ok(());
    }
    let mut guard = holder.write().await;

    if guard.is_some() {
        return Ok(());
    }

    let input_files = InputProtoFiles::TarArchive(InputTarArchive {
        target_archive_file_path: archive.target_file_path.clone(),
        archive_bytes: archive.archive_bytes.clone(),
        decompression: archive.decompression,
    });
    let mut new_descriptor_holder = ProtoDescriptorPreparer::new(input_files);
    new_descriptor_holder
        .prepare()
        .await
        .context("While initializing ProtoDescriptorPreparer")?;
    *guard = Some(new_descriptor_holder);

    Ok(())
}
