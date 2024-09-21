use crate::requests::read_messages::SingleProtoFile;
use anyhow::Context;
use proto_json_converter::ProtoDescriptorPreparer;
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
    let mut new_descriptor_holder =
        ProtoDescriptorPreparer::new(single_proto_file.file.to_string(), vec![]);
    new_descriptor_holder
        .prepare()
        .await
        .context("While initializing ProtoDescriptorPreparer")?;
    *guard = Some(new_descriptor_holder);

    Ok(())
}
