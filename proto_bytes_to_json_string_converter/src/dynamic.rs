use crate::ProtoDescriptorPreparer;
use anyhow::{bail, Context};
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::FileDescriptor;
use protobuf::MessageDyn;
use protobuf_json_mapping::PrintOptions;
use std::path::Path;

pub fn proto_bytes_to_json_string<T: AsRef<Path>>(
    bytes: &[u8],
    descriptor_preparer: &ProtoDescriptorPreparer<T>,
) -> Result<String, anyhow::Error> {
    let Some(file_descriptor_set) = descriptor_preparer.file_descriptor_set() else {
        bail!("ProtoMessageToJsonConverter wasn't initialized")
    };

    let messages = get_messages_from_file(file_descriptor_set);
    let message_name = messages.first().unwrap();
    let file_descriptor = FileDescriptor::new_dynamic(file_descriptor_set.clone(), &[])
        .context("While building dynamic file descriptor")?;

    let Some(message) = file_descriptor.message_by_package_relative_name(message_name) else {
        bail!("Proto message with name {} wasn't found", message_name)
    };
    let deserialized_message = message
        .parse_from_bytes(bytes)
        .context("While parsing dynamic proto message bytes")?;

    let json = proto_message_to_json_string(&*deserialized_message)
        .context("While converting from dynamic descriptor to json")?;

    Ok(json)
}

fn get_messages_from_file(file_proto: &FileDescriptorProto) -> Vec<String> {
    file_proto
        .message_type
        .iter()
        .filter_map(|x| x.name.to_owned())
        .collect()
}

fn proto_message_to_json_string(message: &dyn MessageDyn) -> Result<String, anyhow::Error> {
    let print_options = PrintOptions {
        proto_field_name: true,
        enum_values_int: false,
        always_output_default_values: true,
        ..Default::default()
    };
    let json = protobuf_json_mapping::print_to_string_with_options(message, &print_options)?;

    Ok(json)
}
