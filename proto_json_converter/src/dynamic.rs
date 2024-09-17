use crate::ProtoDescriptorPreparer;
use anyhow::{bail, Context};
use protobuf::descriptor::FileDescriptorProto;
use protobuf::MessageDyn;
use protobuf_json_mapping::{parse_dyn_from_str, PrintOptions};

pub fn proto_bytes_to_json_string(
    bytes: &[u8],
    message_type_name: &str,
    descriptor_preparer: &ProtoDescriptorPreparer,
) -> Result<String, anyhow::Error> {
    let Some(_) = descriptor_preparer.file_descriptor() else {
        bail!("ProtoDescriptorPreparer wasn't initialized")
    };
    // let m = _get_messages_from_file(&file_descriptor_proto);
    // eprintln!("Messages: {:?}", m);

    let Some(message) = descriptor_preparer
        .file_descriptor()
        .and_then(|x| x.message_by_package_relative_name(message_type_name))
    else {
        bail!("Proto message with name {} wasn't found", message_type_name)
    };

    let deserialized_message = message
        .parse_from_bytes(bytes)
        .context("While parsing dynamic proto message bytes")?;

    let json = proto_message_to_json_string(&*deserialized_message)
        .context("While converting from dynamic descriptor to json")?;

    Ok(json)
}

pub fn json_string_to_proto_bytes(
    json: &str,
    message_type_name: &str,
    descriptor_preparer: &ProtoDescriptorPreparer,
) -> Result<Vec<u8>, anyhow::Error> {
    let Some(message) = descriptor_preparer
        .file_descriptor()
        .and_then(|x| x.message_by_package_relative_name(message_type_name))
    else {
        bail!("Proto message with name {} wasn't found", message_type_name)
    };

    let parsed = parse_dyn_from_str(&message, json).context("While parsing json")?;

    let bytes = parsed
        .write_to_bytes_dyn()
        .context("While getting parsed proto bytes from parsed json")?;

    Ok(bytes)
}

fn proto_message_to_json_string(message: &dyn MessageDyn) -> Result<String, anyhow::Error> {
    let print_options = PrintOptions {
        proto_field_name: false,
        enum_values_int: false,
        always_output_default_values: true,
        _future_options: (),
    };
    let json = protobuf_json_mapping::print_to_string_with_options(message, &print_options)?;

    Ok(json)
}

// TODO: remove in future
fn _get_messages_from_file(file_proto: &FileDescriptorProto) -> Vec<String> {
    file_proto
        .message_type
        .iter()
        .filter_map(|x| x.name.to_owned())
        .collect()
}
