use crate::api::kafka_service::proto::message_format_dto::proto_format_dto;
use crate::api::kafka_service::proto::security_protocol_dto::PlaintextProtocolDto;
use crate::api::kafka_service::proto::{message_format_dto, security_protocol_dto, ConnectionSettingsDto, MessageFormatDto, SaslMechanismDto, SecurityProtocolDto};
use anyhow::{anyhow, bail, Context};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use kafka_reader::connection_settings::ConnectionSettings;
use kafka_reader::consumer::{BearerClientCredentials, SaslMechanism, SecurityProtocol};
use kafka_reader::queries::read_messages::{
    Format, ProtoTarArchive, ProtobufDecodeWay, SingleProtoFile,
};
use proto_json_converter::ArchiveDecompression;
use crate::api::kafka_service::proto::message_format_dto::proto_format_dto::ArchiveDecompressionDto;
use crate::api::kafka_service::proto::message_format_dto::proto_format_dto::bytes_or_base64_data_dto::Data;
use crate::api::kafka_service::proto::sasl_mechanism_dto::Mechanism;

pub fn proto_connection_setting_to_internal(
    model: Option<ConnectionSettingsDto>,
) -> Result<ConnectionSettings, anyhow::Error> {
    let Some(model) = model else {
        bail!("Connection settings can't be null")
    };
    let security_protocol = proto_security_protocol_to_protocol(model.security_protocol)?;
    Ok(ConnectionSettings {
        brokers: model.brokers,
        security_protocol,
    })
}

pub fn proto_security_protocol_to_protocol(
    model: Option<SecurityProtocolDto>,
) -> Result<SecurityProtocol, anyhow::Error> {
    let proto_protocol =
        model
            .and_then(|x| x.protocol)
            .unwrap_or(security_protocol_dto::Protocol::Plaintext(
                PlaintextProtocolDto {},
            ));
    let result = match proto_protocol {
        security_protocol_dto::Protocol::Plaintext(_) => SecurityProtocol::Plaintext,
        security_protocol_dto::Protocol::Ssl(_) => SecurityProtocol::Ssl,
        security_protocol_dto::Protocol::SaslPlaintext(p) => {
            let mechanism = proto_sasl_mechanism_to_mechanism(p.sasl_mechanism)?;
            SecurityProtocol::SaslPlaintext(mechanism)
        }
        security_protocol_dto::Protocol::SaslSsl(p) => {
            let mechanism = proto_sasl_mechanism_to_mechanism(p.sasl_mechanism)?;
            SecurityProtocol::SaslSsl(mechanism)
        }
    };

    Ok(result)
}

pub fn proto_sasl_mechanism_to_mechanism(
    model: Option<SaslMechanismDto>,
) -> Result<SaslMechanism, anyhow::Error> {
    let Some(proto_mechanism) = model.and_then(|x| x.mechanism) else {
        bail!("SaslMechanism can't be null")
    };

    let result = match proto_mechanism {
        Mechanism::ConstantToken(constant) => SaslMechanism::ConstantToken(constant.token_value),
        Mechanism::BearerClientCredentials(credentials) => {
            SaslMechanism::BearerClientCredentials(BearerClientCredentials {
                open_id_configuration_url: credentials.open_id_configuration_url,
                client_id: credentials.client_id,
                client_secret: credentials.client_secret,
            })
        }
    };

    Ok(result)
}
pub fn proto_format_to_format(
    model: Option<MessageFormatDto>,
) -> Result<Option<Format>, anyhow::Error> {
    let Some(proto_format) = model else {
        return Ok(None);
    };

    let Some(proto_format_variant) = proto_format.format else {
        return Ok(None);
    };
    let format = match proto_format_variant {
        message_format_dto::Format::StringFormat(_) => Format::String,
        message_format_dto::Format::HexFormat(_) => Format::Hex,
        message_format_dto::Format::Base64Format(_) => Format::Base64,
        message_format_dto::Format::ProtoFormat(protobuf_data) => {
            match protobuf_data
                .decode_way
                .ok_or_else(|| anyhow!("Protobuf format can't be none"))?
            {
                proto_format_dto::DecodeWay::RawProtoFile(single_file) => {
                    Format::Protobuf(ProtobufDecodeWay::SingleProtoFile(SingleProtoFile {
                        file: single_file.file,
                        message_type_name: single_file.message_type_name,
                    }))
                }
                proto_format_dto::DecodeWay::TarArchive(tar_archive) => {
                    let archive_bytes = match tar_archive.data.and_then(|x| x.data) {
                        None => bail!("Archive data can't be null"),
                        Some(data) => match data {
                            Data::DataBytes(bytes) => bytes,
                            Data::DataBase64(base64) => BASE64_STANDARD
                                .decode(base64)
                                .context("While decoding base 64 archive bytes")?,
                        },
                    };

                    let decompression =
                        ArchiveDecompressionDto::try_from(tar_archive.decompression)
                            .ok()
                            .and_then(|x| match x {
                                ArchiveDecompressionDto::None => None,
                                ArchiveDecompressionDto::Gzip => Some(ArchiveDecompression::Gzip),
                            });

                    Format::Protobuf(ProtobufDecodeWay::TarArchive(ProtoTarArchive {
                        target_file_path: tar_archive.target_file_path,
                        message_type_name: tar_archive.message_type_name,
                        archive_bytes,
                        decompression,
                    }))
                }
            }
        }
    };

    Ok(Some(format))
}
