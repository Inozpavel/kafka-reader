// #![deny(clippy::perf)]
//
// mod consumer;
// mod message;
//
// use anyhow::Context;
// use bytes::BytesMut;
// use prost::Message as ProstMessage;
// use proto_bytes_to_json_string_converter::{proto_bytes_to_json_string, ProtoDescriptorPreparer};
// use rdkafka::consumer::{Consumer, StreamConsumer};
// use rdkafka::{ClientConfig, Message};
//
// mod snazzy {
//     include!(concat!(env!("OUT_DIR"), "/snazzy.items.rs"));
// }
// #[tokio::main]
// async fn main() -> Result<(), anyhow::Error> {
//     let shirt = snazzy::Shirt {
//         size: 26123,
//         color: "Red123".into(),
//     };
//
//     let mut serialized = BytesMut::new();
//     prost::Message::encode(&shirt, &mut serialized)?;
//     let f = snazzy::Shirt::decode(serialized.clone())?;
//     println!("Decoded {f:?}");
//
//     let mut descriptor_holder =
//         ProtoDescriptorPreparer::new("./kafka_reader/src/message.proto", vec![]);
//
//     let json1 = proto_bytes_to_json_string(serialized.as_ref(), &mut descriptor_holder).await?;
//     let json2 = proto_bytes_to_json_string(serialized.as_ref(), &mut descriptor_holder).await?;
//
//     println!("{json1}");
//     println!("{json2}");
//
//     Ok(())
// }
//
