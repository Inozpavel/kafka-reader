extern crate kafka_reader_api;

use bytes::BytesMut;
use tonic::include_proto;

include_proto!("snazzy.items");

#[test]
fn print_hex() -> Result<(), Box<dyn std::error::Error>> {
    let shirts = vec![
        Shirt {
            size: 2,
            color: "Yellow".into(),
        },
        Shirt {
            size: 1,
            color: "Black".into(),
        },
        Shirt {
            size: 3,
            color: "Red".into(),
        },
    ];

    for shirt in shirts {
        let mut serialized = BytesMut::new();
        prost::Message::encode(&shirt, &mut serialized)?;

        println!("{:?} {:02X}", shirt, serialized);
    }

    Ok(())
}
