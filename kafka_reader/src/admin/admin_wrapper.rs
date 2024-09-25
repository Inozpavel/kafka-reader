use crate::connection_settings::ConnectionSettings;
use anyhow::Context;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::ClientConfig;
use std::ops::{Deref, DerefMut};

pub struct AdminWrapper {
    client: AdminClient<DefaultClientContext>,
}

impl AdminWrapper {
    pub fn create(connection_settings: ConnectionSettings) -> Result<Self, anyhow::Error> {
        // https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md
        let client: AdminClient<DefaultClientContext> =
            ClientConfig::try_from(&connection_settings)?
                .set("message.max.bytes", "1000000000")
                .set("receive.message.max.bytes", "2147483647")
                .create()
                .context("While creating kafka StreamConsumer")?;

        Ok(Self { client })
    }
}

impl DerefMut for AdminWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl Deref for AdminWrapper {
    type Target = AdminClient<DefaultClientContext>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
