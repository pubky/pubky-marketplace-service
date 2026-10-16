//! The store providers the connector framework knows. Each one implements
//! [`super::provider::ConnectorProvider`] and is registered in
//! [`super::providers_from_env`].

pub mod shopify;
pub mod woocommerce;
