//! Errors produced by the envelope, the stores, and the dispatcher.
//!
//! Documented failure modes are part of the API contract; every variant
//! lists the condition that produces it.

/// An envelope construction or parsing failure.
///
/// Produced by [`OutboxEvent::new`](crate::OutboxEvent::new) and
/// [`EventId::parse`](crate::EventId::parse).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EventError {
    /// The topic failed validation. Topics must match `[a-z0-9_.-]{1,128}`:
    /// lowercase ASCII alphanumerics, `.`, `_`, `-`, 1..=128 characters. The
    /// rejected topic is included for diagnostics.
    #[error("invalid outbox topic {topic:?}: must match [a-z0-9_.-]{{1,128}}")]
    InvalidTopic {
        /// The rejected topic string.
        topic: String,
    },
    /// The event id failed parsing. Ids transport as hyphenated UUID
    /// strings (the [`Display`](std::fmt::Display) form of
    /// [`EventId`](crate::EventId)).
    #[error("invalid event id {input:?}: expected a hyphenated UUID string")]
    InvalidId {
        /// The rejected id string.
        input: String,
    },
}

/// A failure of the underlying [`OutboxStore`](crate::OutboxStore).
///
/// Stores are expected to fail closed: when a store cannot safely record
/// or read outbox state it returns an error rather than silently dropping
/// an event — a dropped envelope is a lost domain event, which is exactly
/// the failure the outbox pattern exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// [`append`](crate::OutboxStore::append) rejected the event because
    /// its topic does not match `[a-z0-9_.-]{1,128}`. The event was not
    /// recorded; fix the topic and append again.
    #[error("invalid outbox topic {topic:?}: must match [a-z0-9_.-]{{1,128}}")]
    InvalidTopic {
        /// The rejected topic string.
        topic: String,
    },
    /// The backend (`SQLite`, serializer, codec) failed. The `String` is the
    /// backend's own diagnostic; the kit never embeds payload data in it.
    /// The store's state is unchanged for a failed write; retry the
    /// operation.
    #[error("outbox store backend failure: {0}")]
    Backend(String),
}

/// A delivery failure surfaced by a dispatcher's
/// [`sender`](crate::Dispatcher) closure.
///
/// The closure maps its own error universe (HTTP client errors, broker
/// rejections, ...) into this type; the dispatcher treats every
/// [`DispatchError::Delivery`] as a retryable failure and never inspects
/// the message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DispatchError {
    /// The delivery attempt failed (downstream 5xx, broker rejection,
    /// connection reset, ...). The `String` is the sender's diagnostic.
    #[error("delivery failed: {0}")]
    Delivery(String),
}

impl From<String> for DispatchError {
    fn from(message: String) -> Self {
        Self::Delivery(message)
    }
}

impl From<&str> for DispatchError {
    fn from(message: &str) -> Self {
        Self::Delivery(message.to_owned())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn event_error_display_is_informative() {
        let err = EventError::InvalidTopic {
            topic: "BAD TOPIC".into(),
        };
        assert_eq!(
            err.to_string(),
            "invalid outbox topic \"BAD TOPIC\": must match [a-z0-9_.-]{1,128}"
        );
        let err = EventError::InvalidId {
            input: "nope".into(),
        };
        assert_eq!(
            err.to_string(),
            "invalid event id \"nope\": expected a hyphenated UUID string"
        );
    }

    #[test]
    fn store_error_display_is_informative() {
        assert_eq!(
            StoreError::InvalidTopic { topic: "X".into() }.to_string(),
            "invalid outbox topic \"X\": must match [a-z0-9_.-]{1,128}"
        );
        assert_eq!(
            StoreError::Backend("disk on fire".into()).to_string(),
            "outbox store backend failure: disk on fire"
        );
    }

    #[test]
    fn dispatch_error_display_and_conversions() {
        assert_eq!(
            DispatchError::Delivery("502 bad gateway".into()).to_string(),
            "delivery failed: 502 bad gateway"
        );
        assert_eq!(
            DispatchError::from("boom".to_string()),
            DispatchError::Delivery("boom".into())
        );
        assert_eq!(
            DispatchError::from("plain"),
            DispatchError::Delivery("plain".into())
        );
    }
}
