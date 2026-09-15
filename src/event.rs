//! The durable event envelope: [`EventId`] and [`OutboxEvent`].

use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::error::EventError;

/// Current unix time in milliseconds (wall clock). The kit's shared clock
/// for `created_at`, due-time math, and dispatcher scheduling. Returns `0`
/// if the system clock is set before the epoch (a broken-clock host has
/// bigger problems, but the kit stays total).
#[must_use]
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Topics must match `[a-z0-9_.-]{1,128}`.
///
/// The charset keeps topics safe as routing keys, queue names, and URL
/// path segments without escaping; the 128-character bound keeps the
/// envelope indexable in every store.
pub(crate) fn validate_topic(topic: &str) -> Result<(), EventError> {
    let valid = (1..=128).contains(&topic.len())
        && topic
            .bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(EventError::InvalidTopic {
            topic: topic.to_owned(),
        })
    }
}

/// The identifier of an outbox event: a `UUIDv7`, time-ordered so ids sort
/// roughly by creation time.
///
/// Time-ordered ids mean `ORDER BY id` is a stable tiebreak for
/// `next_attempt_at` collisions, and pagination by id approximates
/// chronological order without a separate sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventId(Uuid);

impl EventId {
    /// Generate a fresh id from the wall clock (`UUIDv7`).
    #[must_use]
    pub fn now_v7() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wrap an existing UUID — import an id minted elsewhere (a producer
    /// in another service, a migration from a legacy table).
    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// The inner UUID.
    #[must_use]
    pub fn as_uuid(self) -> Uuid {
        self.0
    }

    /// Parse the hyphenated string form (as produced by [`Display`] and by
    /// the stores).
    ///
    /// # Errors
    ///
    /// [`EventError::InvalidId`] unless `input` is a valid hyphenated (or
    /// simple/braced — anything [`Uuid::parse_str`] accepts) UUID string.
    pub fn parse(input: &str) -> Result<Self, EventError> {
        Uuid::parse_str(input)
            .map(Self)
            .map_err(|_| EventError::InvalidId {
                input: input.to_owned(),
            })
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Hyphenated lower-case — the canonical transport and storage form.
        write!(f, "{}", self.0.hyphenated())
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for EventId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0.hyphenated())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for EventId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Uuid::parse_str(&text)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

/// A durable event envelope: the unit of work an outbox store persists and
/// a dispatcher delivers.
///
/// All fields are public: construct with [`OutboxEvent::new`] for the
/// common shape, or with a struct literal for full control (imports,
/// replays, migrations). The struct never stores scheduling metadata —
/// `next_attempt_at` and dispatch state live in the
/// [`OutboxStore`](crate::OutboxStore), keyed by [`OutboxEvent::id`], so
/// the envelope round-trips through any transport unchanged.
///
/// # Determinism
///
/// `headers` is a [`BTreeMap`]: iteration order is deterministic, so two
/// stores that encode the same envelope produce identical bytes (the
/// `SQLite` store relies on this for its codec).
///
/// # Example
///
/// ```
/// use outbox_kit::OutboxEvent;
///
/// let event = OutboxEvent::new("orders.order-placed", br#"{"order_id":"A1"}"#)?;
/// assert_eq!(event.topic, "orders.order-placed");
/// assert_eq!(event.attempts, 0);
/// assert_eq!(event.last_error, None);
/// # Ok::<(), outbox_kit::EventError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OutboxEvent {
    /// The event's unique, idempotency key. `UUIDv7` (time-ordered).
    /// `append` is idempotent on this field: appending the same id twice
    /// records one row.
    pub id: EventId,
    /// The validated routing topic, `[a-z0-9_.-]{1,128}` — e.g.
    /// `"orders.order-placed"`.
    pub topic: String,
    /// The opaque payload bytes. The kit never inspects them; serialize
    /// with whatever the consumer expects (JSON, protobuf, Avro, ...).
    pub payload: Vec<u8>,
    /// Deterministic-order metadata (correlation ids, trace context,
    /// content type, idempotency keys for the consumer, ...).
    pub headers: BTreeMap<String, String>,
    /// Creation time, unix milliseconds. Doubles as the initial
    /// `next_attempt_at`: a freshly appended event is due as soon as its
    /// `created_at` has passed.
    pub created_at: u64,
    /// Number of failed dispatch attempts so far (0 for a fresh event;
    /// incremented by every [`mark_failed`](crate::OutboxStore::mark_failed)).
    pub attempts: u32,
    /// The last dispatch failure's diagnostic, truncated to 1 KiB by the
    /// stores. `None` until the first failure.
    pub last_error: Option<String>,
}

impl OutboxEvent {
    /// Create an event with a fresh `UUIDv7` id and the wall clock as
    /// `created_at` — the common producer path.
    ///
    /// # Errors
    ///
    /// [`EventError::InvalidTopic`] unless `topic` matches
    /// `[a-z0-9_.-]{1,128}`.
    pub fn new(topic: &str, payload: &[u8]) -> Result<Self, EventError> {
        validate_topic(topic)?;
        Ok(Self {
            id: EventId::now_v7(),
            topic: topic.to_owned(),
            payload: payload.to_vec(),
            headers: BTreeMap::new(),
            created_at: now_millis(),
            attempts: 0,
            last_error: None,
        })
    }

    /// Create an event with initial headers — everything else as in
    /// [`OutboxEvent::new`].
    ///
    /// # Errors
    ///
    /// [`EventError::InvalidTopic`] unless `topic` matches
    /// `[a-z0-9_.-]{1,128}`.
    pub fn with_headers(
        topic: &str,
        payload: &[u8],
        headers: BTreeMap<String, String>,
    ) -> Result<Self, EventError> {
        validate_topic(topic)?;
        Ok(Self {
            id: EventId::now_v7(),
            topic: topic.to_owned(),
            payload: payload.to_vec(),
            headers,
            created_at: now_millis(),
            attempts: 0,
            last_error: None,
        })
    }

    /// Validate the envelope's invariants (currently: topic charset).
    ///
    /// `append` calls this before recording; callers that deserialize
    /// envelopes from untrusted bytes can re-validate up front.
    ///
    /// # Errors
    ///
    /// [`EventError::InvalidTopic`] unless `topic` matches
    /// `[a-z0-9_.-]{1,128}`.
    pub fn validate(&self) -> Result<(), EventError> {
        validate_topic(&self.topic)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn event(topic: &str) -> OutboxEvent {
        OutboxEvent::new(topic, b"payload").unwrap()
    }

    #[test]
    fn new_sets_fresh_v7_and_clock() {
        let e = event("orders");
        assert_eq!(e.id.as_uuid().get_version_num(), 7);
        assert_eq!(e.topic, "orders");
        assert_eq!(e.payload, b"payload");
        assert!(e.headers.is_empty());
        let now = now_millis();
        assert!(
            e.created_at <= now && now - e.created_at < 60_000,
            "created_at must track the wall clock"
        );
        assert_eq!(e.attempts, 0);
        assert_eq!(e.last_error, None);
    }

    #[test]
    fn v7_ids_are_time_ordered_and_unique() {
        let first = EventId::now_v7();
        let second = EventId::now_v7();
        assert_ne!(first, second);
        assert!(first < second, "v7 ids must sort by creation time");
    }

    #[test]
    fn from_uuid_and_parse_round_trip() {
        let uuid = Uuid::now_v7();
        let id = EventId::from_uuid(uuid);
        assert_eq!(id.as_uuid(), uuid);
        let parsed = EventId::parse(&id.to_string()).unwrap();
        assert_eq!(parsed, id);
        // Simple and braced forms also parse.
        assert_eq!(EventId::parse(&uuid.simple().to_string()).unwrap(), id);
        assert_eq!(EventId::parse(&format!("{uuid}")).unwrap(), id);
    }

    #[test]
    fn parse_rejects_garbage() {
        let err = EventId::parse("not-a-uuid").unwrap_err();
        assert_eq!(
            err,
            EventError::InvalidId {
                input: "not-a-uuid".into()
            }
        );
        assert!(EventId::parse("").is_err());
    }

    #[test]
    fn topic_validation_accepts_charset() {
        for topic in [
            "a",
            "orders",
            "orders.order-placed",
            "a.b_c-d9",
            "0",
            &"x".repeat(128),
        ] {
            assert!(
                OutboxEvent::new(topic, b"").is_ok(),
                "topic {topic:?} must be accepted"
            );
        }
    }

    #[test]
    fn topic_validation_rejects_the_rest() {
        let rejected = [
            "",               // empty
            "Orders",         // uppercase
            "has space",      // space
            "with/slash",     // path traversal attempt
            "with:colon",     // collides with routing syntax
            "ünicode",        // non-ASCII
            "emoji🙂",        // non-ASCII
            &"x".repeat(129), // too long
        ];
        for topic in rejected {
            let err = OutboxEvent::new(topic, b"").expect_err("topic must be rejected");
            assert!(
                matches!(err, EventError::InvalidTopic { .. }),
                "topic {topic:?} must be InvalidTopic, got {err:?}"
            );
            assert_eq!(OutboxEvent::new(topic, b"").unwrap_err(), err);
        }
    }

    #[test]
    fn with_headers_and_validate() {
        let mut headers = BTreeMap::new();
        headers.insert("trace-id".to_owned(), "abc".to_owned());
        let e = OutboxEvent::with_headers("orders", b"body", headers).unwrap();
        assert_eq!(e.headers.get("trace-id").map(String::as_str), Some("abc"));
        assert!(e.validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_topic() {
        let mut e = event("orders");
        e.topic = "no good".to_owned();
        assert!(e.validate().is_err());
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_round_trip_preserves_every_field() {
        let mut e = event("orders.order-placed");
        e.headers.insert("k".to_owned(), "v".to_owned());
        e.attempts = 3;
        e.last_error = Some("boom".to_owned());

        let json = serde_json::to_string(&e).unwrap();
        let back: OutboxEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, e);

        // Headers serialize in deterministic (sorted) order.
        assert!(json.contains("{\"k\":\"v\"}"));
        // The id is a plain string.
        assert!(json.contains(&format!("\"id\":\"{}\"", e.id)));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_rejects_bad_ids() {
        let json = "{\"id\":\"garbage\",\"topic\":\"orders\",\"payload\":[],\"headers\":{},\"created_at\":0,\"attempts\":0,\"last_error\":null}";
        assert!(serde_json::from_str::<OutboxEvent>(json).is_err());
    }
}
