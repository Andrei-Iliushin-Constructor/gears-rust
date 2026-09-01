//! Version families: the key that groups every version of one logical entity, and
//! the three non-stored rules a family is asked at admission.
//!
//! A family groups `v1~`, `v1.4~` and `v2~` of the same type because
//! `database.sql`'s rules are asked of the family row, their only serialization
//! point. The split here is the split between arithmetic and judgement:
//!
//! * [`key`] derives the family key and the sibling identifiers a rule needs to
//!   look up. Pure string arithmetic over a parsed identifier — no database, no
//!   clock, no state.
//! * [`rules`] holds kind, minor shape and minor contiguity. Each is an **exact**
//!   lookup through `uq_tr_entity_gts_id` on an identifier [`key`] derived, never
//!   a scan of the family.
//!
//! The directory arrived at T12, when the second file did. T8 opened `key.rs` as a
//! flat `family.rs`, and T10 put the kind rule inline in the commit path; T12 moved
//! it here so all three rules read as one list rather than as one rule in the
//! commit and two beside it.

pub mod key;
pub mod rules;

pub use key::{FamilyKey, family_key, sibling_id};
pub use rules::{FamilyRefusal, VersionProbe, admits_new_member, version_probe};
