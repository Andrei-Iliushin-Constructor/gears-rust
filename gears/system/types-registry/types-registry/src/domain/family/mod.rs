//! Version families: the key that groups every version of one logical entity, and
//! the three non-stored rules a family is asked at admission.
//!
//! A family groups `v1~`, `v1.4~` and `v2~` of the same type because
//! `database.sql`'s rules are asked of the family row, their only serialization
//! point. The split here is between arithmetic and judgement:
//!
//! * `key` derives the family key and the sibling identifiers a rule needs to
//!   look up. Pure string arithmetic over a parsed identifier — no database, no
//!   clock, no state.
//! * `rules` holds kind, minor shape and minor contiguity. Each is an **exact**
//!   lookup through `uq_tr_entity_gts_id` on an identifier `key` derived, never
//!   a scan of the family.
//!
//! [`FamilyKey`], [`family_key`], [`FamilyRefusal`] and [`admits_new_member`] leave
//! the directory — the key the storage layer persists, and the question the commit
//! path asks. [`version_probe`] leaves it too, for exactly one reason:
//! [`compat`](crate::domain::compat) needs the identifier of a candidate's
//! preceding minor, which is the same `vM.(n-1)~` the contiguity rule looks up.
//! Reading it from the probe rather than deriving it again is what keeps the
//! baseline and the rule from ever naming different identifiers. `sibling_id`
//! stays private: it is the spelling arithmetic underneath, and the probe is the
//! answer callers actually want.

mod key;
mod rules;

pub use key::{FamilyKey, family_key, lock_order};
pub use rules::{FamilyRefusal, VersionProbe, admits_new_member, version_probe};
