//! Listing runs by type, state, start time and search attributes.
//!
//! A filter is a JSON object. Keys `type`, `status`, `id` and `startedMs`
//! match the run itself; every other key is a search attribute set with
//! `ctx.set_attributes`. A value is matched for equality, or an object of
//! operators: `{"gte": 10, "lt": 20}`, `{"ne": "failed"}`. Numbers compare
//! as numbers and strings in byte order.
//!
//! Loaded runs are read from memory; parked and finished runs from the run
//! store, which keeps each run's merged attributes with its entry. Each
//! shard answers for its own runs and the engine merges the answers.

pub(crate) mod index;
pub use index::Candidates;

use crate::error::Error;
use crate::runtime::RunSummary;
use serde_json::{Map, Value};
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum Op {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Cond {
    field: String,
    op: Op,
    value: Value,
}

/// A parsed `where` filter.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ListFilter {
    conds: Vec<Cond>,
}

impl ListFilter {
    /// Everything.
    pub fn all() -> Self {
        Self::default()
    }

    pub fn parse(filter: &Value) -> Result<Self, Error> {
        let Value::Object(m) = filter else {
            return Err(Error::Codec("a list filter is a JSON object".into()));
        };
        let mut conds = Vec::new();
        for (field, v) in m {
            match v {
                Value::Object(ops) if !ops.is_empty() && ops.keys().all(|k| op(k).is_some()) => {
                    for (k, value) in ops {
                        if let Some(op) = op(k) {
                            conds.push(Cond {
                                field: field.clone(),
                                op,
                                value: value.clone(),
                            });
                        }
                    }
                }
                _ => conds.push(Cond {
                    field: field.clone(),
                    op: Op::Eq,
                    value: v.clone(),
                }),
            }
        }
        Ok(Self { conds })
    }

    pub fn is_empty(&self) -> bool {
        self.conds.is_empty()
    }

    pub fn matches(&self, s: &RunSummary) -> bool {
        if self.conds.is_empty() {
            return true;
        }
        let attrs: Map<String, Value> = serde_json::from_str(&s.attributes).unwrap_or_default();
        self.conds.iter().all(|c| {
            let actual = match c.field.as_str() {
                "type" | "workflowType" => Some(Value::from(s.wtype.clone())),
                "status" => Some(Value::from(s.status.clone())),
                "id" | "workflowId" => Some(Value::from(s.id.clone())),
                "startedMs" => Some(Value::from(s.started_ms)),
                "finishedMs" => s.finished_ms.map(Value::from),
                other => attrs.get(other).cloned(),
            };
            let Some(actual) = actual else {
                return c.op == Op::Ne;
            };
            match (c.op, compare(&actual, &c.value)) {
                (Op::Eq, Some(Ordering::Equal)) => true,
                (Op::Ne, o) => o != Some(Ordering::Equal),
                (Op::Gt, Some(Ordering::Greater)) => true,
                (Op::Gte, Some(Ordering::Greater | Ordering::Equal)) => true,
                (Op::Lt, Some(Ordering::Less)) => true,
                (Op::Lte, Some(Ordering::Less | Ordering::Equal)) => true,
                _ => false,
            }
        })
    }
}

fn op(k: &str) -> Option<Op> {
    Some(match k {
        "eq" => Op::Eq,
        "ne" => Op::Ne,
        "gt" => Op::Gt,
        "gte" => Op::Gte,
        "lt" => Op::Lt,
        "lte" => Op::Lte,
        _ => return None,
    })
}

fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Value::String(x), Value::String(y)) => Some(x.as_bytes().cmp(y.as_bytes())),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        _ => (a == b).then_some(Ordering::Equal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(status: &str, attrs: &str) -> RunSummary {
        RunSummary {
            shard: 0,
            run: 1,
            id: "order-1".into(),
            wtype: "checkout".into(),
            version: 1,
            status: status.into(),
            started_ms: 100,
            finished_ms: None,
            error: None,
            attributes: attrs.into(),
            object: false,
        }
    }

    #[test]
    fn equality_and_ranges() {
        let s = summary("failed", r#"{"customerId":"c-9","amount":120}"#);
        let f = |v: Value| ListFilter::parse(&v).unwrap().matches(&s);
        assert!(f(serde_json::json!({"status": "failed"})));
        assert!(!f(serde_json::json!({"status": "ok"})));
        assert!(f(
            serde_json::json!({"customerId": "c-9", "type": "checkout"})
        ));
        assert!(f(serde_json::json!({"amount": {"gte": 100, "lt": 200}})));
        assert!(!f(serde_json::json!({"amount": {"gt": 120}})));
        assert!(f(serde_json::json!({"startedMs": {"lte": 100}})));
        assert!(f(serde_json::json!({"region": {"ne": "eu"}})));
        assert!(!f(serde_json::json!({"region": "eu"})));
    }
}
