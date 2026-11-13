//! Edits of a key's state from outside its handlers.
//!
//! An edit is an exclusive call to the reserved handler [`EDIT_HANDLER`],
//! which every object type has: it waits for the key like any exclusive
//! call, and its changes commit with its result in the `UpdateDone` record
//! a handler's writes make, so it replays, deduplicates by call ID and is
//! sealed by the codec as they are. The handler's code in each SDK is one
//! call to the engine, which applies the edit below.
//!
//! The edit is a JSON object, applied in this order:
//!
//! | Field | |
//! | --- | --- |
//! | `ifVersion` | refuse the edit unless the state's [`version`] is this |
//! | `clearAll` | `true`: empty the state |
//! | `clear` | fields to remove (absent ones are ignored) |
//! | `set` | fields to set, `{field: value}`; `null` removes one |
//! | `patch` | a JSON Patch (RFC 6902) over the state as one JSON object |
//! | `by` | who asked, recorded with the edit (the HTTP API and CLI set it) |
//!
//! The answer is `{"version": the new version, "changed": [fields]}`.

use crate::error::{Error, Result};
use fusee_core::record::Payload;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The handler every object type has that applies an edit.
pub const EDIT_HANDLER: &str = "fusee.state.edit";

/// How the refusal of an edit whose `ifVersion` is stale begins.
pub const STALE: &str = "the state changed since version";

/// The state's version: a hash of its JSON, so an editor that read one
/// version can refuse to overwrite another (`ifVersion`, HTTP `If-Match`).
pub fn version(state: &BTreeMap<String, Payload>) -> String {
    let json = super::state_json(state);
    format!("{:016x}", fusee_core::wire::xxh64(&json, 0))
}

/// The changes `edit` makes to `state` (`None` removes a field) and the
/// edit's answer. Changes nothing on a refusal.
pub fn apply(
    state: &BTreeMap<String, Payload>,
    edit: &[u8],
) -> Result<(BTreeMap<String, Option<Payload>>, Payload)> {
    let Value::Object(edit) = serde_json::from_slice::<Value>(edit)
        .map_err(|e| Error::Rejected(format!("a state edit is a JSON object: {e}")))?
    else {
        return Err(Error::Rejected("a state edit is a JSON object".into()));
    };
    for k in edit.keys() {
        if !matches!(
            k.as_str(),
            "ifVersion" | "clearAll" | "clear" | "set" | "patch" | "by"
        ) {
            return Err(Error::Rejected(format!(
                "a state edit has ifVersion, clearAll, clear, set, patch and by, not '{k}'"
            )));
        }
    }
    if let Some(want) = edit.get("ifVersion").filter(|v| !v.is_null()) {
        let want = want
            .as_str()
            .ok_or_else(|| Error::Rejected("ifVersion is a string".into()))?;
        let now = version(state);
        if want != now {
            return Err(Error::Rejected(format!(
                "{STALE} {want} (it is {now} now): read it again"
            )));
        }
    }
    let mut doc = Map::new();
    for (k, v) in state {
        doc.insert(
            k.clone(),
            serde_json::from_slice(v).map_err(|e| Error::Codec(e.to_string()))?,
        );
    }
    let before = doc.clone();
    if edit.get("clearAll").and_then(Value::as_bool) == Some(true) {
        doc.clear();
    }
    if let Some(clear) = edit.get("clear") {
        let fields = clear
            .as_array()
            .ok_or_else(|| Error::Rejected("clear is a list of fields".into()))?;
        for f in fields {
            let f = f
                .as_str()
                .ok_or_else(|| Error::Rejected("clear is a list of fields".into()))?;
            doc.remove(f);
        }
    }
    if let Some(set) = edit.get("set") {
        let set = set
            .as_object()
            .ok_or_else(|| Error::Rejected("set is an object of fields and values".into()))?;
        for (k, v) in set {
            if v.is_null() {
                doc.remove(k);
            } else {
                doc.insert(k.clone(), v.clone());
            }
        }
    }
    if let Some(ops) = edit.get("patch") {
        let mut v = Value::Object(doc);
        patch(&mut v, ops)?;
        doc = match v {
            Value::Object(m) => m,
            _ => {
                return Err(Error::Rejected(
                    "the patched state is not a JSON object".into(),
                ));
            }
        };
    }
    let mut writes = BTreeMap::new();
    for (k, v) in &before {
        match doc.get(k) {
            None => {
                writes.insert(k.clone(), None);
            }
            Some(n) if n != v => {
                writes.insert(k.clone(), Some(raw(n)));
            }
            Some(_) => {}
        }
    }
    for (k, v) in &doc {
        if !before.contains_key(k) && !v.is_null() {
            writes.insert(k.clone(), Some(raw(v)));
        }
    }
    let mut after = state.clone();
    for (k, v) in &writes {
        match v {
            Some(v) => after.insert(k.clone(), v.clone()),
            None => after.remove(k),
        };
    }
    let answer = json!({
        "version": version(&after),
        "changed": writes.keys().collect::<Vec<_>>(),
    });
    Ok((writes, raw(&answer)))
}

fn raw(v: &Value) -> Payload {
    Arc::from(v.to_string().into_bytes())
}

/// Applies a JSON Patch (RFC 6902) to `doc`: all of it, or none of it.
pub fn patch(doc: &mut Value, ops: &Value) -> Result<()> {
    let ops = ops
        .as_array()
        .ok_or_else(|| Error::Rejected("a JSON Patch is a list of operations".into()))?;
    let mut work = doc.clone();
    for (i, op) in ops.iter().enumerate() {
        apply_op(&mut work, op).map_err(|why| {
            Error::Rejected(format!("JSON Patch operation {i} ({}): {why}", short(op)))
        })?;
    }
    *doc = work;
    Ok(())
}

fn short(op: &Value) -> String {
    let s = op.to_string();
    match s.char_indices().nth(80) {
        Some((at, _)) => format!("{}…", &s[..at]),
        None => s,
    }
}

fn apply_op(doc: &mut Value, op: &Value) -> std::result::Result<(), String> {
    let name = op["op"].as_str().ok_or("it has no \"op\"")?;
    let path = op["path"].as_str().ok_or("it has no \"path\"")?;
    let path = pointer(path)?;
    let value = || op.get("value").cloned().ok_or("it has no \"value\"");
    let from = || -> std::result::Result<Vec<String>, String> {
        pointer(op["from"].as_str().ok_or("it has no \"from\"")?)
    };
    match name {
        "add" => add(doc, &path, value()?),
        "remove" => remove(doc, &path).map(|_| ()),
        "replace" => {
            let v = value()?;
            let target = get_mut(doc, &path).ok_or("the path does not exist")?;
            *target = v;
            Ok(())
        }
        "move" => {
            let from = from()?;
            if path.len() > from.len() && path[..from.len()] == from[..] {
                return Err("a value cannot move into itself".into());
            }
            let v = remove(doc, &from)?;
            add(doc, &path, v)
        }
        "copy" => {
            let v = get(doc, &from()?).ok_or("\"from\" does not exist")?.clone();
            add(doc, &path, v)
        }
        "test" => {
            let v = value()?;
            match get(doc, &path) {
                Some(found) if *found == v => Ok(()),
                Some(found) => Err(format!("the value is {found}, not {v}")),
                None => Err("the path does not exist".into()),
            }
        }
        other => Err(format!("no operation \"{other}\"")),
    }
}

/// A JSON Pointer (RFC 6901) as its unescaped tokens.
fn pointer(p: &str) -> std::result::Result<Vec<String>, String> {
    if p.is_empty() {
        return Ok(Vec::new());
    }
    let Some(rest) = p.strip_prefix('/') else {
        return Err(format!("the pointer \"{p}\" does not start with /"));
    };
    Ok(rest
        .split('/')
        .map(|t| t.replace("~1", "/").replace("~0", "~"))
        .collect())
}

fn get<'a>(doc: &'a Value, path: &[String]) -> Option<&'a Value> {
    path.iter().try_fold(doc, |v, t| match v {
        Value::Object(m) => m.get(t),
        Value::Array(a) => a.get(index(t, a.len()).ok()?),
        _ => None,
    })
}

fn get_mut<'a>(doc: &'a mut Value, path: &[String]) -> Option<&'a mut Value> {
    path.iter().try_fold(doc, |v, t| match v {
        Value::Object(m) => m.get_mut(t),
        Value::Array(a) => {
            let i = index(t, a.len()).ok()?;
            a.get_mut(i)
        }
        _ => None,
    })
}

fn index(t: &str, len: usize) -> std::result::Result<usize, String> {
    if t.is_empty() || (t.len() > 1 && t.starts_with('0')) || !t.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(format!("\"{t}\" is not an array index"));
    }
    let i: usize = t
        .parse()
        .map_err(|_| format!("\"{t}\" is not an array index"))?;
    if i >= len {
        return Err(format!("index {i} is past the end ({len})"));
    }
    Ok(i)
}

fn add(doc: &mut Value, path: &[String], v: Value) -> std::result::Result<(), String> {
    let Some((last, parent)) = path.split_last() else {
        *doc = v;
        return Ok(());
    };
    match get_mut(doc, parent).ok_or("the parent does not exist")? {
        Value::Object(m) => {
            m.insert(last.clone(), v);
            Ok(())
        }
        Value::Array(a) => {
            if last == "-" {
                a.push(v);
            } else {
                let i = if last.as_str() == a.len().to_string() {
                    a.len()
                } else {
                    index(last, a.len())?
                };
                a.insert(i, v);
            }
            Ok(())
        }
        _ => Err("the parent is not an object or an array".into()),
    }
}

fn remove(doc: &mut Value, path: &[String]) -> std::result::Result<Value, String> {
    let (last, parent) = path
        .split_last()
        .ok_or("the whole state cannot be removed")?;
    match get_mut(doc, parent).ok_or("the parent does not exist")? {
        Value::Object(m) => m
            .remove(last)
            .ok_or_else(|| "the path does not exist".into()),
        Value::Array(a) => {
            let i = index(last, a.len())?;
            Ok(a.remove(i))
        }
        _ => Err("the parent is not an object or an array".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(v: Value) -> BTreeMap<String, Payload> {
        v.as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), raw(v)))
            .collect()
    }

    fn edit(s: &BTreeMap<String, Payload>, e: Value) -> Result<(Value, Value)> {
        let (writes, answer) = apply(s, e.to_string().as_bytes())?;
        let w: Map<String, Value> = writes
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    v.map(|v| serde_json::from_slice(&v).unwrap())
                        .unwrap_or(Value::Null),
                )
            })
            .collect();
        Ok((Value::Object(w), serde_json::from_slice(&answer).unwrap()))
    }

    #[test]
    fn set_clear_and_patch_make_the_writes_that_differ() {
        let s = state(json!({"items": ["a", "b"], "total": 2, "note": "x"}));
        let (w, a) = edit(
            &s,
            json!({
                "clear": ["note", "absent"],
                "set": {"total": 2, "owner": "ana"},
                "patch": [
                    {"op": "remove", "path": "/items/0"},
                    {"op": "add", "path": "/items/-", "value": "c"},
                    {"op": "test", "path": "/owner", "value": "ana"}
                ]
            }),
        )
        .unwrap();
        assert_eq!(
            w,
            json!({"items": ["b", "c"], "note": null, "owner": "ana"})
        );
        assert_eq!(a["changed"], json!(["items", "note", "owner"]));
        let mut after = s.clone();
        after.insert("items".into(), raw(&json!(["b", "c"])));
        after.remove("note");
        after.insert("owner".into(), raw(&json!("ana")));
        assert_eq!(a["version"], json!(version(&after)));
    }

    #[test]
    fn a_failing_patch_or_a_stale_version_changes_nothing() {
        let s = state(json!({"n": 1}));
        let e = edit(
            &s,
            json!({"patch": [{"op": "replace", "path": "/n", "value": 2}, {"op": "test", "path": "/n", "value": 3}]}),
        )
        .unwrap_err();
        assert!(e.to_string().contains("operation 1"), "{e}");
        let stale = edit(
            &s,
            json!({"ifVersion": "0000000000000000", "clearAll": true}),
        )
        .unwrap_err();
        assert!(
            stale.to_string().contains("changed since version"),
            "{stale}"
        );
        let (w, _) = edit(&s, json!({"ifVersion": version(&s), "clearAll": true})).unwrap();
        assert_eq!(w, json!({"n": null}));
        assert!(edit(&s, json!({"bogus": 1})).is_err());
    }

    #[test]
    fn json_patch_follows_rfc_6902() {
        let mut doc = json!({"a": {"b~c": 1, "d/e": [1, 2, 3]}});
        patch(
            &mut doc,
            &json!([
                {"op": "copy", "from": "/a/b~0c", "path": "/x"},
                {"op": "move", "from": "/a/d~1e/0", "path": "/a/d~1e/2"},
                {"op": "add", "path": "/a/d~1e/1", "value": 9},
                {"op": "replace", "path": "/a/b~0c", "value": null}
            ]),
        )
        .unwrap();
        assert_eq!(
            doc,
            json!({"a": {"b~c": null, "d/e": [2, 9, 3, 1]}, "x": 1})
        );
        assert!(
            patch(
                &mut doc,
                &json!([{"op": "move", "from": "/a", "path": "/a/x"}])
            )
            .is_err()
        );
        assert!(patch(&mut doc, &json!([{"op": "remove", "path": "/a/d~1e/01"}])).is_err());
    }
}
