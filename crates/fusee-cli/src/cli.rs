//! The commands that work with runs: against a running engine or server
//! through the HTTP API (`--url http://...`, `FUSEE_URL`), a server's
//! worker protocol (`--url host:port`), or a closed data directory
//! (`--dir`).

use clap::{Args, Parser, Subcommand, ValueEnum};
use fusee::http::{Calls, HttpClient};
use fusee::{Config, Engine, Error};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

#[derive(Parser)]
#[command(
    name = "fusee",
    version,
    about = "Durable execution at the speed of the disk",
    long_about = "Durable execution at the speed of the disk.\n\nCommands that read or change runs reach an engine \
                  through its HTTP API (--url http://127.0.0.1:7601, the default of `fusee dev` and \
                  engine.serve()), a server (--url host:7600), or a data directory nothing has open (--dir).",
    after_help = "Storage and server commands: disk-check, inspect, upgrade, reshard, backup, backups, restore, \
                  export, show, server, replicas (each with --help)."
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
    #[command(flatten)]
    pub target: Target,
}

#[derive(Args, Clone, Default)]
pub struct Target {
    /// An engine's HTTP API (http://host:port) or a server (host:port, unix:/path).
    #[arg(long, global = true, env = "FUSEE_URL")]
    pub url: Option<String>,
    /// A data directory no process has open.
    #[arg(long, global = true, env = "FUSEE_DIR")]
    pub dir: Option<PathBuf>,
    /// The API's or server's token, from a file.
    #[arg(long, global = true, env = "FUSEE_TOKEN_FILE")]
    pub token_file: Option<PathBuf>,
    /// Print JSON instead of text.
    #[arg(long, global = true)]
    pub json: bool,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Create a project: workflows, an app that runs them, and a test.
    Init(InitArgs),
    /// Run the app under the dev profile, serve the UI, restart on change.
    Dev(DevArgs),
    /// List, show, follow and act on runs.
    #[command(subcommand)]
    Runs(RunsCmd),
    /// Start a run of a workflow type.
    Start {
        /// The workflow type.
        r#type: String,
        /// The workflow ID.
        id: String,
        /// The input, JSON.
        #[arg(default_value = "null")]
        input: String,
        /// Attach to a run of this ID that is still going.
        #[arg(long)]
        use_existing: bool,
        /// Wait for the result.
        #[arg(long)]
        wait: bool,
    },
    /// List and control schedules.
    #[command(subcommand)]
    Schedules(SchedulesCmd),
    /// Keys of object types.
    #[command(subcommand)]
    Objects(ObjectsCmd),
    /// Registered workflow types and their versions.
    Types,
    /// Limits and their permits: held, waiting, and the size of each.
    Limits,
    /// Live runs per type and version: when an old version's code can go.
    Versions,
    /// Replays exported journals against workflow code, for CI before a deploy.
    ReplayCheck(ReplayArgs),
    /// Checks a data directory and this machine: lock, format, disk, configuration.
    Doctor {
        /// The data directory (default: --dir, FUSEE_DIR or ./fusee-data).
        dir: Option<PathBuf>,
    },
    /// Shell completions.
    Completions {
        #[arg(value_enum)]
        shell: Shell,
    },
    /// Decodes encrypted payloads for the UI: point the server's --codec-url here.
    CodecServer {
        /// AES-256-GCM keys: lines of `<key-id> <64 hex digits>`, as the
        /// application's codec has them.
        #[arg(long)]
        keys_file: PathBuf,
        /// The UI's origin; only its pages may call (e.g. http://127.0.0.1:7601).
        #[arg(long)]
        allow_origin: String,
        #[arg(long, default_value = "127.0.0.1:7602")]
        listen: String,
        /// Listen off loopback: only behind a proxy that signs people in.
        #[arg(long)]
        insecure: bool,
    },
}

/// `fusee codec-server --keys-file`: lines of `<key-id> <64 hex digits>`.
fn read_keys(path: &std::path::Path) -> Result<Vec<(String, [u8; 32])>, Error> {
    let text =
        std::fs::read_to_string(path).map_err(|e| Error::Io(format!("{}: {e}", path.display())))?;
    let mut keys = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let bad = || {
            Error::Rejected(format!(
                "{}:{}: expected `<key-id> <64 hex digits>`",
                path.display(),
                n + 1
            ))
        };
        let (id, hex) = line.split_once(char::is_whitespace).ok_or_else(bad)?;
        let hex = hex.trim();
        if hex.len() != 64 {
            return Err(bad());
        }
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| bad())?;
        }
        keys.push((id.to_owned(), key));
    }
    if keys.is_empty() {
        return Err(Error::Rejected(format!("{}: no keys", path.display())));
    }
    Ok(keys)
}

#[derive(Clone, Copy, ValueEnum)]
pub enum Shell {
    Bash,
    Zsh,
    Fish,
}

#[derive(Subcommand)]
pub enum RunsCmd {
    /// List runs, newest first.
    Ls {
        #[arg(long)]
        r#type: Option<String>,
        /// running, suspended, parked, paused, stuck, waiting_for_code, ok, failed, cancelled, timed_out, terminated
        #[arg(long)]
        status: Option<String>,
        /// A JSON filter on search attributes: '{"amount": {"gte": 100}}'.
        #[arg(long = "where")]
        filter: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// A run's timeline.
    Show {
        id: String,
        /// The raw journal records.
        #[arg(long)]
        raw: bool,
    },
    /// Follows a run until it finishes, printing each record as it is added.
    Tail { id: String },
    /// Waits for a run's result.
    Result { id: String },
    /// Sends a signal.
    Signal {
        id: String,
        name: String,
        #[arg(default_value = "null")]
        value: String,
    },
    /// Sends an update and prints its result.
    Update {
        id: String,
        name: String,
        #[arg(default_value = "null")]
        arg: String,
        /// The update ID: a retried call gets the recorded result.
        #[arg(long = "id")]
        update_id: Option<String>,
    },
    /// Calls a query.
    Query {
        id: String,
        name: String,
        #[arg(default_value = "null")]
        arg: String,
    },
    /// Delivers cancellation; the run's code may compensate.
    Cancel {
        id: String,
        #[arg(long, default_value = "cancelled from the CLI")]
        reason: String,
    },
    /// Ends a run at once.
    Terminate {
        id: String,
        #[arg(long, default_value = "terminated from the CLI")]
        reason: String,
        /// Do not ask.
        #[arg(long, short)]
        yes: bool,
    },
    /// Starts a new run from this one's journal cut before a step.
    Reset {
        id: String,
        #[arg(long)]
        before: String,
        #[arg(long, short)]
        yes: bool,
    },
    /// Runs a failed run again from its failed step.
    Retry { id: String },
    /// Resumes a paused run: the step whose retries ran out runs again,
    /// with a fresh budget, on the code the app has now.
    Resume { id: String },
    /// One action on every run a filter matches: cancel, terminate, retry
    /// or resume. Lists the runs and asks first; --dry-run only lists them.
    Bulk {
        /// cancel, terminate, retry or resume
        action: String,
        #[arg(long)]
        r#type: Option<String>,
        #[arg(long)]
        status: Option<String>,
        /// A JSON filter on search attributes: '{"tenant": "acme"}'.
        #[arg(long = "where")]
        filter: Option<String>,
        /// At most this many runs.
        #[arg(long, default_value_t = 1000)]
        limit: usize,
        #[arg(long, default_value = "from the CLI (bulk)")]
        reason: String,
        /// List what would change; change nothing.
        #[arg(long)]
        dry_run: bool,
        /// Do not ask.
        #[arg(long, short)]
        yes: bool,
    },
    /// Writes a run's journal as an export file (.fjnl), for `replay-check`.
    Export {
        id: String,
        #[arg(short, long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
pub enum SchedulesCmd {
    Ls,
    Show { name: String },
    Pause { name: String },
    Resume { name: String },
    Trigger { name: String },
    Rm { name: String },
}

#[derive(Subcommand)]
pub enum ObjectsCmd {
    /// Keys of an object type.
    Ls { r#type: String },
    /// A key: its state and pending calls.
    Show { r#type: String, key: String },
    /// Calls a handler.
    Call {
        r#type: String,
        key: String,
        handler: String,
        #[arg(default_value = "null")]
        arg: String,
        #[arg(long)]
        id: Option<String>,
    },
    /// Sets one field of a key's state (the value is JSON, or a bare word).
    Set {
        r#type: String,
        key: String,
        field: String,
        value: String,
        /// Refuse the edit unless the state is still at this version.
        #[arg(long)]
        if_version: Option<String>,
    },
    /// Applies a JSON Patch (RFC 6902) to a key's state.
    Patch {
        r#type: String,
        key: String,
        /// The patch: '[{"op": "remove", "path": "/items/0"}]'.
        patch: String,
        #[arg(long)]
        if_version: Option<String>,
        /// Print the state the patch would make; change nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Clears one field of a key's state, or all of it (the key remains).
    Clear {
        r#type: String,
        key: String,
        field: Option<String>,
        #[arg(long)]
        if_version: Option<String>,
        /// Do not ask (clearing all of it asks).
        #[arg(long, short)]
        yes: bool,
    },
    /// Opens a key's state in $EDITOR and applies what changed.
    Edit { r#type: String, key: String },
}

#[derive(Args)]
pub struct InitArgs {
    /// hello (default), saga, approval, schedule, agent, cart
    #[arg(default_value = "hello")]
    pub template: String,
    /// typescript, python or rust (default: what the directory already has, else typescript)
    #[arg(long)]
    pub lang: Option<String>,
    /// Where (default: the current directory).
    #[arg(long, default_value = ".")]
    pub path: PathBuf,
}

#[derive(Args)]
pub struct DevArgs {
    /// The UI and HTTP API's address.
    #[arg(long, default_value = "127.0.0.1:7601", env = "FUSEE_HTTP")]
    pub http: String,
    /// The app's data directory.
    #[arg(long = "data", default_value = ".fusee/dev")]
    pub data: PathBuf,
    /// Do not restart the app when a file changes.
    #[arg(long)]
    pub no_watch: bool,
    /// The app's command (default: found from the project).
    #[arg(last = true)]
    pub command: Vec<String>,
}

#[derive(Args)]
pub struct ReplayArgs {
    /// Export files (.fjnl) or directories of them.
    #[arg(required = true)]
    pub journals: Vec<PathBuf>,
    /// The module that defines the workflows (a .js/.mjs/.ts or .py file).
    #[arg(long)]
    pub module: PathBuf,
}

// ------------------------------------------------------------------ targets

/// A server's worker protocol, as protocol calls.
struct ServerCalls(Arc<fusee_server::Client>);

impl Calls for ServerCalls {
    fn call<'a>(&'a self, method: &'a str, args: Value) -> fusee::http::calls::CallFuture<'a> {
        let c = self.0.clone();
        let method = method.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || c.call(&method, &args))
                .await
                .map_err(|e| Error::Io(e.to_string()))?
        })
    }
}

/// What the commands call.
pub struct Conn {
    calls: Box<dyn Calls>,
    engine: Option<Engine>,
}

impl Conn {
    pub async fn call(&self, method: &str, mut args: Value) -> Result<Value, Error> {
        // Audit: the changes this CLI makes name its user. An HTTP API
        // replaces this with who it saw (its token, a proxy's user).
        if fusee::http::calls::AUDITED.contains(&method) && args.is_object() {
            args["by"] = Value::from(cli_user());
        }
        self.calls.call(method, args).await
    }

    pub async fn close(self) {
        if let Some(e) = self.engine {
            let _ = e.shutdown().await;
        }
    }
}

const DEV_URL: &str = "http://127.0.0.1:7601";

/// `cli:<user>`, as the CLI's changes name their requester.
fn cli_user() -> String {
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".into());
    format!("cli:{user}")
}

fn token(t: &Target) -> Result<Option<String>, Error> {
    t.token_file
        .as_ref()
        .map(|p| {
            std::fs::read_to_string(p)
                .map(|s| s.trim().to_owned())
                .map_err(|e| Error::Io(format!("cannot read the token file {}: {e}", p.display())))
        })
        .transpose()
}

/// How a command opens a data directory: with nothing expiring. The
/// application's retention decides what a shard removes when it opens, not
/// the command's (see [`fusee::engine::TOOL_RETENTION`]).
pub fn tool_config(dir: &std::path::Path) -> Config {
    Config::local(dir).retention(fusee::engine::TOOL_RETENTION)
}

pub async fn connect(t: &Target) -> Result<Conn, Error> {
    if let Some(dir) = &t.dir {
        let engine = Engine::open(tool_config(dir)).await.map_err(|e| match e {
            Error::Io(m) if m.contains("open in another process") => Error::Io(format!(
                "{m}. A running engine answers through its HTTP API: pass --url (engine.serve(), `fusee dev`)"
            )),
            e => e,
        })?;
        return Ok(Conn {
            calls: Box::new(engine.clone()),
            engine: Some(engine),
        });
    }
    let url = t.url.clone().unwrap_or_else(|| DEV_URL.to_owned());
    if url.starts_with("http://") {
        let c = HttpClient::new(&url, token(t)?)?;
        if t.url.is_none() {
            // The default: say what to pass when nothing is there.
            if c.call_method("info", json!({})).await.is_err() {
                return Err(Error::Connection(format!(
                    "no engine at {DEV_URL} (the default). Pass --url (an engine's HTTP API, or a server's \
                     host:port), --dir (a data directory nothing has open), or start one with `fusee dev`"
                )));
            }
        }
        return Ok(Conn {
            calls: Box::new(c),
            engine: None,
        });
    }
    let mut security = fusee_server::security::Security::default();
    if let Some(path) = &t.token_file {
        security.token = Some(
            fusee_server::security::Token::from_file(path)
                .map_err(|e| Error::Io(format!("the token file {}: {e}", path.display())))?,
        );
    }
    let addr = url.clone();
    let client = tokio::task::spawn_blocking(move || {
        if security.token.is_some() {
            fusee_server::Client::connect_with(&addr, &security)
        } else {
            fusee_server::Client::connect(&addr)
        }
    })
    .await
    .map_err(|e| Error::Io(e.to_string()))?
    .map_err(|e| Error::Connection(e.to_string()))?;
    Ok(Conn {
        calls: Box::new(ServerCalls(Arc::new(client))),
        engine: None,
    })
}

// ------------------------------------------------------------------ output

fn json_arg(s: &str) -> Result<Value, Error> {
    serde_json::from_str(s).or_else(|_| {
        // A bare word is a string: `fusee runs signal o-1 go yes`.
        if s.chars()
            .all(|c| c.is_alphanumeric() || "-_.:@/".contains(c))
        {
            Ok(Value::String(s.to_owned()))
        } else {
            Err(Error::Rejected(format!(
                "'{s}' is not JSON (quote strings: '\"text\"')"
            )))
        }
    })
}

fn ago(ms: u64) -> String {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(ms);
    let s = now.saturating_sub(ms) / 1000;
    match s {
        0..=59 => format!("{s}s ago"),
        60..=3599 => format!("{}m ago", s / 60),
        3600..=86_399 => format!("{}h ago", s / 3600),
        _ => format!("{}d ago", s / 86_400),
    }
}

fn duration(ms: u64) -> String {
    match ms {
        0..=999 => format!("{ms} ms"),
        1000..=59_999 => format!("{:.1} s", ms as f64 / 1000.0),
        60_000..=3_599_999 => format!("{}m {}s", ms / 60_000, (ms / 1000) % 60),
        _ => format!("{}h {}m", ms / 3_600_000, (ms / 60_000) % 60),
    }
}

fn table(rows: &[Vec<String>]) {
    let Some(first) = rows.first() else {
        return;
    };
    let mut widths = vec![0; first.len()];
    for r in rows {
        for (i, c) in r.iter().enumerate() {
            widths[i] = widths[i].max(c.chars().count());
        }
    }
    for r in rows {
        let line: Vec<String> = r
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if i + 1 == r.len() {
                    c.clone()
                } else {
                    format!("{c:<w$}", w = widths[i])
                }
            })
            .collect();
        println!("{}", line.join("  ").trim_end());
    }
}

fn print_value(v: &Value) {
    match v {
        Value::String(s) => println!("{s}"),
        other => println!(
            "{}",
            serde_json::to_string_pretty(other).unwrap_or_default()
        ),
    }
}

fn summary_row(r: &Value) -> Vec<String> {
    let started = r["startedMs"].as_u64().unwrap_or(0);
    let took = match r["finishedMs"].as_u64() {
        Some(f) => duration(f.saturating_sub(started)),
        None => "-".into(),
    };
    vec![
        r["id"].as_str().unwrap_or("").to_owned(),
        r["type"].as_str().unwrap_or("").to_owned(),
        format!("v{}", r["version"].as_u64().unwrap_or(1)),
        r["status"].as_str().unwrap_or("").to_owned(),
        ago(started),
        took,
        error_message(r["error"].as_str().unwrap_or(""))
            .chars()
            .take(80)
            .collect(),
    ]
}

/// A run summary's error holds the failure envelope as JSON: its message.
fn error_message(e: &str) -> String {
    if e.starts_with('{')
        && let Ok(f) = serde_json::from_str::<fusee::Failure>(e)
    {
        return f.message.clone();
    }
    e.to_owned()
}

/// One journal record as a timeline line.
fn record_line(r: &Value) -> String {
    let kind = r["kind"].as_str().unwrap_or("?");
    let name = r["name"].as_str().unwrap_or("");
    let short = |v: &Value| {
        let s = v.to_string();
        if s.chars().count() > 60 {
            format!("{}…", s.chars().take(60).collect::<String>())
        } else {
            s
        }
    };
    match kind {
        "start" => format!(
            "started   {} v{} input {}",
            r["type"].as_str().unwrap_or(""),
            r["version"],
            short(&r["input"])
        ),
        "step" => {
            let mark = if r["status"] == "ok" { "✓" } else { "✗" };
            let attempts = r["attempt"].as_u64().unwrap_or(1);
            let tries = if attempts > 1 {
                format!(" (attempt {attempts})")
            } else {
                String::new()
            };
            let out = if r["status"] == "ok" {
                short(&r["payload"])
            } else {
                r["payload"]["message"].as_str().unwrap_or("").to_owned()
            };
            format!("{mark} step    {name}{tries} → {out}")
        }
        "attempt" => format!(
            "  retry   {name} attempt {} failed: {}",
            r["attempt"],
            r["error"]["message"].as_str().unwrap_or("")
        ),
        "timer" => format!("  sleep   {name}"),
        "wait" => format!("  wait    {name}"),
        "signal" => format!("→ signal  {name} {}", short(&r["payload"])),
        "child" => format!(
            "  child   {} {}",
            r["type"].as_str().unwrap_or(""),
            r["id"].as_str().unwrap_or("")
        ),
        "update" => format!("→ update  {name} ({})", r["id"].as_str().unwrap_or("")),
        "done" => format!(
            "■ {} {}",
            r["status"].as_str().unwrap_or(""),
            if r["status"] == "ok" {
                short(&r["payload"])
            } else {
                r["payload"]["message"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| short(&r["payload"]))
            }
        ),
        other => format!("  {other:<7} {}", short(r)),
    }
}

// ------------------------------------------------------------------ commands

pub async fn run(cli: Cli) -> Result<(), Error> {
    match cli.cmd {
        Cmd::Init(a) => crate::project::init(&a.template, a.lang.as_deref(), &a.path),
        Cmd::Dev(a) => crate::project::dev(a).await,
        Cmd::Completions { shell } => {
            let mut cmd = <Cli as clap::CommandFactory>::command();
            print!("{}", completions(shell, &mut cmd));
            Ok(())
        }
        Cmd::Doctor { dir } => crate::project::doctor(dir.or(cli.target.dir.clone())).await,
        Cmd::CodecServer {
            keys_file,
            allow_origin,
            listen,
            insecure,
        } => {
            let codec = fusee::aes_gcm(read_keys(&keys_file)?);
            let opts = fusee::http::CodecOptions {
                allow_origin,
                insecure,
            };
            let server = fusee::http::serve_codec(codec, &listen, opts)
                .await
                .map_err(|e| Error::Io(e.to_string()))?;
            eprintln!(
                "fusee codec-server: POST {}/decode; pass --codec-url {} to the server",
                server.url(),
                server.url()
            );
            std::future::pending::<()>().await;
            Ok(())
        }
        Cmd::ReplayCheck(a) => crate::project::replay_check(&a.journals, &a.module),
        cmd => {
            let json_out = cli.target.json;
            let conn = connect(&cli.target).await?;
            let r = command(&conn, cmd, json_out).await;
            conn.close().await;
            r
        }
    }
}

async fn command(c: &Conn, cmd: Cmd, json_out: bool) -> Result<(), Error> {
    match cmd {
        Cmd::Runs(r) => runs(c, r, json_out).await,
        Cmd::Start {
            r#type,
            id,
            input,
            use_existing,
            wait,
        } => {
            let policy = if use_existing {
                json!({"running": "use-existing"})
            } else {
                Value::Null
            };
            c.call(
                "start",
                json!({"type": r#type, "id": id, "input": json_arg(&input)?, "version": null,
                    "options": {"timeout": null, "idPolicy": policy}}),
            )
            .await?;
            if wait {
                print_value(&c.call("result", json!({ "id": id })).await?);
            } else if !json_out {
                println!("started {id}");
            }
            Ok(())
        }
        Cmd::Schedules(s) => schedules(c, s, json_out).await,
        Cmd::Objects(o) => objects(c, o, json_out).await,
        Cmd::Types => {
            let t = c.call("types", json!({})).await?;
            if json_out {
                print_value(&t);
                return Ok(());
            }
            let mut rows = vec![vec!["TYPE".into(), "VERSIONS".into(), "KIND".into()]];
            for x in t.as_array().into_iter().flatten() {
                rows.push(vec![
                    x["name"].as_str().unwrap_or("").to_owned(),
                    x["versions"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|v| format!("v{v}"))
                        .collect::<Vec<_>>()
                        .join(" "),
                    if x["object"] == true {
                        "object".into()
                    } else {
                        "workflow".into()
                    },
                ]);
            }
            table(&rows);
            Ok(())
        }
        Cmd::Versions => versions(c, json_out).await,
        Cmd::Limits => {
            let l = c.call("limits", json!({})).await?;
            if json_out {
                print_value(&l);
                return Ok(());
            }
            let rows: Vec<&Value> = l.as_array().into_iter().flatten().collect();
            if rows.is_empty() {
                println!(
                    "no limits defined. engine.limit(\"stripe\", concurrent=5) defines one; a step takes a \
                     permit with limit=\"stripe\""
                );
                return Ok(());
            }
            let mut t = vec![vec![
                "LIMIT".into(),
                "IN USE".into(),
                "CONCURRENT".into(),
                "PER SECOND".into(),
                "WAITING".into(),
            ]];
            let or_dash = |v: &Value| match v.as_f64() {
                Some(x) if x > 0.0 => format!("{x}"),
                _ => "-".into(),
            };
            for x in rows {
                t.push(vec![
                    x["name"].as_str().unwrap_or("").to_owned(),
                    x["inUse"].to_string(),
                    or_dash(&x["concurrent"]),
                    or_dash(&x["perSecond"]),
                    x["waiting"].to_string(),
                ]);
            }
            table(&t);
            Ok(())
        }
        _ => unreachable!("handled in run"),
    }
}

/// A list filter from --type, --status and --where.
fn filter_of(
    r#type: Option<String>,
    status: Option<String>,
    filter: Option<String>,
) -> Result<Value, Error> {
    let mut f = match filter {
        Some(w) => serde_json::from_str(&w)
            .map_err(|e| Error::Rejected(format!("--where is not JSON: {e}")))?,
        None => json!({}),
    };
    if let Some(t) = r#type {
        f["type"] = json!(t);
    }
    if let Some(s) = status {
        f["status"] = json!(s);
    }
    Ok(f)
}

/// Runs a failed run again from its failed step; returns the step.
async fn retry(c: &Conn, id: &str) -> Result<String, Error> {
    let d = c.call("describe", json!({ "id": id })).await?;
    let step = d["journal"]
        .as_array()
        .into_iter()
        .flatten()
        .rev()
        .find(|r| r["kind"] == "step" && r["status"] == "failed")
        .and_then(|r| r["name"].as_str().map(str::to_owned))
        .ok_or_else(|| {
            Error::Rejected(format!(
                "workflow '{id}' has no failed step to retry (status {})",
                d["status"].as_str().unwrap_or("unknown")
            ))
        })?;
    c.call(
        "reset",
        json!({"id": id, "beforeStep": step, "reapplySignals": true}),
    )
    .await?;
    Ok(step)
}

/// `fusee runs bulk`: one action on every run a filter matches.
async fn bulk(
    c: &Conn,
    action: &str,
    f: Value,
    limit: usize,
    reason: &str,
    dry_run: bool,
    yes: bool,
) -> Result<(), Error> {
    if !matches!(action, "cancel" | "terminate" | "retry" | "resume") {
        return Err(Error::Rejected(format!(
            "the bulk action is cancel, terminate, retry or resume, not '{action}'"
        )));
    }
    if f.as_object().is_none_or(|o| o.is_empty()) {
        return Err(Error::Rejected(
            "a bulk action needs a filter: --type, --status or --where".into(),
        ));
    }
    let rows = list(c, f, limit).await?;
    // Finished runs cannot be cancelled or terminated; only failed ones are
    // retried and only paused ones resumed.
    let ids: Vec<String> = rows
        .iter()
        .filter(|r| {
            let st = r["status"].as_str().unwrap_or("");
            let done = matches!(
                st,
                "ok" | "failed" | "cancelled" | "timed_out" | "terminated"
            );
            match action {
                "retry" => st == "failed",
                "resume" => st == "paused",
                _ => !done,
            }
        })
        .filter_map(|r| r["id"].as_str().map(str::to_owned))
        .collect();
    if ids.is_empty() {
        println!(
            "no run matches (finished runs are not cancelled or terminated; only failed runs are retried and paused runs resumed)"
        );
        return Ok(());
    }
    for id in &ids {
        println!("{id}");
    }
    let n = ids.len();
    if dry_run {
        println!(
            "would {action} {n} run{} (dry run: nothing changed)",
            if n == 1 { "" } else { "s" }
        );
        return Ok(());
    }
    confirm(
        &format!("{action} these {n} runs? This cannot be undone."),
        yes,
    )?;
    let mut failed = 0usize;
    for id in &ids {
        let r = match action {
            "cancel" => c
                .call("cancel", json!({"id": id, "reason": reason}))
                .await
                .map(|_| ()),
            "terminate" => c
                .call("terminate", json!({"id": id, "reason": reason}))
                .await
                .map(|_| ()),
            "resume" => c.call("resume", json!({ "id": id })).await.map(|_| ()),
            _ => retry(c, id).await.map(|_| ()),
        };
        if let Err(e) = r {
            failed += 1;
            eprintln!("{id}: {e}");
        }
    }
    println!("{action}: {} done, {failed} failed", n - failed);
    if failed > 0 {
        return Err(Error::failed(format!("{failed} of {n} runs failed")));
    }
    Ok(())
}

async fn list(c: &Conn, filter: Value, limit: usize) -> Result<Vec<Value>, Error> {
    let mut rows = match c
        .call("list", json!({"where": filter, "limit": limit}))
        .await?
    {
        Value::Array(a) => a,
        _ => Vec::new(),
    };
    rows.sort_by_key(|r| std::cmp::Reverse(r["startedMs"].as_u64().unwrap_or(0)));
    rows.truncate(limit);
    Ok(rows)
}

fn confirm(question: &str, yes: bool) -> Result<(), Error> {
    use std::io::{BufRead, IsTerminal, Write};
    if yes {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        return Err(Error::Rejected(format!(
            "{question} Pass --yes to confirm without a terminal"
        )));
    }
    print!("{question} [y/N] ");
    let _ = std::io::stdout().flush();
    let mut line = String::new();
    let _ = std::io::stdin().lock().read_line(&mut line);
    if matches!(line.trim(), "y" | "Y" | "yes") {
        Ok(())
    } else {
        Err(Error::Rejected("not confirmed; nothing changed".into()))
    }
}

async fn runs(c: &Conn, cmd: RunsCmd, json_out: bool) -> Result<(), Error> {
    match cmd {
        RunsCmd::Ls {
            r#type,
            status,
            filter,
            limit,
        } => {
            let f = filter_of(r#type, status, filter)?;
            let rows = list(c, f, limit).await?;
            if json_out {
                print_value(&Value::Array(rows));
                return Ok(());
            }
            if rows.is_empty() {
                println!(
                    "No runs. Start one from your code, or with `fusee start <type> <id> [input]`."
                );
                return Ok(());
            }
            let mut t = vec![
                [
                    "WORKFLOW ID",
                    "TYPE",
                    "VERSION",
                    "STATUS",
                    "STARTED",
                    "DURATION",
                    "ERROR",
                ]
                .map(String::from)
                .to_vec(),
            ];
            t.extend(rows.iter().map(summary_row));
            table(&t);
            Ok(())
        }
        RunsCmd::Show { id, raw } => {
            let d = c.call("describe", json!({ "id": id })).await?;
            if json_out || raw {
                print_value(&d);
                return Ok(());
            }
            let started = d["startedMs"].as_u64().unwrap_or(0);
            let took = d["finishedMs"]
                .as_u64()
                .map(|f| format!(" · {}", duration(f.saturating_sub(started))))
                .unwrap_or_default();
            println!("{id}");
            println!(
                "{} v{} · run {} · {} · started {}{took}",
                d["type"].as_str().unwrap_or(""),
                d["version"],
                d["run"],
                d["status"].as_str().unwrap_or(""),
                ago(started)
            );
            if let Some(e) = d["error"].as_str() {
                println!("\n  {e}");
            }
            println!();
            for r in d["journal"].as_array().into_iter().flatten() {
                println!("  {}", record_line(r));
            }
            if d["status"] == "stuck" {
                println!(
                    "\nFix: deploy code that matches the journal, guard the change with patched(), or \
                     `fusee runs reset {id} --before <step>`."
                );
            } else if d["status"] == "failed" {
                println!("\n`fusee runs retry {id}` runs it again from its failed step.");
            }
            Ok(())
        }
        RunsCmd::Tail { id } => {
            let mut seen = 0usize;
            loop {
                let d = c.call("describe", json!({ "id": id })).await?;
                let journal = d["journal"].as_array().cloned().unwrap_or_default();
                for r in journal.iter().skip(seen) {
                    if json_out {
                        println!("{r}");
                    } else {
                        println!("{}", record_line(r));
                    }
                }
                seen = journal.len();
                if d["finishedMs"].is_u64() {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
        RunsCmd::Result { id } => {
            print_value(&c.call("result", json!({ "id": id })).await?);
            Ok(())
        }
        RunsCmd::Signal { id, name, value } => {
            c.call(
                "signal",
                json!({"id": id, "name": name, "value": json_arg(&value)?}),
            )
            .await?;
            if !json_out {
                println!("signalled {id}: {name}");
            }
            Ok(())
        }
        RunsCmd::Update {
            id,
            name,
            arg,
            update_id,
        } => {
            let v = c
                .call(
                    "update",
                    json!({"id": id, "name": name, "arg": json_arg(&arg)?, "updateId": update_id.unwrap_or_default()}),
                )
                .await?;
            print_value(&v);
            Ok(())
        }
        RunsCmd::Query { id, name, arg } => {
            print_value(
                &c.call(
                    "query",
                    json!({"id": id, "name": name, "arg": json_arg(&arg)?}),
                )
                .await?,
            );
            Ok(())
        }
        RunsCmd::Cancel { id, reason } => {
            c.call("cancel", json!({"id": id, "reason": reason}))
                .await?;
            println!("cancellation delivered to {id}");
            Ok(())
        }
        RunsCmd::Terminate { id, reason, yes } => {
            confirm(
                &format!(
                    "Terminate {id}? A step still running is discarded; its children follow their close policy."
                ),
                yes,
            )?;
            c.call("terminate", json!({"id": id, "reason": reason}))
                .await?;
            println!("terminated {id}");
            Ok(())
        }
        RunsCmd::Reset { id, before, yes } => {
            confirm(
                &format!(
                    "Reset {id} to before step '{before}'? A new run replaces it; steps from there on run again."
                ),
                yes,
            )?;
            c.call(
                "reset",
                json!({"id": id, "beforeStep": before, "reapplySignals": true}),
            )
            .await?;
            println!("reset {id} before '{before}'");
            Ok(())
        }
        RunsCmd::Retry { id } => {
            let step = retry(c, &id).await?;
            println!("retrying {id} from step '{step}'");
            Ok(())
        }
        RunsCmd::Resume { id } => {
            c.call("resume", json!({ "id": id })).await?;
            println!("resumed {id}");
            Ok(())
        }
        RunsCmd::Bulk {
            action,
            r#type,
            status,
            filter,
            limit,
            reason,
            dry_run,
            yes,
        } => {
            bulk(
                c,
                &action,
                filter_of(r#type, status, filter)?,
                limit,
                &reason,
                dry_run,
                yes,
            )
            .await
        }
        RunsCmd::Export { id, out } => {
            let v = c.call("export", json!({ "id": id })).await?;
            let file = v["file"]
                .as_str()
                .and_then(fusee::http::calls::unbase64)
                .ok_or_else(|| Error::Io("the export was not base64".into()))?;
            let path =
                out.unwrap_or_else(|| PathBuf::from(v["name"].as_str().unwrap_or("run.fjnl")));
            std::fs::write(&path, &file)?;
            println!("{} ({} bytes)", path.display(), file.len());
            Ok(())
        }
    }
}

async fn schedules(c: &Conn, cmd: SchedulesCmd, json_out: bool) -> Result<(), Error> {
    let id = |n: &str| format!("fusee.schedule/{n}");
    let control = |n: &str, op: &str| json!({"id": id(n), "name": "fusee.schedule.control", "value": {"op": op}});
    match cmd {
        SchedulesCmd::Ls => {
            let rows: Vec<Value> = list(c, json!({"type": "fusee.schedule"}), 1000)
                .await?
                .into_iter()
                .filter(|r| r["finishedMs"].is_null())
                .collect();
            if json_out {
                print_value(&Value::Array(rows));
                return Ok(());
            }
            let mut t = vec![vec!["SCHEDULE".into(), "SINCE".into()]];
            for r in &rows {
                let name = r["id"]
                    .as_str()
                    .unwrap_or("")
                    .trim_start_matches("fusee.schedule/");
                t.push(vec![
                    name.to_owned(),
                    ago(r["startedMs"].as_u64().unwrap_or(0)),
                ]);
            }
            table(&t);
            Ok(())
        }
        SchedulesCmd::Show { name } => {
            print_value(
                &c.call(
                    "query",
                    json!({"id": id(&name), "name": "describe", "arg": null}),
                )
                .await?,
            );
            Ok(())
        }
        SchedulesCmd::Pause { name } => c.call("signal", control(&name, "pause")).await.map(|_| ()),
        SchedulesCmd::Resume { name } => {
            c.call("signal", control(&name, "resume")).await.map(|_| ())
        }
        SchedulesCmd::Trigger { name } => c
            .call("signal", control(&name, "trigger"))
            .await
            .map(|_| ()),
        SchedulesCmd::Rm { name } => c
            .call(
                "terminate",
                json!({"id": id(&name), "reason": "schedule deleted"}),
            )
            .await
            .map(|_| ()),
    }
}

async fn objects(c: &Conn, cmd: ObjectsCmd, json_out: bool) -> Result<(), Error> {
    match cmd {
        ObjectsCmd::Ls { r#type } => {
            let prefix = format!("{type}/", type = r#type);
            let keys: Vec<String> = list(c, json!({ "type": r#type }), 10_000)
                .await?
                .into_iter()
                .filter(|r| r["finishedMs"].is_null())
                .filter_map(|r| {
                    r["id"]
                        .as_str()
                        .and_then(|i| i.strip_prefix(&prefix))
                        .map(str::to_owned)
                })
                .collect();
            if json_out {
                print_value(&json!(keys));
            } else {
                keys.iter().for_each(|k| println!("{k}"));
            }
            Ok(())
        }
        ObjectsCmd::Show { r#type, key } => {
            let id = format!("{}/{key}", r#type);
            let mut d = c.call("describe", json!({ "id": id })).await?;
            if let Ok(s) = c
                .call(
                    "query",
                    json!({"id": id, "name": "fusee.state", "arg": null}),
                )
                .await
            {
                d["state"] = s;
            }
            if json_out {
                print_value(&d);
            } else {
                println!("{id} · {}", d["status"].as_str().unwrap_or(""));
                println!(
                    "state: {}",
                    serde_json::to_string_pretty(&d["state"]).unwrap_or_default()
                );
                if let Some(o) = d
                    .get("objectDetail")
                    .or_else(|| d.get("object"))
                    .filter(|o| !o.is_null())
                {
                    println!("version: {}", o["version"].as_str().unwrap_or(""));
                    println!("running: {}  waiting: {}", o["running"], o["waiting"]);
                }
            }
            Ok(())
        }
        ObjectsCmd::Set {
            r#type,
            key,
            field,
            value,
            if_version,
        } => {
            let mut set = serde_json::Map::new();
            set.insert(field, json_arg(&value)?);
            edit_state(
                c,
                &r#type,
                &key,
                json!({ "set": set }),
                if_version,
                json_out,
            )
            .await
        }
        ObjectsCmd::Patch {
            r#type,
            key,
            patch,
            if_version,
            dry_run,
        } => {
            let ops: Value = serde_json::from_str(&patch)
                .map_err(|e| Error::Rejected(format!("the patch is JSON: {e}")))?;
            if dry_run {
                let (mut state, version) = object_state(c, &r#type, &key).await?;
                if let Some(v) = &if_version
                    && *v != version
                {
                    return Err(Error::Rejected(format!(
                        "{} {v} (it is {version} now)",
                        fusee::object::edit::STALE
                    )));
                }
                fusee::object::edit::patch(&mut state, &ops)?;
                print_value(&state);
                println!("(dry run: nothing changed)");
                return Ok(());
            }
            edit_state(
                c,
                &r#type,
                &key,
                json!({ "patch": ops }),
                if_version,
                json_out,
            )
            .await
        }
        ObjectsCmd::Clear {
            r#type,
            key,
            field,
            if_version,
            yes,
        } => {
            let edit = match field {
                Some(f) => json!({ "clear": [f] }),
                None => {
                    confirm(
                        &format!(
                            "Clear all of {}/{key}'s state? Its handlers will see it empty.",
                            r#type
                        ),
                        yes,
                    )?;
                    json!({ "clearAll": true })
                }
            };
            edit_state(c, &r#type, &key, edit, if_version, json_out).await
        }
        ObjectsCmd::Edit { r#type, key } => {
            let (state, version) = object_state(c, &r#type, &key).await?;
            let path = std::env::temp_dir().join(format!(
                "fusee-{}-{}.json",
                r#type,
                key.replace('/', "_")
            ));
            std::fs::write(
                &path,
                serde_json::to_string_pretty(&state).unwrap_or_default(),
            )?;
            let editor = std::env::var("VISUAL")
                .or_else(|_| std::env::var("EDITOR"))
                .unwrap_or_else(|_| "vi".into());
            let ok = std::process::Command::new("sh")
                .arg("-c")
                .arg(format!("{editor} \"$1\""))
                .arg("sh")
                .arg(&path)
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            let _ = std::fs::remove_file(&path);
            if !ok {
                return Err(Error::Rejected(format!(
                    "{editor} did not exit cleanly: nothing changed"
                )));
            }
            let after: Value = serde_json::from_str(&text)
                .map_err(|e| Error::Rejected(format!("the state is a JSON object: {e}")))?;
            if !after.is_object() {
                return Err(Error::Rejected("the state is a JSON object".into()));
            }
            if after == state {
                println!("no change");
                return Ok(());
            }
            // The whole state replaced: only the fields that differ are written.
            edit_state(
                c,
                &r#type,
                &key,
                json!({ "clearAll": true, "set": after }),
                Some(version),
                json_out,
            )
            .await
        }
        ObjectsCmd::Call {
            r#type,
            key,
            handler,
            arg,
            id,
        } => {
            let v = c
                .call(
                    "updateWithStart",
                    json!({"type": r#type, "id": format!("{}/{key}", r#type), "input": null, "name": handler,
                        "arg": json_arg(&arg)?, "updateId": id.unwrap_or_default(), "version": null,
                        "options": {"timeout": null, "idPolicy": {"running": "use-existing", "finished": "allow"}}}),
                )
                .await?;
            print_value(&v);
            Ok(())
        }
    }
}

/// A key's committed state and its version.
async fn object_state(c: &Conn, wtype: &str, key: &str) -> Result<(Value, String), Error> {
    let id = format!("{wtype}/{key}");
    let d = c.call("describe", json!({ "id": id })).await?;
    let o = d
        .get("objectDetail")
        .or_else(|| d.get("object"))
        .filter(|o| !o.is_null())
        .ok_or_else(|| Error::Rejected(format!("{id} is not a key of an object type")))?;
    Ok((
        o["state"].clone(),
        o["version"].as_str().unwrap_or_default().to_owned(),
    ))
}

/// Edits a key's state and prints the new version and what changed.
async fn edit_state(
    c: &Conn,
    wtype: &str,
    key: &str,
    mut edit: Value,
    if_version: Option<String>,
    json_out: bool,
) -> Result<(), Error> {
    if let Some(v) = if_version {
        edit["ifVersion"] = json!(v);
    }
    let v = c
        .call(
            "editState",
            json!({"id": format!("{wtype}/{key}"), "edit": edit}),
        )
        .await?;
    if json_out {
        print_value(&v);
    } else {
        let changed: Vec<&str> = v["changed"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        println!(
            "{wtype}/{key}: {} · version {}",
            if changed.is_empty() {
                "nothing changed".to_owned()
            } else {
                format!("changed {}", changed.join(", "))
            },
            v["version"].as_str().unwrap_or("")
        );
    }
    Ok(())
}

/// The statuses of a run that has not finished.
const LIVE_STATUSES: [&str; 6] = [
    "running",
    "suspended",
    "parked",
    "paused",
    "stuck",
    "waiting_for_code",
];

/// Live runs per type and version: a version with none left and a newer
/// one registered can have its code removed.
async fn versions(c: &Conn, json_out: bool) -> Result<(), Error> {
    let types = c.call("types", json!({})).await.unwrap_or(json!([]));
    // Every unfinished run, by status: a newest-N listing of all runs could
    // miss old runs that still wait, and then say their code can go.
    let mut rows = Vec::new();
    for status in LIVE_STATUSES {
        rows.extend(list(c, json!({ "status": status }), u32::MAX as usize).await?);
    }
    let mut live: std::collections::BTreeMap<(String, u64), usize> = Default::default();
    for r in rows.iter().filter(|r| r["finishedMs"].is_null()) {
        let t = r["type"].as_str().unwrap_or("").to_owned();
        if t.starts_with("fusee.") {
            continue;
        }
        *live
            .entry((t, r["version"].as_u64().unwrap_or(1)))
            .or_default() += 1;
    }
    for t in types.as_array().into_iter().flatten() {
        let name = t["name"].as_str().unwrap_or("").to_owned();
        for v in t["versions"].as_array().into_iter().flatten() {
            live.entry((name.clone(), v.as_u64().unwrap_or(1)))
                .or_default();
        }
    }
    if json_out {
        print_value(&Value::Array(
            live.iter()
                .map(|((t, v), n)| json!({"type": t, "version": v, "live": n}))
                .collect(),
        ));
        return Ok(());
    }
    let mut t = vec![vec![
        "TYPE".into(),
        "VERSION".into(),
        "LIVE RUNS".into(),
        "".into(),
    ]];
    let newest: std::collections::BTreeMap<&str, u64> =
        live.keys().fold(Default::default(), |mut m, (t, v)| {
            let e = m.entry(t.as_str()).or_insert(*v);
            *e = (*e).max(*v);
            m
        });
    for ((ty, v), n) in &live {
        let note = if *n == 0 && newest.get(ty.as_str()).is_some_and(|nv| nv > v) {
            "no live runs: its code can go"
        } else {
            ""
        };
        t.push(vec![
            ty.clone(),
            format!("v{v}"),
            n.to_string(),
            note.into(),
        ]);
    }
    table(&t);
    Ok(())
}

fn completions(shell: Shell, cmd: &mut clap::Command) -> String {
    let mut names: Vec<String> = cmd
        .get_subcommands()
        .map(|s| s.get_name().to_owned())
        .collect();
    names.extend(
        [
            "disk-check",
            "inspect",
            "upgrade",
            "reshard",
            "backup",
            "backups",
            "restore",
            "export",
            "show",
            "server",
            "replicas",
        ]
        .map(String::from),
    );
    let words = names.join(" ");
    match shell {
        Shell::Bash => format!(
            "_fusee() {{ local cur=${{COMP_WORDS[COMP_CWORD]}}; if [ $COMP_CWORD -eq 1 ]; then COMPREPLY=( $(compgen -W \"{words}\" -- \"$cur\") ); fi; }}\ncomplete -F _fusee fusee\n"
        ),
        Shell::Zsh => format!("#compdef fusee\n_arguments '1: :({words})' '*::arg:_files'\n"),
        Shell::Fish => names
            .iter()
            .map(|n| format!("complete -c fusee -n '__fish_use_subcommand' -a {n}\n"))
            .collect(),
    }
}

/// Waits until `url` answers (an app under `fusee dev` starting).
pub async fn wait_for(url: &str, within: Duration) -> bool {
    let Ok(c) = HttpClient::new(url, None) else {
        return false;
    };
    let until = Instant::now() + within;
    while Instant::now() < until {
        if c.call_method("info", json!({})).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Commands open a directory without a retention of their own.
    #[test]
    fn commands_expire_nothing() {
        let c = tool_config(std::path::Path::new("data"));
        assert_eq!(c.retention, fusee::engine::TOOL_RETENTION);
    }
}
