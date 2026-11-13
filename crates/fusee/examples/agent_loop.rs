//! An AI agent loop with tool calls. Each model call and each tool call is a
//! step: after a crash the loop resumes where it was, without paying for (or
//! repeating the side effects of) calls that already returned.
use fusee::{Config, Ctx, Engine, Error};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
enum Turn {
    Tool { name: String, args: String },
    Answer(String),
}

// A stand-in model: asks for the weather once, then answers.
async fn model(history: &[String]) -> Result<Turn, String> {
    Ok(if history.iter().any(|h| h.starts_with("tool:")) {
        Turn::Answer(format!(
            "Pack an umbrella ({}).",
            history.last().cloned().unwrap_or_default()
        ))
    } else {
        Turn::Tool {
            name: "weather".into(),
            args: "Lisbon".into(),
        }
    })
}

async fn tool(name: &str, args: &str) -> Result<String, String> {
    match name {
        "weather" => Ok(format!("tool: rain in {args}")),
        other => Err(format!("unknown tool {other}")),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let engine = Engine::open(Config::local("./data/agent-loop")).await?;
    let agent = engine.register("agent", |ctx: Ctx, question: String| async move {
        let mut history = vec![format!("user: {question}")];
        for turn in 0..10 {
            let t: Turn = ctx
                .step(&format!("model-{turn}"), || {
                    let h = history.clone();
                    async move { model(&h).await }
                })
                .await?;
            match t {
                Turn::Answer(a) => return Ok(a),
                Turn::Tool { name, args } => {
                    let out: String = ctx
                        .step(&format!("tool-{turn}-{name}"), || {
                            let (n, a) = (name.clone(), args.clone());
                            async move { tool(&n, &a).await }
                        })
                        .await?;
                    history.push(out);
                }
            }
        }
        Err(Error::Failed("no answer after 10 turns".into()))
    });
    let run = engine
        .start(
            &agent,
            "trip-question-1",
            &"Do I need an umbrella in Lisbon?".to_string(),
        )
        .await?;
    println!("{}", run.result().await?);
    engine.shutdown().await
}
