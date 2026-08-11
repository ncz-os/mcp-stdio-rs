use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use mcp_stdio_rs::{ContentBlock, McpStdioClient};
use serde_json::Value;

#[derive(Debug, Parser)]
#[command(name = "mcp-stdio-cli")]
#[command(about = "Minimal CLI for the mcp-stdio-rs library")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    ListTools {
        #[arg(long)]
        cmd: String,
    },
    Call {
        #[arg(long)]
        cmd: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        args: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cli = Cli::parse();

    match cli.command {
        Commands::ListTools { cmd } => {
            let client = spawn_from_command_string(&cmd).await?;
            let operation = async {
                let tools = client.list_tools().await?;
                for tool in tools {
                    println!("{}", tool.name);
                    if !tool.description.is_empty() {
                        println!("  {}", tool.description);
                    }
                    println!(
                        "  input_schema: {}",
                        serde_json::to_string_pretty(&tool.input_schema)?
                    );
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let shutdown = client.shutdown().await;
            operation?;
            shutdown?;
        }
        Commands::Call { cmd, name, args } => {
            let args_json: Value =
                serde_json::from_str(&args).context("failed to parse --args as JSON")?;
            let client = spawn_from_command_string(&cmd).await?;
            let operation = async {
                let result = client.call_tool(&name, args_json).await?;
                println!("is_error: {}", result.is_error);
                for block in result.content {
                    match block {
                        ContentBlock::Text(text) => println!("{text}"),
                        ContentBlock::Image { data, mime_type } => {
                            println!("[image: {} bytes, {mime_type}]", data.len());
                        }
                        ContentBlock::ResourceLink(uri) => println!("[resource: {uri}]"),
                        ContentBlock::EmbeddedResource {
                            uri,
                            mime_type,
                            text,
                            blob,
                        } => {
                            if let Some(text) = text {
                                println!("[embedded resource: {uri}]\n{text}");
                            } else {
                                println!(
                                    "[embedded resource: {uri}, {} bytes, {}]",
                                    blob.map_or(0, |blob| blob.len()),
                                    mime_type.as_deref().unwrap_or("application/octet-stream")
                                );
                            }
                        }
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let shutdown = client.shutdown().await;
            operation?;
            shutdown?;
        }
    }

    Ok(())
}

async fn spawn_from_command_string(cmd: &str) -> Result<McpStdioClient> {
    let parts = split_command(cmd)?;
    let (program, args) = parts
        .split_first()
        .ok_or_else(|| anyhow!("--cmd must contain an executable"))?;
    let args = args.iter().map(String::as_str).collect::<Vec<_>>();

    McpStdioClient::spawn(program, &args, &[]).await
}

fn split_command(input: &str) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = input.chars().peekable();
    let mut quote = None;

    while let Some(ch) = chars.next() {
        match (quote, ch) {
            (None, ch) if ch.is_whitespace() => {
                if !current.is_empty() {
                    parts.push(std::mem::take(&mut current));
                }
            }
            (None, '\'' | '"') => quote = Some(ch),
            (Some(active), ch) if ch == active => quote = None,
            (_, '\\') => {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            _ => current.push(ch),
        }
    }

    if let Some(active) = quote {
        return Err(anyhow!("unterminated {active} quote in --cmd"));
    }
    if !current.is_empty() {
        parts.push(current);
    }

    Ok(parts)
}
