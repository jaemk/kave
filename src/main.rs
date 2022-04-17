use kave::{config::LogFormat, Result, CONFIG};

async fn run() -> Result<()> {
    setup()?;

    tracing::info!("running");
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn setup() -> Result<()> {
    // parse cli args
    let matches = build_app().get_matches();

    // source .env based on cli arg
    // todo: does this need to be a fancier config file?
    let (dotenv_path, dotenv_err) = if let Some(raw_path) = matches.value_of_os("env") {
        let path = std::path::Path::new(raw_path);
        let r = dotenv::from_path(path);
        (Some(path.to_owned()), r.err())
    } else {
        let r = dotenv::dotenv();
        (r.as_ref().ok().map(std::clone::Clone::clone), r.err())
    };

    // force the config global to load
    CONFIG.initialize();

    // figure out log level and format
    let log_level = matches
        .value_of("log_level")
        .unwrap_or(CONFIG.log_level.as_str());
    let log_format = match matches.value_of("log_format") {
        Some(f) => f.parse::<LogFormat>()?,
        None => CONFIG.log_format,
    };

    let filter = tracing_subscriber::filter::EnvFilter::new(log_level);
    let sub = tracing_subscriber::fmt().with_env_filter(filter);
    match log_format {
        LogFormat::Json => {
            sub.json().init();
        }
        LogFormat::Pretty => {
            sub.init();
        }
    }
    // report any dotenv errors
    if let Some(e) = dotenv_err {
        tracing::error!("error loading dotenv: {e}");
    }
    if let Some(p) = dotenv_path {
        tracing::info!("using .env: {p:?}")
    }
    Ok(())
}

fn build_app() -> clap::Command<'static> {
    clap::command!()
        .arg(clap::arg!([name] "Optional name to operate on"))
        .arg(
            clap::arg!(
                -e --env <FILE> "Sets a custom env file, default: .env"
            )
            .required(false)
            // Support non-UTF8 paths
            .allow_invalid_utf8(true),
        )
        .arg(
            clap::Arg::new("log_level")
                .long("log-level")
                .takes_value(true)
                .value_name("LOG_LEVEL")
                .help("Configure log level (trace|debug|info|warn|error), default: info"),
        )
        .arg(
            clap::Arg::new("log_format")
                .long("log-format")
                .takes_value(true)
                .value_name("LOG_FORMAT")
                .help("Configure log format (pretty|json), default: json"),
        )
        .subcommand(
            clap::Command::new("list-nodes")
                .about("Lists known nodes of cluster")
                .arg(clap::arg!(-n --names "list names only")),
        )
}
