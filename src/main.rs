use kave::{
    config::LogFormat,
    get_config,
    server::{load_certs, load_keys, Server},
    Result,
};

async fn run() -> Result<()> {
    setup()?;

    let config = get_config();
    tracing::info!(
        "loading ssl certificates: {}, {}",
        config.cert_path,
        config.key_path
    );

    let certs = load_certs(&config.cert_path)?;
    let keys = load_keys(&config.key_path)?;
    tracing::debug!("found {} certs, {} keys", certs.len(), keys.len());

    tracing::info!("initializing");
    let (svr_shutdown_send, mut svr_shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
    let (sig_shutdown_send, sig_shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
    let (store_shutdown_send, store_shutdown_recv) = tokio::sync::mpsc::unbounded_channel();

    let store =
        kave::store::lsm::LSMStore::initialize_from_config(&config, store_shutdown_recv).await?;
    let svr = Server::new(svr_shutdown_send, sig_shutdown_recv, certs, keys, store);
    tokio::spawn(async move { svr.start().await });
    tracing::info!("server spawned");

    let mut shutdown_confirmations = Vec::new();

    let server_initiated_shutdown = tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("handling sigint");
            sig_shutdown_send.send(true).expect("error sending sigint shutdown signal");
            let (send, recv) = tokio::sync::oneshot::channel();
            shutdown_confirmations.push(recv);
            store_shutdown_send.send(send).expect("Error sending shutdown signal");
            false
        },
        _ = svr_shutdown_recv.recv() => {
            true
        },
    };

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        futures::future::join_all(shutdown_confirmations),
    )
    .await?;

    if !server_initiated_shutdown {
        tracing::info!("shutdown initiated, waiting for server shutdown signal");

        if tokio::time::timeout(std::time::Duration::from_secs(5), svr_shutdown_recv.recv())
            .await
            .is_err()
        {
            return Err("server failed to shutdown within 5s timeout".into());
        }
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
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

    let config = get_config();
    // figure out log level and format
    let log_level = matches
        .value_of("log_level")
        .unwrap_or(config.log_level.as_str());
    let log_format = match matches.value_of("log_format") {
        Some(f) => f.parse::<LogFormat>()?,
        None => config.log_format,
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
