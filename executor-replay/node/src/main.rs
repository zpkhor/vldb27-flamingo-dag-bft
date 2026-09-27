// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{crate_name, crate_version, App, AppSettings, Arg, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, ExecutorId, KeyPair, Parameters, ShardingStrategy, WorkerId};
use consensus::Consensus;
use crypto::PublicKey;
use env_logger::Env;
use network::SimpleSender;
use primary::{ConsensusOutput, Primary, PrimaryWorkerMessage};
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use worker::{Executor, Worker};

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Narwhal and Tusk.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .subcommand(SubCommand::with_name("primary").about("Run a single primary"))
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .subcommand(
                    SubCommand::with_name("executor")
                        .about("Run a single executor")
                        .args_from_usage("--id=<INT> 'The executor id'")
                        .arg(Arg::with_name("validator-range")
                            .long("validator-range")
                            .value_name("START:COUNT")
                            .help("Validator account range (format: start:count). Repeat once per validator.")
                            .required(true)
                            .multiple(true)
                            .takes_value(true)),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => KeyPair::new()
            .export(sub_matches.value_of("filename").unwrap())
            .context("Failed to generate key pair")?,
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

// Runs either a worker or a primary.
async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    // Read the committee and node's keypair from file.
    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let name = keypair.name;
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    // Load default parameters if none are specified.
    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // Make the data store.
    let store = if std::env::var("IN_MEMORY_STORE").as_deref() == Ok("1") {
        Store::new_in_memory()
    } else {
        Store::new(store_path).context("Failed to create a store")?
    };

    // Channels the sequence of consensus output (certificates + migration notices).
    let (tx_output, rx_output) = channel::<ConsensusOutput>(CHANNEL_CAPACITY);

    // Check whether to run a primary, a worker, or an entire authority.
    match matches.subcommand() {
        // Spawn the primary and consensus core.
        ("primary", _) => {
            let replay_mode = !parameters.replay_csv.is_empty();

            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
            Primary::spawn(
                keypair,
                committee.clone(),
                parameters.clone(),
                store,
                tx_new_certificates,
                rx_feedback,
            );

            if replay_mode {
                // Replay mode: no consensus, ReplaySequencer runs inside Primary.
                // Keep process alive.
                std::future::pending::<()>().await;
            } else {
                let analyze_committee = committee.clone();
                Consensus::spawn(
                    name,
                    committee,
                    parameters.gc_depth,
                    rx_new_certificates,
                    tx_feedback,
                    tx_output,
                    parameters.baseline_mode,
                );
                analyze(rx_output, analyze_committee, name).await;
            }
        }

        // Spawn a single worker.
        ("worker", Some(sub_matches)) => {
            let id = sub_matches
                .value_of("id")
                .unwrap()
                .parse::<WorkerId>()
                .context("The worker id must be a positive integer")?;
            Worker::spawn(name, id, committee, parameters, store);
        }

        // Spawn a single executor.
        ("executor", Some(sub_matches)) => {
            let id = sub_matches
                .value_of("id")
                .unwrap()
                .parse::<ExecutorId>()
                .context("The executor id must be a positive integer")?;
            let validator_ranges: Vec<(u64, u64)> = sub_matches
                .values_of("validator-range")
                .unwrap()
                .map(|s| {
                    let parts: Vec<&str> = s.splitn(2, ':').collect();
                    assert!(parts.len() == 2, "--validator-range must be 'start:count', got: {}", s);
                    let start = parts[0].parse::<u64>()
                        .expect("--validator-range start must be a non-negative integer");
                    let count = parts[1].parse::<u64>()
                        .expect("--validator-range count must be a positive integer");
                    (start, count)
                })
                .collect();
            let sharding_strategy =
                ShardingStrategy::from_str(&parameters.sharding_strategy);
            let initial_partition = config::create_initial_partition(
                parameters.num_executors,
                &validator_ranges,
                sharding_strategy,
            );
            Executor::spawn(
                name,
                id,
                committee,
                parameters,
                sharding_strategy,
                initial_partition,
            );
        }
        _ => unreachable!(),
    }

    // For workers, keep the process alive (primary path awaits analyze() forever).
    std::future::pending::<()>().await;
    unreachable!();
}

/// Receives consensus output (certificates + migration notices) and dispatches to our workers. Only for primary
async fn analyze(mut rx_output: Receiver<ConsensusOutput>, committee: Committee, name: PublicKey) {
    let mut network = SimpleSender::new();

    // Build a map from worker_id -> our worker's primary_to_worker address.
    let our_workers: HashMap<WorkerId, _> = committee
        .authorities
        .get(&name)
        .expect("Our key is not in the committee")
        .workers
        .iter()
        .map(|(id, addr)| (*id, addr.primary_to_worker))
        .collect();

    // Monotonically increasing sequence number for Execute dispatch to executors.
    let mut next_sequence: u64 = 0;

    while let Some(output) = rx_output.recv().await {
        match output {
            ConsensusOutput::Certificate(certificate) => {
                // Group committed batch digests by worker_id.
                let mut per_worker: HashMap<WorkerId, Vec<_>> = HashMap::new();
                for (digest, worker_id) in &certificate.header.payload {
                    per_worker
                        .entry(*worker_id)
                        .or_default()
                        .push(digest.clone());
                }

                // Send CommittedBatches and Execute to each of our workers.
                for (worker_id, digests) in per_worker {
                    if let Some(&address) = our_workers.get(&worker_id) {
                        let message = PrimaryWorkerMessage::CommittedBatches(certificate.round(), digests.clone());
                        let bytes = bincode::serialize(&message)
                            .expect("Failed to serialize CommittedBatches");
                        network.send(address, Bytes::from(bytes)).await;

                        // Send Execute for each batch digest with a global sequence number.
                        for digest in digests {
                            let execute_msg = PrimaryWorkerMessage::Execute(digest, worker_id, next_sequence);
                            let execute_bytes = bincode::serialize(&execute_msg)
                                .expect("Failed to serialize Execute");
                            network.send(address, Bytes::from(execute_bytes)).await;
                            next_sequence += 1;
                        }
                    }
                }
            }
            ConsensusOutput::Migrations(notices) => {
                // Distribute migration notices across workers by account_id.
                let num_workers = our_workers.len() as u64;
                assert!(num_workers > 0, "No workers configured");
                let mut per_worker: HashMap<WorkerId, Vec<_>> = HashMap::new();
                for notice in notices {
                    let worker_id = (notice.account_id % num_workers) as WorkerId;
                    per_worker.entry(worker_id).or_default().push(notice);
                }
                for (worker_id, worker_notices) in per_worker {
                    if let Some(&address) = our_workers.get(&worker_id) {
                        for chunk in worker_notices.chunks(primary::MIGRATION_CHUNK_SIZE) {
                            let message = PrimaryWorkerMessage::MigrationNotices(chunk.to_vec());
                            let bytes = bincode::serialize(&message)
                                .expect("Failed to serialize MigrationNotices");
                            network.send(address, Bytes::from(bytes)).await;
                        }
                    }
                }
            }
        }
    }
}
