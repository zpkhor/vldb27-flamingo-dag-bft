use crate::processor::SerializedBatchMessage;
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use config::{Committee, Stake, WorkerId};
use crypto::{Digest, PublicKey};
use ed25519_dalek::{Digest as _, Sha512};
use futures::future::BoxFuture;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use futures::FutureExt as _;
use log::{info, warn};
use network::{CancelHandler, ReliableSender, SimpleSender};
use primary::QuorumMetrics;
use std::collections::{HashMap, VecDeque};
use std::convert::TryInto as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Toggle: true = f+1 (validity_latency_ms), false = 2f+1 (quorum_latency_ms).
const USE_VALIDITY_FOR_ST: bool = true;
// const USE_VALIDITY_FOR_ST: bool = false; // uncomment for 2f+1

/// Sliding window size for ST history.
const ST_WINDOW: usize = 50;

/// Timeout for proxy to complete forwarded batch broadcast (ms).
const FORWARD_TIMEOUT_MS: u64 = 5000;

/// Ban duration for unresponsive proxies (s).
const BAN_DURATION_SECS: u64 = 5;

/// Maximum concurrent pending forwards per proxy peer.
const MAX_PENDING_PER_PEER: usize = 4;

fn compute_st(qm: &QuorumMetrics) -> u64 {
    let latency = if USE_VALIDITY_FOR_ST {
        qm.validity_latency_ms
    } else {
        qm.quorum_latency_ms
    };
    qm.queue_delay_ms + latency
}

/// Messages routed from WorkerReceiverHandler to StratusManager.
pub enum StratusMessage {
    Forward { batch: Vec<u8>, sender: PublicKey, mb_id: Digest },
    Proof { mb_id: Digest, quorum_metrics: QuorumMetrics },
}

/// Result from a completed proxy broadcast task, sent back to StratusManager.
struct ProxyResult {
    mb_id: Digest,
    sender: PublicKey,
    qm: QuorumMetrics,
}

struct PendingForward {
    batch: SerializedBatchMessage,
    proxy: PublicKey,
    sent_at: Instant,
    /// Must be kept alive until the message is actually sent over TCP.
    /// ReliableSender skips messages whose CancelHandler (oneshot::Receiver) is dropped.
    _forward_handler: CancelHandler,
}

pub struct StratusManager {
    // Identity
    name: PublicKey,
    worker_id: WorkerId,
    committee: Committee,
    store: Store,

    // ST tracking
    st_history: VecDeque<u64>,
    busy_threshold_ms: u64,
    current_st_ms: Arc<AtomicU64>,

    // Ban list
    ban_list: HashMap<PublicKey, Instant>,

    // Pending forwards awaiting LBProof from proxy
    pending_forwards: HashMap<Digest, PendingForward>,

    // Concurrent quorum waiting (replaces sequential QuorumWaiter)
    pending_quorums: FuturesUnordered<BoxFuture<'static, (SerializedBatchMessage, QuorumMetrics)>>,

    // Channels
    /// Sealed batches from BatchMaker (no handlers — BatchMaker skips broadcast in stratus mode).
    rx_batch: Receiver<QuorumWaiterMessage>,
    /// To Processor (after quorum reached or proof received).
    tx_processor: Sender<(SerializedBatchMessage, QuorumMetrics)>,
    /// LB messages from other workers (LBForward, LBProof).
    rx_lb_messages: Receiver<StratusMessage>,
    /// Completed proxy broadcast results (from spawned tasks back to main loop).
    rx_proxy_done: Receiver<ProxyResult>,
    tx_proxy_done: Sender<ProxyResult>,

    // Network
    network: ReliableSender,
    /// Separate network for LBForward messages — must not share connections with
    /// self-broadcast traffic, otherwise forwards get stuck behind queued broadcasts.
    forward_network: ReliableSender,
    simple_network: SimpleSender,
}

impl StratusManager {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        worker_id: WorkerId,
        committee: Committee,
        store: Store,
        busy_threshold_ms: u64,
        current_st_ms: Arc<AtomicU64>,
        rx_batch: Receiver<QuorumWaiterMessage>,
        tx_processor: Sender<(SerializedBatchMessage, QuorumMetrics)>,
        rx_lb_messages: Receiver<StratusMessage>,
    ) {
        let (tx_proxy_done, rx_proxy_done) = channel(1000);
        tokio::spawn(async move {
            Self {
                name,
                worker_id,
                committee,
                store,
                st_history: VecDeque::with_capacity(ST_WINDOW + 1),
                busy_threshold_ms,
                current_st_ms,
                ban_list: HashMap::new(),
                pending_forwards: HashMap::new(),
                pending_quorums: FuturesUnordered::new(),
                rx_batch,
                tx_processor,
                rx_lb_messages,
                rx_proxy_done,
                tx_proxy_done,
                network: ReliableSender::new(),
                forward_network: ReliableSender::new(),
                simple_network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    fn is_busy(&self) -> bool {
        if self.st_history.len() < ST_WINDOW / 2 {
            return false;
        }
        let p95 = self.compute_p95();
        p95 > self.busy_threshold_ms
    }

    fn compute_p95(&self) -> u64 {
        let mut sorted: Vec<u64> = self.st_history.iter().copied().collect();
        sorted.sort_unstable();
        let idx = ((sorted.len() as f64 * 0.95) as usize).min(sorted.len() - 1);
        sorted[idx]
    }

    fn record_st(&mut self, qm: &QuorumMetrics) {
        let st = compute_st(qm);
        self.st_history.push_back(st);
        if self.st_history.len() > ST_WINDOW {
            self.st_history.pop_front();
        }
        self.current_st_ms.store(self.compute_p95(), Ordering::Relaxed);
    }

    fn is_banned(&self, pk: &PublicKey) -> bool {
        self.ban_list.get(pk)
            .map(|t| t.elapsed() < Duration::from_secs(BAN_DURATION_SECS))
            .unwrap_or(false)
    }

    fn ban(&mut self, pk: &PublicKey) {
        self.ban_list.insert(*pk, Instant::now());
    }

    fn batch_digest(batch: &[u8]) -> Digest {
        Digest(Sha512::digest(batch).as_ref()[..32].try_into().unwrap())
    }

    /// Get worker_to_worker addresses of all peers (excluding self), with their public keys.
    fn peer_addresses(&self) -> Vec<(PublicKey, SocketAddr)> {
        self.committee
            .others_workers(&self.name, &self.worker_id)
            .iter()
            .map(|(pk, addr)| (*pk, addr.worker_to_worker))
            .collect()
    }

    /// Look up a peer's worker_to_worker address.
    fn peer_address(&self, pk: &PublicKey) -> Option<SocketAddr> {
        self.committee
            .worker(pk, &self.worker_id)
            .ok()
            .map(|addr| addr.worker_to_worker)
    }

    async fn run(&mut self) {
        let timeout_check_interval = Duration::from_millis(500);
        let mut timeout_timer = tokio::time::interval(timeout_check_interval);
        let mut batch_count: u64 = 0;

        loop {
            tokio::select! {
                // New batch from BatchMaker
                Some(msg) = self.rx_batch.recv() => {
                    let is_busy = self.is_busy();
                    batch_count += 1;
                    if batch_count % 50 == 1 {
                        let p95 = if self.st_history.len() >= ST_WINDOW / 2 { self.compute_p95() } else { 0 };
                        info!(
                            "stratus: batch #{} is_busy={} p95_st={}ms threshold={}ms pending_fwd={} pending_q={}",
                            batch_count, is_busy, p95, self.busy_threshold_ms,
                            self.pending_forwards.len(), self.pending_quorums.len()
                        );
                    }
                    if is_busy {
                        self.forward_load(msg).await;
                    } else {
                        self.self_broadcast(msg).await;
                    }
                }

                // Quorum reached for a self-broadcast batch (concurrent)
                Some((batch, qm)) = self.pending_quorums.next() => {
                    self.record_st(&qm);
                    self.tx_processor.send((batch, qm)).await
                        .expect("Failed to send to Processor");
                }

                // LB messages from other workers
                Some(lb_msg) = self.rx_lb_messages.recv() => {
                    match lb_msg {
                        StratusMessage::Forward { batch, sender, mb_id } => {
                            self.handle_proxy_forward(batch, sender, mb_id).await;
                        }
                        StratusMessage::Proof { mb_id, quorum_metrics } => {
                            self.handle_proof(mb_id, quorum_metrics).await;
                        }
                    }
                }

                // Completed proxy broadcasts (from spawned quorum-wait tasks)
                Some(result) = self.rx_proxy_done.recv() => {
                    self.send_proof_to_sender(result).await;
                }

                // Periodic timeout check
                _ = timeout_timer.tick() => {
                    self.check_timeouts().await;
                }
            }
        }
    }

    /// Broadcast the batch to peers and track quorum concurrently.
    /// Replaces the sequential QuorumWaiter for stratus mode.
    async fn self_broadcast(&mut self, msg: QuorumWaiterMessage) {
        let peers = self.peer_addresses();
        let (names, addresses): (Vec<PublicKey>, Vec<SocketAddr>) = peers.into_iter().unzip();
        let bytes = Bytes::from(msg.batch.clone());
        let batch_sending_enqueue_at = Instant::now();
        let handlers = self.network.broadcast(addresses, bytes).await;

        let committee = self.committee.clone();
        let own_stake = committee.stake(&self.name);
        let batch = msg.batch;

        // Spawn a concurrent quorum-wait future (all batches wait in parallel)
        let fut = async move {
            let quorum_start_at = Instant::now();
            let queue_delay_ms = quorum_start_at.duration_since(batch_sending_enqueue_at).as_millis() as u64;

            let mut wait_for_quorum: FuturesUnordered<_> = names
                .into_iter()
                .zip(handlers.into_iter())
                .map(|(name, handler)| {
                    let stake = committee.stake(&name);
                    async move { let _ = handler.await; stake }
                })
                .collect();

            let mut total_stake: Stake = own_stake;
            let mut validity_reached_at: Option<Instant> = None;
            let quorum_threshold = committee.quorum_threshold();
            let validity_threshold = committee.validity_threshold();

            while let Some(stake) = wait_for_quorum.next().await {
                total_stake += stake;
                if validity_reached_at.is_none() && total_stake >= validity_threshold {
                    validity_reached_at = Some(Instant::now());
                }
                if total_stake >= quorum_threshold {
                    break;
                }
            }

            let quorum_latency_ms = quorum_start_at.elapsed().as_millis() as u64;
            let validity_latency_ms = validity_reached_at
                .map(|t| t.duration_since(quorum_start_at).as_millis() as u64)
                .unwrap_or(quorum_latency_ms);

            #[cfg(feature = "benchmark")]
            {
                let digest = Digest(Sha512::digest(&batch).as_ref()[..32].try_into().unwrap());
                info!(
                    "Quorum for batch {:?} queue_delay {}ms quorum_latency {}ms validity_latency {}ms",
                    digest, queue_delay_ms, quorum_latency_ms, validity_latency_ms
                );
            }

            let qm = QuorumMetrics { queue_delay_ms, quorum_latency_ms, validity_latency_ms };
            (batch, qm)
        };

        self.pending_quorums.push(Box::pin(fut));
    }

    /// Forward batch to a random eligible proxy (no query round-trip).
    async fn forward_load(&mut self, msg: QuorumWaiterMessage) {
        let mb_id = Self::batch_digest(&msg.batch);

        // Count pending forwards per peer for throttling
        let mut pending_counts: HashMap<PublicKey, usize> = HashMap::new();
        for pf in self.pending_forwards.values() {
            *pending_counts.entry(pf.proxy).or_insert(0) += 1;
        }

        // Get eligible peers: not banned, below max concurrent limit
        let eligible: Vec<(PublicKey, SocketAddr)> = self.peer_addresses()
            .into_iter()
            .filter(|(pk, _)| !self.is_banned(pk))
            .filter(|(pk, _)| pending_counts.get(pk).copied().unwrap_or(0) < MAX_PENDING_PER_PEER)
            .collect();

        if eligible.is_empty() {
            // No eligible proxies — broadcast ourselves
            self.self_broadcast(msg).await;
            return;
        }

        // Pick proxy with fewest pending forwards (tie-break random)
        let min_pending = eligible.iter()
            .map(|(pk, _)| pending_counts.get(pk).copied().unwrap_or(0))
            .min().unwrap();
        let best: Vec<_> = eligible.iter()
            .filter(|(pk, _)| pending_counts.get(pk).copied().unwrap_or(0) == min_pending)
            .collect();
        let idx = rand::random::<usize>() % best.len();
        let (proxy_pk, proxy_addr) = *best[idx];

        // Send LBForward to proxy
        let forward = WorkerMessage::LBForward {
            batch: msg.batch.clone(),
            sender: self.name,
            mb_id: mb_id.clone(),
        };
        let bytes = Bytes::from(bincode::serialize(&forward).unwrap());
        let forward_handler = self.forward_network.send(proxy_addr, bytes).await;

        // Store pending forward (keeps CancelHandler alive so ReliableSender doesn't skip it)
        self.pending_forwards.insert(mb_id, PendingForward {
            batch: msg.batch,
            proxy: proxy_pk,
            sent_at: Instant::now(),
            _forward_handler: forward_handler,
        });
    }

    /// Proxy-side: broadcast the forwarded batch using self.network (reuses connections),
    /// then spawn a lightweight task to wait for quorum and report back via channel.
    async fn handle_proxy_forward(&mut self, batch: Vec<u8>, sender: PublicKey, mb_id: Digest) {
        let peers = self.peer_addresses();
        let (names, addresses): (Vec<PublicKey>, Vec<SocketAddr>) = peers.into_iter().unzip();
        let bytes = Bytes::from(batch.clone());
        let batch_sending_enqueue_at = Instant::now();
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Store the batch (proxy must have it for sync requests)
        let digest = Self::batch_digest(&batch);
        self.store.write(digest.to_vec(), batch).await;

        // Spawn lightweight task to wait for quorum (only awaits handlers, no new connections)
        let committee = self.committee.clone();
        let name = self.name;
        let tx_proxy_done = self.tx_proxy_done.clone();

        tokio::spawn(async move {
            let quorum_start_at = Instant::now();
            let queue_delay_ms = quorum_start_at.duration_since(batch_sending_enqueue_at).as_millis() as u64;

            let mut wait_for_quorum: FuturesUnordered<_> = names
                .into_iter()
                .zip(handlers.into_iter())
                .map(|(name, handler)| {
                    let stake = committee.stake(&name);
                    async move { let _ = handler.await; stake }
                })
                .collect();

            let own_stake = committee.stake(&name);
            let mut total_stake: Stake = own_stake;
            let mut validity_reached_at: Option<Instant> = None;
            let quorum_threshold = committee.quorum_threshold();
            let validity_threshold = committee.validity_threshold();

            let forward_timeout = Duration::from_millis(FORWARD_TIMEOUT_MS);
            let deadline = tokio::time::Instant::now() + forward_timeout;

            let mut quorum_reached = false;
            while let Ok(Some(stake)) = tokio::time::timeout_at(deadline, wait_for_quorum.next()).await {
                total_stake += stake;
                if validity_reached_at.is_none() && total_stake >= validity_threshold {
                    validity_reached_at = Some(Instant::now());
                }
                if total_stake >= quorum_threshold {
                    quorum_reached = true;
                    break;
                }
            }

            if !quorum_reached {
                warn!(
                    "stratus: proxy failed quorum for {:?} (stake={}/{})",
                    mb_id, total_stake, quorum_threshold
                );
                return;
            }

            let quorum_latency_ms = quorum_start_at.elapsed().as_millis() as u64;
            let validity_latency_ms = validity_reached_at
                .map(|t| t.duration_since(quorum_start_at).as_millis() as u64)
                .unwrap_or(quorum_latency_ms);

            let qm = QuorumMetrics {
                queue_delay_ms,
                quorum_latency_ms,
                validity_latency_ms,
            };

            let _ = tx_proxy_done.send(ProxyResult { mb_id, sender, qm }).await;
        });
    }

    /// Send LBProof back to the original sender after proxy quorum is reached.
    async fn send_proof_to_sender(&mut self, result: ProxyResult) {
        let sender_addr = match self.peer_address(&result.sender) {
            Some(addr) => addr,
            None => {
                warn!("stratus: sender {} address not found", result.sender);
                return;
            }
        };
        let proof = WorkerMessage::LBProof {
            mb_id: result.mb_id,
            quorum_metrics: result.qm,
        };
        let bytes = Bytes::from(bincode::serialize(&proof).unwrap());
        self.simple_network.send(sender_addr, bytes).await;
    }

    /// Sender receives proof that proxy achieved quorum.
    async fn handle_proof(&mut self, mb_id: Digest, quorum_metrics: QuorumMetrics) {
        let pending = match self.pending_forwards.remove(&mb_id) {
            Some(p) => p,
            None => {
                warn!("stratus: received proof for unknown batch {:?}", mb_id);
                return;
            }
        };

        // Log quorum line compatible with the log parser's stage breakdown.
        #[cfg(feature = "benchmark")]
        {
            let digest = Self::batch_digest(&pending.batch);
            info!(
                "Quorum for batch {:?} queue_delay {}ms quorum_latency {}ms validity_latency {}ms",
                digest, quorum_metrics.queue_delay_ms, quorum_metrics.quorum_latency_ms, quorum_metrics.validity_latency_ms
            );
        }

        // Remove proxy from ban list (success)
        self.ban_list.remove(&pending.proxy);

        // Do NOT record proxy's quorum metrics as our ST — proxy's low latency
        // would make us think we're not busy, causing oscillation.

        // Send batch + metrics to Processor for hashing, storage, and primary notification
        self.tx_processor
            .send((pending.batch, quorum_metrics))
            .await
            .expect("Failed to send to Processor");
    }

    /// Check for timed-out forwarded batches and fall back to self-broadcast.
    async fn check_timeouts(&mut self) {
        let timeout = Duration::from_millis(FORWARD_TIMEOUT_MS);
        let timed_out: Vec<Digest> = self.pending_forwards
            .iter()
            .filter(|(_, p)| p.sent_at.elapsed() > timeout)
            .map(|(d, _)| d.clone())
            .collect();

        for mb_id in timed_out {
            let pending = self.pending_forwards.remove(&mb_id).unwrap();
            warn!(
                "stratus: forward timeout for batch {:?} (proxy {}), falling back to self-broadcast",
                mb_id, pending.proxy
            );
            let peers = self.peer_addresses();
            let (names, addresses): (Vec<PublicKey>, Vec<SocketAddr>) = peers.into_iter().unzip();
            let bytes = Bytes::from(pending.batch.clone());
            let batch_sending_enqueue_at = std::time::Instant::now();
            let handlers = self.network.broadcast(addresses, bytes).await;

            // Re-use self_broadcast logic but with pre-existing batch
            let committee = self.committee.clone();
            let own_stake = committee.stake(&self.name);
            let batch = pending.batch;

            let fut = async move {
                let quorum_start_at = Instant::now();
                let queue_delay_ms = quorum_start_at.duration_since(batch_sending_enqueue_at).as_millis() as u64;

                let mut wait_for_quorum: FuturesUnordered<_> = names
                    .into_iter()
                    .zip(handlers.into_iter())
                    .map(|(name, handler)| {
                        let stake = committee.stake(&name);
                        async move { let _ = handler.await; stake }
                    })
                    .collect();

                let mut total_stake: Stake = own_stake;
                let mut validity_reached_at: Option<Instant> = None;
                let quorum_threshold = committee.quorum_threshold();
                let validity_threshold = committee.validity_threshold();

                while let Some(stake) = wait_for_quorum.next().await {
                    total_stake += stake;
                    if validity_reached_at.is_none() && total_stake >= validity_threshold {
                        validity_reached_at = Some(Instant::now());
                    }
                    if total_stake >= quorum_threshold {
                        break;
                    }
                }

                let quorum_latency_ms = quorum_start_at.elapsed().as_millis() as u64;
                let validity_latency_ms = validity_reached_at
                    .map(|t| t.duration_since(quorum_start_at).as_millis() as u64)
                    .unwrap_or(quorum_latency_ms);

                #[cfg(feature = "benchmark")]
                {
                    let digest = Digest(Sha512::digest(&batch).as_ref()[..32].try_into().unwrap());
                    info!(
                        "Quorum for batch {:?} queue_delay {}ms quorum_latency {}ms validity_latency {}ms",
                        digest, queue_delay_ms, quorum_latency_ms, validity_latency_ms
                    );
                }

                let qm = QuorumMetrics { queue_delay_ms, quorum_latency_ms, validity_latency_ms };
                (batch, qm)
            };

            self.pending_quorums.push(Box::pin(fut));
        }
    }
}
