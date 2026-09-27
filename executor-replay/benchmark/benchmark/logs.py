# Copyright(C) Facebook, Inc. and its affiliates.
from collections import defaultdict
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import basename, join
from re import findall, search
from statistics import mean, quantiles, stdev

from benchmark.utils import Print


class ParseError(Exception):
    pass


def to_posix(string):
    x = datetime.fromisoformat(string.replace('Z', '+00:00'))
    return datetime.timestamp(x)


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0,
                 workers_by_validator=None, clients_by_validator=None,
                 duration=None, warmup=0, verbose=False, executor_logs=None):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        # Parse executor logs: executor_logs is a dict {(validator_id, executor_id): content}
        # Result: self.executor_total_executed[(validator_id, executor_id)] = final cumulative count
        self.executor_total_executed = {}
        if executor_logs:
            for (v, e), content in executor_logs.items():
                matches = findall(r'cumulative total_executed (\d+)', content)
                self.executor_total_executed[(v, e)] = int(matches[-1]) if matches else 0

        self.faults = faults
        self.bench_duration = float(duration) if duration is not None else None
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers =  len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples, e2e_completed_list \
            = zip(*results)
        self.misses = sum(misses)
        # Merge e2e completed timestamps across all clients
        self.e2e_completed = {}
        for d in e2e_completed_list:
            self.e2e_completed.update(d)
        _client_cache = {id(log): r for log, r in zip(clients, results)}

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, commits, certified, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results_unique([x.items() for x in proposals])
        self.commits = self._merge_results_fplus1([x.items() for x in commits])
        self.certified = self._merge_results_unique([x.items() for x in certified])

        # Map each primary's validator key to its integer ID (sort order = validator ID)
        key_to_id = {}
        for i, p_log in enumerate(primaries):
            m = search(r'Primary (\S+) successfully booted', p_log)
            if m:
                key_to_id[m.group(1)] = i

        raw_timeline = self._parse_primaries_dags(primaries[0])
        raw_certified = self._parse_certified_tps(primaries[0])
        raw_migration = self._parse_migration_tallies(primaries[0])

        # Assign IDs to faulty validators referenced in logs but not booted
        all_keys = set()
        for row in raw_timeline.values():
            all_keys.update(row.keys())
        for row in raw_certified.values():
            all_keys.update(row.keys())
        all_keys.update(raw_migration.keys())
        next_id = len(primaries)
        for k in sorted(all_keys - set(key_to_id)):
            key_to_id[k] = next_id
            next_id += 1

        self.dag_timeline = {
            sr: {key_to_id[k]: v for k, v in row.items()}
            for sr, row in raw_timeline.items()
        }

        self.certified_tps_timeline = {
            r: {key_to_id[k]: v for k, v in row.items()}
            for r, row in raw_certified.items()
        }

        self.migration_tallies = {
            key_to_id[k]: v for k, v in raw_migration.items()
        }

        # Warmup trimming: discard commits/proposals in the warmup window.
        self.warmup = warmup
        self.verbose = verbose
        if warmup and self.start:
            cutoff = min(self.start) + warmup
            self.commits = {d: t for d, t in self.commits.items() if t >= cutoff}
            end_cutoff = min(self.start) + self.bench_duration - warmup / 2
            self.commits = {d: t for d, t in self.commits.items() if t < end_cutoff}
            self.proposals = {d: t for d, t in self.proposals.items() if d in self.commits}
        self.effective_start = cutoff if (warmup and self.start) else min(self.start)

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.sample_to_batch, workers_ips, \
            arrival_times_list, seal_times_list, quorum_times_list, processed_times_list, \
            committed_times_list, queue_delay_list, quorum_latency_list \
            = zip(*results)
        _worker_cache = {id(log): r for log, r in zip(workers, results)}
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        # Merge stage timing dicts across all workers.
        self.arrival_times = {k: v for d in arrival_times_list for k, v in d.items()}
        self.seal_times = {k: v for d in seal_times_list for k, v in d.items()}
        self.quorum_times = {k: v for d in quorum_times_list for k, v in d.items()}
        self.processed_times = {k: v for d in processed_times_list for k, v in d.items()}

        # Each worker contributes one timestamp per batch (the earliest it logged).
        # Result: per batch, a list of timestamps — one per validator/worker.
        self.committed_times_by_batch = defaultdict(list)
        for d in committed_times_list:
            for batch_id, earliest_time in d.items():
                self.committed_times_by_batch[batch_id].append(earliest_time)

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Parse per-validator data if available.
        self.sizes_by_validator = {}
        self.sample_to_batch_by_validator = {}
        self.sent_samples_by_validator = {}
        self.misses_by_validator = {}
        self.queue_delay_by_validator = {}
        self.quorum_latency_by_validator = {}
        if workers_by_validator:
            for v, logs in workers_by_validator.items():
                v_sizes = {}
                v_received_list = []
                v_queue_delay = {}
                v_quorum_latency = {}
                for log in logs:
                    s, r, _, _, _, _, _, _, qd, ql = _worker_cache[id(log)]
                    v_sizes.update(s)
                    v_received_list.append(r)
                    v_queue_delay.update(qd)
                    v_quorum_latency.update(ql)
                self.sizes_by_validator[v] = v_sizes
                self.sample_to_batch_by_validator[v] = v_received_list
                self.queue_delay_by_validator[v] = v_queue_delay
                self.quorum_latency_by_validator[v] = v_quorum_latency
        if clients_by_validator:
            for v, logs in clients_by_validator.items():
                v_sent_list = []
                v_misses = 0
                for log in logs:
                    _, _, _, misses, samples, _ = _client_cache[id(log)]
                    v_sent_list.append(samples)
                    v_misses += misses
                self.sent_samples_by_validator[v] = v_sent_list
                self.misses_by_validator[v] = v_misses

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _merge_results_unique(self, input):
        # Each key should come from exactly one source (one primary).
        merged = {}
        for x in input:
            for k, v in x:
                assert k not in merged, f'Duplicate key {k} during unique merge'
                merged[k] = v
        return merged

    def _merge_results_fplus1(self, input):
        # Use the (f+1)th earliest timestamp (f=1, so 2nd earliest).
        # A commit is only meaningful once f+1 validators have committed it.
        collected = {}
        for x in input:
            for k, v in x:
                collected.setdefault(k, []).append(v)
        merged = {}
        for k, timestamps in collected.items():
            timestamps.sort()
            # Pick the 2nd earliest (index 1) if available, else the earliest
            merged[k] = timestamps[min(1, len(timestamps) - 1)]
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+) account (\d+)', log)
        samples = {(int(s), int(a)): self._to_posix(t) for t, s, a in tmp}

        # Parse e2e completion (executor reply received)
        tmp = findall(r'\[(.*Z) .* Sample transaction (\d+) completed with (\d+) confirmations', log)
        e2e_completed = {int(s): self._to_posix(t) for t, s, _ in tmp}

        return size, rate, start, misses, samples, e2e_completed

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Certified B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        certified = self._merge_results([tmp])

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        
        return proposals, commits, certified, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+) account (\d+)', log)
        samples = {(int(s), int(a)): d for d, s, a in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        # Stage 1: Worker arrival timestamps for sample txs
        tmp = findall(r'\[(.*Z) .* Worker received sample tx (\d+) account (\d+)', log)
        arrival_times = {(int(s), int(a)): self._to_posix(t) for t, s, a in tmp}

        # Stage 2: Batch seal timestamps (from "Batch X contains Y B" log)
        tmp = findall(r'\[(.*Z) .* Batch ([^ ]+) contains \d+ B', log)
        seal_times = {d: self._to_posix(t) for t, d in tmp}

        # Stage 3: Quorum achieved timestamps
        tmp = findall(r'\[(.*Z) .* Quorum for batch (\S+) queue_delay (\d+)ms quorum_latency (\d+)ms', log)
        quorum_times = {d: self._to_posix(t) for t, d, _, __ in tmp}
        queue_delay_by_batch = {d: int(q) for _, d, q, __ in tmp}
        quorum_latency_by_batch = {d: int(l) for _, d, __, l in tmp}

        # Stage 4: Processed batch timestamps
        tmp = findall(r'\[(.*Z) .* Processed batch (\S+)', log)
        processed_times = {d: self._to_posix(t) for t, d in tmp}

        # Stage 5: Earliest committed timestamp per batch (one entry per worker/validator)
        tmp = findall(r'\[(.*Z) .* Committed sample tx \d+ from batch (\S+)', log)
        committed_times_by_batch = {}
        for t, full_digest in tmp:
            ts = self._to_posix(t)
            if full_digest not in committed_times_by_batch or ts < committed_times_by_batch[full_digest]:
                committed_times_by_batch[full_digest] = ts

        return sizes, samples, ip, arrival_times, seal_times, quorum_times, processed_times, committed_times_by_batch, queue_delay_by_batch, quorum_latency_by_batch

    def _to_posix(self, string):
        return to_posix(string)

    def _calculate_latency_metrics(self, latency_list):
        """Calculate mean, p50, p90, p95, and p99 latency from a list of latencies."""
        if not latency_list:
            return {'mean': 0, 'p50': 0, 'p90': 0, 'p95': 0, 'p99': 0}

        result = {'mean': mean(latency_list)}

        if len(latency_list) >= 20:
            # quantiles(data, n=100) gives 99 cut points at 1st through 99th percentile
            q = quantiles(latency_list, n=100)
            result['p50'] = q[49]
            result['p90'] = q[89]
            result['p95'] = q[94]
            result['p99'] = q[98]
        else:
            sorted_list = sorted(latency_list)
            result['p50'] = sorted_list[len(sorted_list) // 2]
            result['p90'] = max(latency_list)
            result['p95'] = max(latency_list)
            result['p99'] = max(latency_list)

        return result

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return self._calculate_latency_metrics(latency)

    def _e2e_latency(self):
        """Compute end-to-end latency: client send -> executor reply received (f+1)."""
        if not self.e2e_completed:
            return None

        # Build sent map: counter -> earliest sent_time (client sends same counter
        # to multiple validators with different accounts; use earliest send)
        global_sent = {}
        for sent in self.sent_samples:
            for (counter, _acct), t in sent.items():
                if counter not in global_sent or t < global_sent[counter]:
                    global_sent[counter] = t

        latency = []
        for counter, completed_time in self.e2e_completed.items():
            if counter in global_sent:
                send_time = global_sent[counter]
                if send_time < self.effective_start:
                    continue
                lat = (completed_time - send_time) * 1000  # ms
                if lat > 0:
                    latency.append(lat)

        if not latency:
            return None
        return self._calculate_latency_metrics(latency)

    def _end_to_end_throughput(self):
        """Compute e2e throughput: from effective_start to last executor-reply completion.

        Each completed sample represents (rate / PRECISION) actual transactions,
        since 1 sample is sent per burst and there are PRECISION bursts/sec.
        Only samples sent after warmup are counted.
        """
        if not self.e2e_completed or not self.commits:
            return 0, 0, 0
        PRECISION = 20  # bursts/sec, matches benchmark_client PRECISION constant

        # Build sent map for warmup filtering (earliest send per counter)
        global_sent = {}
        for sent in self.sent_samples:
            for (counter, _acct), t in sent.items():
                if counter not in global_sent or t < global_sent[counter]:
                    global_sent[counter] = t

        # Only count completions whose send time is after warmup
        valid_completed = {
            c: t for c, t in self.e2e_completed.items()
            if c in global_sent and global_sent[c] >= self.effective_start
        }
        if not valid_completed:
            return 0, 0, 0

        start = self.effective_start
        end = max(valid_completed.values())
        duration = end - start
        if duration <= 0:
            return 0, 0, 0

        num_completed = len(valid_completed)
        sample_multiplier = self.rate[0] / PRECISION
        bytes_represented = num_completed * sample_multiplier * self.size[0]
        bps = bytes_represented / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _committed_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = self.effective_start, max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _sent_to_seal_latency(self):
        global_sent = {}
        for sent in self.sent_samples:
            global_sent.update(sent)

        latency = []
        for received in self.sample_to_batch:
            for key, batch_id in received.items():
                if batch_id not in self.commits:
                    continue
                sent = global_sent.get(key)
                seal = self.seal_times.get(batch_id)
                if sent < self.effective_start:
                    continue
                assert sent is not None and seal is not None, "Committed txs must have been sent and sealed"
                latency.append(seal - sent)
        return self._calculate_latency_metrics(latency)

    def _seal_to_quorum_latency(self):
        latency = []
        for batch_id in self.commits:
            seal = self.seal_times.get(batch_id)
            quorum = self.quorum_times.get(batch_id)
            if seal < self.effective_start:
                continue
            assert seal is not None and quorum is not None, "Committed batches must have seal and quorum times"
            latency.append(quorum - seal)
        return self._calculate_latency_metrics(latency)

    def _worker_committed_latency(self):
        if not isinstance(self.faults, int):
            return {'mean': 0, 'p50': 0, 'p90': 0, 'p95': 0, 'p99': 0}
        # BFT f+1 threshold: n = 3f+1 → f = (n-1)/3 → f+1 = (n-1)//3 + 1
        threshold = (self.committee_size - 1) // 3 + 1

        global_sent = {}
        for sent in self.sent_samples:
            global_sent.update(sent)

        latency = []
        for received in self.sample_to_batch:
            for key, batch_id in received.items():
                if batch_id not in self.commits:
                    continue
                send_time = global_sent.get(key)
                if send_time is None or send_time < self.effective_start:
                    continue
                timestamps = self.committed_times_by_batch.get(batch_id, [])
                if len(timestamps) < threshold:
                    continue
                end_time = sorted(timestamps)[threshold - 1] # (f+1)-th earliest
                latency.append(end_time - send_time)
        return self._calculate_latency_metrics(latency)

    def _validator_load_distribution(self):
        result = {}
        for v, v_sizes in sorted(self.sizes_by_validator.items()):
            committed_bytes = sum(
                s for d, s in v_sizes.items() if d in self.commits
            )
            tx_count = committed_bytes // self.size[0]
            result[v] = tx_count
        total = sum(result.values())
        percentages = {
            v: (count / total * 100 if total else 0)
            for v, count in result.items()
        }
        return result, percentages

    def _executor_load_distribution(self):
        # Aggregate by executor index (averaged across validators, since all process the same global set).
        # Returns (counts_by_executor_idx, percentages_by_executor_idx) where counts are averaged.
        if not self.executor_total_executed:
            return None, None
        by_executor = defaultdict(list)
        for (v, e), count in self.executor_total_executed.items():
            by_executor[e].append(count)
        avg_counts = {e: sum(counts) // len(counts) for e, counts in sorted(by_executor.items())}
        total = sum(avg_counts.values())
        percentages = {e: (c / total * 100 if total else 0) for e, c in avg_counts.items()}
        return avg_counts, percentages

    def _per_validator_committed_tps(self):
        if not self.commits:
            return {}
        start, end = self.effective_start, max(self.commits.values())
        duration = end - start
        if duration == 0:
            return {}
        result = {}
        for v, v_sizes in sorted(self.sizes_by_validator.items()):
            committed_bytes = sum(
                s for d, s in v_sizes.items() if d in self.commits
            )
            result[v] = (committed_bytes / duration) / self.size[0]
        return result

    def _per_validator_committed_latency(self):
        global_sent = {}
        for sent in self.sent_samples:
            global_sent.update(sent)

        result = {}
        for v in sorted(self.sizes_by_validator.keys()):
            v_received_list = self.sample_to_batch_by_validator.get(v, [])
            latencies = []
            for received in v_received_list:
                for key, batch_id in received.items():
                    if batch_id in self.commits:
                        assert key in global_sent  # We receive txs that we sent.
                        start = global_sent[key]
                        if start < self.effective_start:
                            continue
                        latencies.append(self.commits[batch_id] - start)
            if latencies:
                result[v] = self._calculate_latency_metrics(latencies)
        return result

    def _global_stage_latency_breakdown(self):
        global_sent = {}
        for sent in self.sent_samples:
            global_sent.update(sent)

        stage_labels = [
            'Client -> Worker',
            'Worker -> Batch seal',
            'Batch seal -> Quorum',
            'Quorum -> Processed',
            'Processed -> Header',
            'Header -> Certified',
            'Certified -> Committed',
        ]
        stages = {label: [] for label in stage_labels}

        for received in self.sample_to_batch:
            for key, batch_id in received.items():
                if batch_id not in self.commits:
                    continue
                send_time = global_sent.get(key)
                if send_time is None or send_time < self.effective_start:
                    continue

                arrival = self.arrival_times.get(key)
                if arrival is not None:
                    stages['Client -> Worker'].append(arrival - send_time)

                seal = self.seal_times.get(batch_id)
                if arrival is not None and seal is not None:
                    stages['Worker -> Batch seal'].append(seal - arrival)

        for batch_id in self.commits:
            seal = self.seal_times.get(batch_id)
            quorum = self.quorum_times.get(batch_id)
            processed = self.processed_times.get(batch_id)
            proposed = self.proposals.get(batch_id)
            certified = self.certified.get(batch_id)
            committed = self.commits[batch_id]

            if seal is not None and quorum is not None:
                stages['Batch seal -> Quorum'].append(quorum - seal)

            if quorum is not None and processed is not None:
                stages['Quorum -> Processed'].append(processed - quorum)

            if processed is not None and proposed is not None:
                stages['Processed -> Header'].append(proposed - processed)

            if proposed is not None and certified is not None:
                stages['Header -> Certified'].append(certified - proposed)

            if certified is not None:
                stages['Certified -> Committed'].append(committed - certified)

        result = {}
        for label, latencies in stages.items():
            if latencies:
                metrics = self._calculate_latency_metrics(latencies)
                result[label] = {
                    'mean': metrics['mean'],
                    'p50': metrics['p50'],
                    'p95': metrics['p95'],
                    'p99': metrics['p99'],
                    'count': len(latencies),
                }
            else:
                result[label] = {'mean': 0, 'p50': 0, 'p95': 0, 'p99': 0, 'count': 0}
        return {'all': result}

    def _per_validator_stage_latency_breakdown(self):
        global_sent = {}
        for sent in self.sent_samples:
            global_sent.update(sent)

        stage_labels = [
            'Client -> Worker',
            'Worker -> Batch seal',
            'Batch seal -> Quorum',
            'Quorum -> Processed',
            'Processed -> Header',
            'Header -> Certified',
            'Certified -> Committed',
        ]

        if self.sizes_by_validator:
            validators = sorted(self.sizes_by_validator.keys())
        else:
            validators = ['all']

        result = {}
        for v in validators:
            stages = {label: [] for label in stage_labels}

            if v == 'all':
                v_committed_batches = set(self.commits.keys())
                v_received_list = self.sample_to_batch
            else:
                v_committed_batches = {
                    d for d in self.sizes_by_validator[v] if d in self.commits
                }
                v_received_list = self.sample_to_batch_by_validator.get(v, [])

            # Stages 1-2: keyed by sample tx
            for received in v_received_list:
                for key, batch_id in received.items():
                    if batch_id not in self.commits:
                        continue
                    send_time = global_sent[key]
                    if send_time < self.effective_start:
                        continue

                    arrival = self.arrival_times[key]
                    stages['Client -> Worker'].append(arrival - send_time)

                    seal = self.seal_times[batch_id]
                    stages['Worker -> Batch seal'].append(seal - arrival)

            # Stages 3-7: keyed by batch digest
            for batch_id in v_committed_batches:
                seal = self.seal_times[batch_id]
                quorum = self.quorum_times[batch_id]
                processed = self.processed_times[batch_id]
                proposed = self.proposals[batch_id]
                certified = self.certified.get(batch_id)
                committed = self.commits[batch_id]

                if seal is not None and quorum is not None:
                    stages['Batch seal -> Quorum'].append(quorum - seal)

                if quorum is not None and processed is not None:
                    stages['Quorum -> Processed'].append(processed - quorum)

                if processed is not None and proposed is not None:
                    stages['Processed -> Header'].append(proposed - processed)

                if proposed is not None and certified is not None:
                    stages['Header -> Certified'].append(certified - proposed)

                if certified is not None:
                    stages['Certified -> Committed'].append(committed - certified)

            v_result = {}
            for label, latencies in stages.items():
                if latencies:
                    metrics = self._calculate_latency_metrics(latencies)
                    v_result[label] = {
                        'mean': metrics['mean'],
                        'p50': metrics['p50'],
                        'p95': metrics['p95'],
                        'p99': metrics['p99'],
                        'count': len(latencies),
                    }
                else:
                    v_result[label] = {'mean': 0, 'p50': 0, 'p95': 0, 'p99': 0, 'count': 0}
            result[v] = v_result

        return result

    def _format_stage_matrix(self, title, metric_key, stage_data):
        validators = sorted(stage_data.keys())
        stage_labels = [
            'Client -> Worker',
            'Worker -> Batch seal',
            'Batch seal -> Quorum',
            'Quorum -> Processed',
            'Processed -> Header',
            'Header -> Certified',
            'Certified -> Committed',
        ]
        col_w = 8
        label_w = 28

        v_headers = [f'V{v}' if v != 'all' else 'All' for v in validators]
        header_row = f'   {"Stage":<{label_w}}' + ''.join(f'{h:>{col_w}}' for h in v_headers)
        output = f'\n + {title}:\n{header_row}\n'

        sums = {v: 0.0 for v in validators}
        for label in stage_labels:
            row = f'   {label + ":":<{label_w}}'
            for v in validators:
                val_ms = stage_data[v][label][metric_key] * 1000
                sums[v] += val_ms
                row += f'{round(val_ms):>{col_w},}'
            output += row + '\n'

        # Sum of per-stage p95s, not the p95 of the commit distribution.
        # Sum of percentiles >= percentile of sum (stages are not perfectly correlated).
        sum_row = (f'   {"Sum:":<{label_w}}'
                   + ''.join(f'{round(sums[v]):>{col_w},}' for v in validators))
        output += sum_row + '\n'

        count_row = f'   {"(n=)":<{label_w}}'
        for v in validators:
            count = stage_data[v]['Client -> Worker']['count']
            if count == 0:
                count = stage_data[v]['Batch seal -> Quorum']['count']
            count_row += f'{count:>{col_w},}'
        output += count_row + '\n'

        return output

    def _parse_primaries_dags(self, log):
        """Parse 'stable_account_counts' entries from one primary log.
        Returns {safe_round: {validator_key_str: (total, tps, tpr, tpx)}}.
        """
        pattern = r'stable_account_counts \(safe_round=(\d+)\) validator (\S+): total=(\d+) tx/s=([\d.]+) tx/r=(\d+) tx/x=([\d.]+)'
        result = {}
        for safe_round_s, key, total_s, tps_s, tpr_s, tpx_s in findall(pattern, log):
            result.setdefault(int(safe_round_s), {})[key] = (int(total_s), float(tps_s), int(tpr_s), float(tpx_s))
        return result

    def _parse_certified_tps(self, log):
        """Parse 'certified_tps' entries from one primary log.
        Returns {round: {validator_key_str: (tps, capacity, spare)}}.
        """
        pattern = r'certified_tps \(round=(\d+)\) validator (\S+): ([\d.]+) tx/s \(capacity: (\d+) tx/s, spare: (-?[\d.]+)\)'
        result = {}
        for round_s, key, tps_s, cap_s, spare_s in findall(pattern, log):
            result.setdefault(int(round_s), {})[key] = (float(tps_s), int(cap_s), float(spare_s))
        return result

    def _parse_migration_tallies(self, log):
        pattern = r'reroute \(round=\d+\) migration_tally (\S+): donated=(\d+) received=(\d+)'
        cumulative = {}
        for key, donated_s, received_s in findall(pattern, log):
            prev = cumulative.get(key, (0, 0))
            cumulative[key] = (prev[0] + int(donated_s), prev[1] + int(received_s))
        return cumulative

    def _format_dag_timeline(self):
        if not self.dag_timeline:
            return ''
        sorted_rounds = sorted(self.dag_timeline.keys())
        all_validators = sorted({v for row in self.dag_timeline.values() for v in row})
        if not all_validators:
            return ''
        label_w = max(len('safe_r'), max(len(str(sr)) for sr in sorted_rounds))

        def make_cell(entry, row_total):
            val, tps, tpr, tpx = entry
            pct = f'{val / row_total * 100:.0f}%' if row_total else '-%'
            return f'{val if self.verbose else ""} ({pct}) {tps:.0f}t/s {tpr}t/r {tpx:.0f}t/x'

        cells = []
        for sr in sorted_rounds:
            row_entries = [self.dag_timeline[sr].get(v, (0, 0.0, 0, 0.0)) for v in all_validators]
            row_total = sum(e[0] for e in row_entries)
            cells.append([make_cell(e, row_total) for e in row_entries])

        col_w = max(max(len(c) for row in cells for c in row), max(len('V'+str(v)) for v in all_validators)) + 2

        header = f'   {"safe_r":<{label_w}}' + ''.join(f'{"V"+str(v):>{col_w}}' for v in all_validators)
        output = f'\n + DAG TIMELINE (t/s=self-reported, t/r=per-round, t/x=median-duration):\n{header}\n'
        for i, sr in enumerate(sorted_rounds):
            row_str = f'   {sr:<{label_w}}'
            for cell in cells[i]:
                row_str += f'{cell:>{col_w}}'
            output += row_str + '\n'
        return output

    def _format_migration_section(self):
        if not self.migration_tallies:
            return ''
        lines = ['\n + ACCOUNT MIGRATIONS (cumulative):\n']
        lines.append(f' {"Validator":<12} {"Donated":>10} {"Received":>10}\n')
        for v in sorted(self.migration_tallies.keys()):
            donated, received = self.migration_tallies[v]
            lines.append(f' {v:<12} {donated:>10,} {received:>10,}\n')
        return ''.join(lines)

    def _format_certified_tps_timeline(self):
        if not self.certified_tps_timeline:
            return ''
        sorted_rounds = sorted(self.certified_tps_timeline.keys())
        all_validators = sorted({v for row in self.certified_tps_timeline.values() for v in row})
        if not all_validators:
            return ''
        label_w = max(len('round'), max(len(str(r)) for r in sorted_rounds))

        def make_cell(entry, pct):
            tps, cap, spare = entry
            return f'{pct:.1f}% {tps:.0f}/{cap} ({spare:+.0f})'

        cells = []
        for r in sorted_rounds:
            row_entries = [self.certified_tps_timeline[r].get(v, (0.0, 0, 0.0)) for v in all_validators]
            total_tps = sum(e[0] for e in row_entries) or 1.0
            cells.append([make_cell(e, e[0] / total_tps * 100) for e in row_entries])

        col_w = max(max(len(c) for row in cells for c in row), max(len('V'+str(v)) for v in all_validators)) + 2

        header = f'   {"round":<{label_w}}' + ''.join(f'{"V"+str(v):>{col_w}}' for v in all_validators)
        output = f'\n + CERTIFIED TPS TIMELINE (actual/capacity (spare)):\n{header}\n'
        for i, r in enumerate(sorted_rounds):
            row_str = f'   {r:<{label_w}}'
            for cell in cells[i]:
                row_str += f'{cell:>{col_w}}'
            output += row_str + '\n'
        return output

    def _per_validator_quorum_timing(self):
        result = {}
        for v in sorted(self.queue_delay_by_validator.keys()):
            delays = [ms for d, ms in self.queue_delay_by_validator[v].items() if d in self.commits]
            latencies = [ms for d, ms in self.quorum_latency_by_validator[v].items() if d in self.commits]
            delay_metrics = self._calculate_latency_metrics(delays)
            latency_metrics = self._calculate_latency_metrics(latencies)
            result[v] = {
                'queue_delay_mean': delay_metrics['mean'],
                'queue_delay_p95': delay_metrics['p95'],
                'queue_delay_std': round(stdev(delays)) if len(delays) >= 2 else 0,
                'quorum_latency_mean': latency_metrics['mean'],
                'quorum_latency_p95': latency_metrics['p95'],
                'quorum_latency_std': round(stdev(latencies)) if len(latencies) >= 2 else 0,
            }
        return result

    def _format_warnings_section(self):
        _, _, consensus_duration = self._consensus_throughput()
        _, _, commit_duration = self._committed_throughput()
        assert isinstance(self.bench_duration, float), 'Bench duration is not set'
        effective_bench_duration = self.bench_duration - self.warmup - self.warmup / 2
        warnings = []
        if (consensus_duration / effective_bench_duration) < 0.9:
            warnings.append('Consensus stalled the system')
        if (commit_duration / effective_bench_duration) < 0.9:
            warnings.append('Commit stalled the system')
        if not warnings:
            return ''
        return '\n WARNINGS:\n' + ''.join(f'  - {msg}\n' for msg in warnings)

    def _format_config_section(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']
        _, _, consensus_duration = self._consensus_throughput()
        _, _, commit_duration = self._committed_throughput()
        s = (
            ' + CONFIG:\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Benchmark duration: {self.bench_duration:,} s\n'
            f' Consensus duration: {round(consensus_duration, 2):,} s\n'
            f' Commit duration: {round(commit_duration, 2):,} s\n'
            '\n'
        )
        if self.verbose:
            s += (
                f' Faults: {self.faults} node(s)\n'
                f' Collocate primary and workers: {self.collocate}\n'
                f' Header size: {header_size:,} B\n'
                f' Max header delay: {max_header_delay:,} ms\n'
                f' GC depth: {gc_depth:,} round(s)\n'
                f' Sync retry delay: {sync_retry_delay:,} ms\n'
                f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
                f' batch size: {batch_size:,} B\n'
                f' Max batch delay: {max_batch_delay:,} ms\n'
                '\n'
            )
        return s

    def _format_results_section(self):
        commit_metrics = self._worker_committed_latency()
        commit_latency = commit_metrics['mean'] * 1_000
        commit_p50 = commit_metrics['p50'] * 1_000
        commit_p90 = commit_metrics['p90'] * 1_000
        commit_p95 = commit_metrics['p95'] * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        committed_tps, committed_bps, _ = self._committed_throughput()
        e2e = self._e2e_latency()
        e2e_tps, e2e_bps, e2e_duration = self._end_to_end_throughput()
        e2e_lines1 = ''
        e2e_lines2 = ''
        if e2e:
            e2e_lines1 = (
                f' E2E latency (send -> exec reply) (mean): {round(e2e["mean"]):,} ms\n'
                f' E2E latency (send -> exec reply) (p50): {round(e2e["p50"]):,} ms\n'
                f' E2E latency (send -> exec reply) (p90): {round(e2e["p90"]):,} ms\n'
                f' E2E latency (send -> exec reply) (p95): {round(e2e["p95"]):,} ms\n'
            )

            e2e_lines2 = (
                f' E2E TPS: {round(e2e_tps):,} tx/s\n'
                f' E2E duration: {round(e2e_duration, 2):,} s\n'
            )

        return (
            ' + RESULTS:\n'
            f' f+1 Commit latency (workers) (mean): {round(commit_latency):,} ms\n'
            f' f+1 Commit latency (workers) (p50): {round(commit_p50):,} ms\n'
            f' f+1 Commit latency (workers) (p90): {round(commit_p90):,} ms\n'
            f' f+1 Commit latency (workers) (p95): {round(commit_p95):,} ms\n'
            + e2e_lines1 +
            '\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Committed TPS: {round(committed_tps):,} tx/s\n'
            f' Committed BPS: {round(committed_bps):,} B/s\n'
            + e2e_lines2
        )

    def _format_validator_commit_section(self):
        tx_counts, percentages = self._validator_load_distribution()
        per_v_tps = self._per_validator_committed_tps()
        per_v_latency = self._per_validator_committed_latency()
        lines = [
            '\n'
            ' + VALIDATOR LOAD DISTRIBUTION:\n'
        ]
        for v in sorted(tx_counts.keys()):
            lines.append(f' Validator {v}: {tx_counts[v]:,} tx ({percentages[v]:.1f}%)\n')
        exec_counts, exec_pcts = self._executor_load_distribution()
        if exec_counts is not None:
            lines.append('\n + EXECUTOR LOAD DISTRIBUTION (avg across validators):\n')
            for e in sorted(exec_counts.keys()):
                lines.append(f' Executor {e}: {exec_counts[e]:,} tx ({exec_pcts[e]:.1f}%)\n')
        lines.append(
            '\n'
            ' + PER-VALIDATOR COMMIT METRICS:\n'
            ' Validator    TPS (tx/s)    Mean (ms)    p95 (ms)    Misses\n'
        )
        for v in sorted(per_v_tps.keys()):
            lat_metrics = per_v_latency.get(v)
            mean_str = f'{round(lat_metrics["mean"] * 1_000):,}' if lat_metrics else 'N/A'
            p95_str = f'{round(lat_metrics["p95"] * 1_000):,}' if lat_metrics else 'N/A'
            misses = self.misses_by_validator.get(v, 0)
            lines.append(f' {v:<12} {round(per_v_tps[v]):<13,} {mean_str:<12} {p95_str:<11} {misses}\n')
        total_tps = sum(per_v_tps.values())
        weighted_lat = sum(
            per_v_latency[v]['mean'] * percentages[v] / 100
            for v in per_v_latency if v in percentages
        )
        weighted_lat_str = f'{round(weighted_lat * 1_000):,}' if per_v_latency else 'N/A'
        weighted_p95_str = 'N/A'
        if per_v_latency:
            weighted_p95 = sum(
                per_v_latency[v]['p95'] * percentages[v] / 100
                for v in per_v_latency if v in percentages
            )
            weighted_p95_str = f'{round(weighted_p95 * 1_000):,}'
        total_misses = sum(self.misses_by_validator.values())
        lines.append(
            f' {"Overall":<12} {round(total_tps):<13,}'
            f' {weighted_lat_str + " (wtd)":<12} {weighted_p95_str + " (wtd)":<11} {total_misses}\n'
        )
        return ''.join(lines)

    def _format_quorum_timing_section(self):
        quorum_timing = self._per_validator_quorum_timing()
        dw, lw = 7, 6
        pair_w = dw + 1 + lw        # 14
        group_header = (
            f' {"":12}'
            f'  {"mean":^{pair_w}}'
            f'  {"p95":^{pair_w}}'
            f'  {"std":^{pair_w}}\n'
        )
        sub_header = (
            f' {"Validator":<12}'
            f'  {"delay":>{dw}} {"lat":>{lw}}'
            f'  {"delay":>{dw}} {"lat":>{lw}}'
            f'  {"delay":>{dw}} {"lat":>{lw}}\n'
        )
        lines = ['\n + PER-VALIDATOR QUORUM TIMING (ms):\n', group_header, sub_header]
        for v in sorted(quorum_timing.keys()):
            t = quorum_timing[v]
            lines.append(
                f' {v:<12}'
                f'  {round(t["queue_delay_mean"]):>{dw},} {round(t["quorum_latency_mean"]):>{lw},}'
                f'  {round(t["queue_delay_p95"]):>{dw},} {round(t["quorum_latency_p95"]):>{lw},}'
                f'  {t["queue_delay_std"]:>{dw},} {t["quorum_latency_std"]:>{lw},}\n'
            )
        return ''.join(lines)

    def result(self):
        sections = [
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n',
            self._format_config_section(),
            self._format_results_section(),
        ]
        if self.sizes_by_validator:
            sections.append(self._format_validator_commit_section())
        if self.queue_delay_by_validator:
            sections.append(self._format_quorum_timing_section())

        # Global and per-validator per-stage latency breakdown
        global_stage_data = self._global_stage_latency_breakdown()
        stage_data = self._per_validator_stage_latency_breakdown()
        has_stage_data = any(
            stage_data[v][label]['count'] > 0
            for v in stage_data
            for label in stage_data[v]
        )
        if has_stage_data:
            sections.append(self._format_stage_matrix(
                'PER-STAGE LATENCY BREAKDOWN (mean, ms)', 'mean', global_stage_data
            ))
            if self.verbose:
                sections.append(self._format_stage_matrix(
                    'PER-VALIDATOR PER-STAGE LATENCY BREAKDOWN (mean, ms)', 'mean', stage_data
                ))
                sections.append(self._format_stage_matrix(
                    'PER-VALIDATOR PER-STAGE LATENCY BREAKDOWN (p50, ms)', 'p50', stage_data
                ))
                sections.append(self._format_stage_matrix(
                    'PER-VALIDATOR PER-STAGE TAIL LATENCY BREAKDOWN (p95, ms)', 'p95', stage_data
                ))
                sections.append(self._format_stage_matrix(
                    'PER-VALIDATOR PER-STAGE TAIL LATENCY BREAKDOWN (p99, ms)', 'p99', stage_data
                ))
        sections.append(self._format_certified_tps_timeline())
        sections.append(self._format_migration_section())
        sections.append(self._format_warnings_section())
        sections.append('-----------------------------------------\n')
        return ''.join(s for s in sections if s)

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0, duration=None, warmup=0, verbose=False):
        assert isinstance(directory, str)

        clients = []
        clients_by_validator = defaultdict(list)
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                content = f.read()
            clients.append(content)
            m = search(r'client-(\d+)-\d+', basename(filename))
            if m:
                clients_by_validator[int(m.group(1))].append(content)

        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]

        workers = []
        workers_by_validator = defaultdict(list)
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                content = f.read()
            workers.append(content)
            m = search(r'worker-(\d+)-\d+', basename(filename))
            if m:
                workers_by_validator[int(m.group(1))].append(content)

        executor_logs = {}
        for filename in sorted(glob(join(directory, 'executor-*.log'))):
            m = search(r'executor-(\d+)-(\d+)', basename(filename))
            if m:
                with open(filename, 'r') as f:
                    executor_logs[(int(m.group(1)), int(m.group(2)))] = f.read()

        return cls(
            clients, primaries, workers, faults=faults,
            workers_by_validator=dict(workers_by_validator),
            clients_by_validator=dict(clients_by_validator),
            duration=duration,
            warmup=warmup,
            verbose=verbose,
            executor_logs=executor_logs if executor_logs else None,
        )
