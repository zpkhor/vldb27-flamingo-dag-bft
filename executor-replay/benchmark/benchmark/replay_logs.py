from collections import defaultdict
from datetime import datetime
from glob import glob
from os.path import basename, join
from re import findall, search
from statistics import mean, quantiles

from benchmark.utils import Print


class ReplayParseError(Exception):
    pass


def _to_posix(string):
    x = datetime.fromisoformat(string.replace('Z', '+00:00'))
    return datetime.timestamp(x)


class ReplayLogParser:
    """Parses replay mode logs to compute TPS and latency metrics.

    Log lines used:
      Primary:  "[ts] Replay Execute seq=N worker=W num_tx=T"
      Executor: "[ts] Executed batch seq=N txs=T total_executed=E"
    """

    def __init__(self, primary_log, executor_logs, tx_size):
        self.tx_size = tx_size

        # Parse primary: seq -> (posix_time, num_tx)
        self.dispatched = {}
        for t, seq, worker, num_tx in findall(
            r'\[(.*?Z) .*Replay Execute seq=(\d+) worker=(\d+) num_tx=(\d+)',
            primary_log
        ):
            self.dispatched[int(seq)] = (_to_posix(t), int(num_tx))

        # Parse executors.
        # txs = full batch size (same for all executors, NOT per-executor processed count).
        # total_executed = cumulative per-executor count of transactions actually owned/executed.
        # For E2E TPS: sum each executor's final total_executed to get total work done.
        self.executed = {}  # seq -> max completion timestamp across executors
        self.per_executor_executed = {}  # label -> final total_executed
        for label, log in executor_logs:
            last_total = 0
            for t, seq, txs, total in findall(
                r'\[(.*?Z) .*Executed batch seq=(\d+) txs=(\d+) total_executed=(\d+)',
                log
            ):
                s = int(seq)
                ts = _to_posix(t)
                if s not in self.executed or ts > self.executed[s]:
                    self.executed[s] = ts
                last_total = int(total)
            self.per_executor_executed[label] = last_total
        self.total_actually_executed = sum(self.per_executor_executed.values())

        if not self.dispatched:
            raise ReplayParseError('No "Replay Execute" entries found in primary log')

    def _latencies_ms(self):
        """Per-batch latency: Execute dispatch -> executor completion (ms)."""
        latencies = []
        for seq, (dispatch_time, _) in self.dispatched.items():
            if seq in self.executed:
                lat_ms = (self.executed[seq] - dispatch_time) * 1000
                if lat_ms >= 0:
                    latencies.append(lat_ms)
        return latencies

    def _percentiles(self, data):
        if not data:
            return {'mean': 0, 'p50': 0, 'p90': 0, 'p95': 0}
        result = {'mean': mean(data)}
        if len(data) >= 20:
            q = quantiles(data, n=100)
            result['p50'] = q[49]
            result['p90'] = q[89]
            result['p95'] = q[94]
        else:
            s = sorted(data)
            result['p50'] = s[len(s) // 2]
            result['p90'] = s[int(len(s) * 0.9)]
            result['p95'] = s[int(len(s) * 0.95)]
        return result

    def _tps(self):
        """Throughput: total executed txs / execution window duration (first to last executor completion)."""
        if len(self.executed) < 2:
            return 0, 0
        start = min(self.executed.values())
        end = max(self.executed.values())
        duration = end - start
        if duration <= 0:
            return 0, 0
        tps = self.total_actually_executed / duration
        return tps, duration

    def _committed_tps(self):
        """Committed TPS: total dispatched txs / dispatch window duration."""
        if len(self.dispatched) < 2:
            return 0
        times = [t for t, _ in self.dispatched.values()]
        duration = max(times) - min(times)
        if duration <= 0:
            return 0
        total_tx = sum(n for _, n in self.dispatched.values())
        return total_tx / duration

    def _executor_distribution(self):
        if not self.per_executor_executed:
            return ''
        total = self.total_actually_executed
        rows = sorted(self.per_executor_executed.items())
        counts = list(self.per_executor_executed.values())
        mn, mx = min(counts), max(counts)
        ratio = f'{mx / mn:.2f}x' if mn > 0 else 'N/A'
        lines = [
            '\n'
            '-----------------------------------------\n'
            ' EXECUTOR DISTRIBUTION:\n'
            '-----------------------------------------\n'
            f' {"executor":<12} {"txs_executed":>14}  {"share":>7}\n'
        ]
        for label, count in rows:
            share = f'{count / total * 100:.1f}%' if total > 0 else 'N/A'
            lines.append(f' {label:<12} {count:>14,}  {share:>7}\n')
        lines.append(
            f' Min: {mn:,}  Max: {mx:,}  (imbalance ratio: {ratio})\n'
            '-----------------------------------------\n'
        )
        return ''.join(lines)

    def result(self):
        tps, duration = self._tps()
        committed_tps = self._committed_tps()
        latencies = self._latencies_ms()
        lat = self._percentiles(latencies)

        total_dispatched = len(self.dispatched)
        total_executed = len(self.executed)
        total_tx_dispatched = sum(n for _, n in self.dispatched.values())

        s = (
            '\n'
            '-----------------------------------------\n'
            ' REPLAY SUMMARY:\n'
            '-----------------------------------------\n'
            f' Batches dispatched: {total_dispatched:,}\n'
            f' Batches executed:   {total_executed:,}\n'
            f' Transactions dispatched: {total_tx_dispatched:,}\n'
            f' Replay duration: {duration:.2f} s\n'
            '\n'
            f' Committed TPS: {round(committed_tps):,} tx/s\n'
            f' E2E TPS:       {round(tps):,} tx/s\n'
            f' BPS: {round(tps * self.tx_size):,} B/s\n'
            '\n'
            f' Execution latency (dispatch -> executed):\n'
            f'   Mean: {round(lat["mean"]):,} ms\n'
            f'   p50:  {round(lat["p50"]):,} ms\n'
            f'   p90:  {round(lat["p90"]):,} ms\n'
            f'   p95:  {round(lat["p95"]):,} ms\n'
            f'   (n={len(latencies):,} batches)\n'
            '-----------------------------------------\n'
        )
        return s + self._executor_distribution()

    @classmethod
    def process(cls, directory, tx_size=512):
        primary_log = ''
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primary_log += f.read()

        executor_logs = []
        for filename in sorted(glob(join(directory, 'executor-*.log'))):
            m = search(r'executor-(\d+)-(\d+)', basename(filename))
            label = f'{m.group(1)}-{m.group(2)}' if m else basename(filename)
            with open(filename, 'r') as f:
                executor_logs.append((label, f.read()))

        if not primary_log:
            raise ReplayParseError(f'No primary log found in {directory}')
        if not executor_logs:
            raise ReplayParseError(f'No executor logs found in {directory}')

        return cls(primary_log, executor_logs, tx_size)
