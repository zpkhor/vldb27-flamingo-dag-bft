# CloudLab benchmark orchestrator.
# Deploys Narwhal on CloudLab physical machines using SSH + tmux.
import io
import json
import os
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from os.path import basename, splitext
from time import sleep, time as _now

import paramiko
from fabric import Connection

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    CloudLabCommittee,
    CloudLabReplayCommittee,
    NodeParameters,
    BenchParameters,
    ConfigError,
)
from benchmark.instance import CloudLabInstanceManager
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class CloudLabBench:
    BASE_PORT = 5000

    def __init__(
        self,
        bench_parameters_dict,
        node_parameters_dict,
        manifest_file,
        username,
        baseline=False,
        round_robin=False,
        latency_ms=100,
        primary_bw_kbps=25000,
        worker_bws_kbps=None,
        in_memory_store=False,
        num_executors=0,
        no_send_payment=False,
        zipf_exponent=0.0,
        new_scheduler=False,
        executor_bw_kbps=10_000_000,
    ):
        self.username = username
        self.latency_ms = latency_ms
        self.in_memory_store = in_memory_store
        self.num_executors = num_executors
        self.no_send_payment = no_send_payment
        self.zipf_exponent = zipf_exponent
        self.new_scheduler = new_scheduler
        self.executor_bw_kbps = executor_bw_kbps

        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        self.manager = CloudLabInstanceManager.make(manifest_file, username)
        nodes = self.bench_parameters.nodes[0]
        # Validators use node-0..nodes-1, executors use node-nodes..2*nodes-1
        required_machines = nodes + (nodes if num_executors > 0 else 0)
        assert required_machines <= self.manager.num_validators(), (
            f'Need {required_machines} node-X machines '
            f'({nodes} validators + {nodes if num_executors > 0 else 0} executor hosts) '
            f'but manifest has {self.manager.num_validators()}'
        )

        self.baseline = baseline
        self.round_robin = round_robin
        self.primary_bw_kbps = primary_bw_kbps
        assert worker_bws_kbps is not None, 'worker_bws_kbps must be provided'
        assert len(worker_bws_kbps) == nodes, (
            f'worker_bws_kbps has {len(worker_bws_kbps)} entries but nodes={nodes}'
        )
        self.worker_bws_kbps = worker_bws_kbps

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _ssh(self, host, retries=3, delay=2):
        """Create a Fabric Connection to a host with retry on DNS failures."""
        for attempt in range(retries):
            try:
                conn = Connection(host, user=self.username)
                conn.open()
                return conn
            except socket.gaierror as e:
                if attempt == retries - 1:
                    raise
                Print.warn(f'DNS resolution failed for {host}: {e}, retrying in {delay}s...')
                sleep(delay)

    def _background_run(self, ssh_host, command, log_file):
        """Run a command in a tmux session on a remote host."""
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = self._ssh(ssh_host)
        c.run(cmd, hide=True)

    def _parallel_ssh(self, hosts, fn):
        """Run fn(host) in parallel across all hosts. Asserts no failures."""
        failed = []
        with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
            futures = {pool.submit(fn, h): h for h in hosts}
            for future in as_completed(futures):
                host = futures[future]
                try:
                    future.result()
                except Exception as e:
                    failed.append((host, e))
        assert not failed, f'Parallel SSH failed on: {failed}'

    def _reset_tcp_buffers(self, ssh_hosts):
        """Reset TCP buffers to defaults to avoid bufferbloat."""
        Print.info('Resetting TCP buffers...')
        sysctl_cmd = (
            'sysctl -w '
            'net.core.rmem_max=8388608 '
            'net.core.wmem_max=8388608 '
            'net.ipv4.tcp_rmem="4096 131072 6291456" '
            'net.ipv4.tcp_wmem="4096 16384 4194304" '
            'net.core.netdev_max_backlog=2048'
        )
        def _tune(host):
            self._ssh(host).run(f'sudo {sysctl_cmd}', hide=True)
        self._parallel_ssh(ssh_hosts, _tune)

    @staticmethod
    def _detect_iface():
        """Shell snippet to find the 10.10.1.x experiment LAN interface."""
        return (
            'IFACE=$(ip -o addr show | grep " 10\\\\." | awk "{print \\$2}" | head -1)\n'
            '[ -z "$IFACE" ] && echo "FATAL: no 10.x interface" && exit 1'
        )

    @staticmethod
    def _strip_all_tc():
        """Shell snippet to remove Emulab-applied TC from every interface."""
        return (
            'for dev in $(ls /sys/class/net/ | grep -v lo); do '
            'tc qdisc del dev $dev root 2>/dev/null || true; '
            'tc qdisc del dev $dev ingress 2>/dev/null || true; '
            'done'
        )

    def _apply_tc_shaping(self, v_ssh, e_ssh, nodes, committee):
        """Egress-only HTB + netem on validators/executors, SO_MARK-based client shaping."""
        Print.info('Applying TC QoS shaping...')

        primary_bw = self.primary_bw_kbps
        half_lat = self.latency_ms // 2  # egress-only delay per hop

        # Primary-to-primary ports for traffic classification
        primary_ports = [
            auth['primary']['primary_to_primary'].split(':')[1]
            for auth in committee.json['authorities'].values()
        ]

        # Executor IPs for traffic exemption on validator machines
        e_ip_set = set(self.manager.validator_ips()[nodes:2 * nodes]) if e_ssh else set()

        # Per-validator transactions ports (for loopback TC shaping)
        v_ips = self.manager.validator_ips()[:nodes]
        v_tx_ports = {}  # i -> [port, ...]
        for i, v_ip in enumerate(v_ips):
            ports = []
            for auth in committee.json['authorities'].values():
                for wid in sorted(auth['workers'].keys()):
                    addr = auth['workers'][wid]['transactions']
                    host, port = addr.rsplit(':', 1)
                    if host == v_ip:
                        ports.append(port)
            v_tx_ports[i] = ports

        def _shape_validator(i):
            worker_bw = self.worker_bws_kbps[i]
            executor_bw = 5_000_000  # 5 Gbit — co-located, no real limit
            total_bw = primary_bw + worker_bw + (executor_bw if e_ssh else 0)
            # Port filters: classify primary traffic into class 1:10
            port_filters = []
            for p in primary_ports:
                port_filters.append(f'tc filter add dev $IFACE parent 1:0 protocol ip prio 2 u32 match ip sport {p} 0xffff flowid 1:10')
                port_filters.append(f'tc filter add dev $IFACE parent 1:0 protocol ip prio 2 u32 match ip dport {p} 0xffff flowid 1:10')

            # IP filters: classify executor traffic into class 1:30 (no delay)
            executor_filters = []
            for e_ip in e_ip_set:
                executor_filters.append(f'tc filter add dev $IFACE parent 1:0 protocol ip prio 1 u32 match ip dst {e_ip}/32 flowid 1:30')

            executor_class_lines = []
            if e_ssh:
                executor_class_lines = [
                    # Executor class: high bandwidth, NO netem delay (simulates co-located)
                    f'tc class add dev $IFACE parent 1:1 classid 1:30 htb rate {executor_bw}kbit ceil {executor_bw}kbit prio 0',
                ]

            script = '\n'.join([
                'set -e',
                self._detect_iface(),
                self._strip_all_tc(),
                # HTB: primary (1:10) + worker (1:20, default) + executor (1:30, no delay)
                # No client exemption — client replies go through worker class (50ms netem)
                # so client-to-own-validator RTT = 50ms, client-to-remote = 100ms
                f'tc qdisc add dev $IFACE root handle 1: htb default 20',
                f'tc class add dev $IFACE parent 1: classid 1:1 htb rate {total_bw}kbit',
                f'tc class add dev $IFACE parent 1:1 classid 1:10 htb rate {primary_bw}kbit ceil {primary_bw}kbit prio 0',
                f'tc class add dev $IFACE parent 1:1 classid 1:20 htb rate {worker_bw}kbit ceil {worker_bw}kbit prio 1',
                *executor_class_lines,
                f'tc qdisc add dev $IFACE parent 1:10 handle 10: netem delay {half_lat}ms limit 10000',
                f'tc qdisc add dev $IFACE parent 1:20 handle 20: netem delay {half_lat}ms limit 10000',
                *executor_filters,
                *port_filters,
            ])
            # Loopback TC: shape local client → worker transactions traffic
            # so colocated client consumes bandwidth like a remote client would
            lo_port_filters = []
            for p in v_tx_ports.get(i, []):
                lo_port_filters.append(
                    f'tc filter add dev lo parent 1:0 protocol ip prio 1 '
                    f'u32 match ip dport {p} 0xffff flowid 1:10'
                )
            lo_script = '\n'.join([
                'tc qdisc del dev lo root 2>/dev/null || true',
                f'tc qdisc add dev lo root handle 1: htb default 99',
                f'tc class add dev lo parent 1: classid 1:1 htb rate 10gbit',
                f'tc class add dev lo parent 1:1 classid 1:10 htb rate {worker_bw}kbit ceil {worker_bw}kbit',
                f'tc class add dev lo parent 1:1 classid 1:99 htb rate 10gbit',
                f'tc qdisc add dev lo parent 1:10 handle 10: netem delay {half_lat}ms limit 10000',
                *lo_port_filters,
            ])

            full_script = script + '\n' + lo_script
            self._ssh(v_ssh[i]).run(f'sudo bash -c \'{full_script}\'', hide=True)

        def _shape_executor(i):
            # Executor machines: cap inter-executor traffic at LAN bandwidth, no netem.
            exec_bw = self.executor_bw_kbps
            script = '\n'.join([
                'set -e',
                self._detect_iface(),
                self._strip_all_tc(),
                f'tc qdisc add dev $IFACE root handle 1: htb default 10',
                f'tc class add dev $IFACE parent 1: classid 1:1 htb rate {exec_bw}kbit',
                f'tc class add dev $IFACE parent 1:1 classid 1:10 htb rate {exec_bw}kbit ceil {exec_bw}kbit',
            ])
            self._ssh(e_ssh[i]).run(f'sudo bash -c \'{script}\'', hide=True)

        with ThreadPoolExecutor(max_workers=nodes + len(e_ssh)) as pool:
            futures = [pool.submit(_shape_validator, i) for i in range(nodes)]
            for i in range(len(e_ssh)):
                futures.append(pool.submit(_shape_executor, i))
            failed = []
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    failed.append(e)
            assert not failed, f'TC shaping failed: {failed}'

        Print.info(
            f'TC QoS applied: primary={primary_bw}kbps, '
            f'worker_bws={[self.worker_bws_kbps[i] for i in range(nodes)]}kbps, '
            f'client={max(self.worker_bws_kbps)}kbps'
            f'{", executors=" + str(len(e_ssh)) + " machines (" + str(self.executor_bw_kbps) + "kbps LAN cap, no netem)" if e_ssh else ""}'
        )

    def _kill(self, ssh_hosts, delete_logs=False):
        """Kill all tmux sessions on the given hosts."""
        delete_cmd = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = f'{delete_cmd} && ({CommandMaker.kill()} || true)'
        def _kill_one(host):
            try:
                self._ssh(host).run(cmd, hide=True)
            except Exception:
                pass
        self._parallel_ssh(ssh_hosts, _kill_one)

    def run(self, debug=False, log_dir=None):
        assert isinstance(debug, bool)
        Print.heading('Starting CloudLab benchmark')

        nodes, rate = self.nodes[0], self.rate[0]

        # Slice to requested node count
        v_ssh = self.manager.validator_ssh_hosts()[:nodes]
        v_ips = self.manager.validator_ips()[:nodes]
        c_ip = v_ips[0]  # clients are collocated with validators; placeholder (overridden below)

        # Executor machines: next `nodes` machines after validators
        if self.num_executors > 0:
            e_ssh = self.manager.validator_ssh_hosts()[nodes:2 * nodes]
            e_ips = self.manager.validator_ips()[nodes:2 * nodes]
            assert len(e_ssh) == nodes, (
                f'Need {nodes} executor machines but only {len(e_ssh)} available'
            )
        else:
            e_ssh = []
            e_ips = []

        all_ssh = v_ssh + e_ssh
        Print.info(f"All SSH hosts: {all_ssh}")

        try:
            # Kill any previous run
            Print.info('Killing previous processes...')
            self._kill(all_ssh, delete_logs=True)

            # Reset TCP buffers to defaults to avoid bufferbloat
            self._reset_tcp_buffers(all_ssh)

            # Clean up locally
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
            else:
                cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
                subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)

            def _cfg(path):
                return os.path.join(log_dir, os.path.basename(path)) if log_dir else path

            # Compile locally
            Print.info('Compiling...')
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create local symlinks
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Rsync binaries and required shared libs to all machines (parallel)
            Print.info(f'Distributing binaries to {len(all_ssh)} machines...')
            binary_path = PathMaker.binary_path()
            # Ship libc and ld-linux from control-host so CloudLab nodes (Ubuntu 22,
            # GLIBC 2.35) can run binaries compiled on control-host (Ubuntu 24, GLIBC 2.38+).
            extra_libs = [
                '/lib/x86_64-linux-gnu/libc.so.6',
                '/lib64/ld-linux-x86-64.so.2',
            ]

            def _distribute_one(host):
                subprocess.run(
                    f'rsync -azL {binary_path}/node {binary_path}/benchmark_client '
                    f'{" ".join(extra_libs)} '
                    f'{self.username}@{host}:~/',
                    shell=True, check=True,
                )
                self._ssh(host).run(
                    'patchelf --set-interpreter ~/ld-linux-x86-64.so.2 '
                    '         --set-rpath ~ ~/node && '
                    'patchelf --set-interpreter ~/ld-linux-x86-64.so.2 '
                    '         --set-rpath ~ ~/benchmark_client && '
                    'sudo setcap cap_net_admin+ep ~/benchmark_client',
                    hide=True,
                )

            self._parallel_ssh(all_ssh, _distribute_one)

            # Generate keys
            Print.info('Generating configuration files...')
            keys = []
            key_files = [_cfg(PathMaker.key_file(i)) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]

            # Build committee (single client IP)
            committee = CloudLabCommittee(
                names, self.BASE_PORT, self.workers, v_ips, c_ip,
                num_executors=self.num_executors,
                executor_ips=e_ips,
            )

            latency_matrix = {
                name: {other: (0 if name == other else self.latency_ms) for other in names}
                for name in names
            }
            committee.set_latency_matrix(latency_matrix)

            if not self.baseline:
                capacities = []
                for i in range(nodes):
                    workers_bw_bytes = self.worker_bws_kbps[i] * 1000 / 8
                    capacity = int(workers_bw_bytes / (self.tx_size + 44) / (nodes - 1) * 0.9)
                    capacities.append(capacity)
                committee.set_capacities(capacities)

            if self.baseline:
                self.node_parameters.json['baseline_mode'] = True

            # Set executor parameters in node params
            if self.num_executors > 0:
                self.node_parameters.set_executor_params(
                    num_executors=self.num_executors,
                    num_accounts=self.bench_parameters.num_accounts,
                    min_balance=10_000,
                    max_balance=100_000,
                    sharding_strategy='range',
                    no_send_payment_tx=self.no_send_payment,
                    use_new_scheduler=self.new_scheduler,
                )

            self.node_parameters.print(_cfg(PathMaker.parameters_file()))

            # Account distribution
            num_accounts = self.bench_parameters.num_accounts
            account_weights = self.bench_parameters.account_weights or [1] * nodes
            total_aw = sum(account_weights)
            acct_counts = [num_accounts * w // total_aw for w in account_weights]
            remainder = num_accounts - sum(acct_counts)
            for k in range(remainder):
                acct_counts[k] += 1
            acct_starts = []
            s = 0
            for c_val in acct_counts:
                acct_starts.append(s)
                s += c_val

            account_ranges = {
                name: (acct_starts[i], acct_counts[i])
                for i, name in enumerate(names)
            }
            committee.set_account_ranges(account_ranges)

            # Set client_reply_addresses for executor -> client replies (e2e latency)
            if self.num_executors > 0:
                exec_reply_port = self.BASE_PORT + 9000
                committee.set_client_reply_addresses({
                    i: f'{v_ips[i]}:{exec_reply_port}'
                    for i in range(nodes)
                })

            # Each authority's client_reply points to its validator machine
            reply_port = list(committee.json['authorities'].values())[0]['client_reply'].split(':')[1]
            for i, name in enumerate(names):
                committee.json['authorities'][name]['client_reply'] = f'{v_ips[i]}:{reply_port}'

            committee.print(_cfg(PathMaker.committee_file()))

            # Exclude faulty validators from running processes
            good_nodes = nodes - self.faults
            names = names[:good_nodes]

            # Upload config files to all machines (parallel)
            Print.info('Uploading config files...')
            def _upload_validator(i):
                conn = self._ssh(v_ssh[i])
                db_dirs = f'mkdir -p .db-{i} ' + ' '.join(f'.db-{i}-{w}' for w in range(self.workers))
                conn.run(
                    f'{CommandMaker.remote_cleanup()} || true && mkdir -p logs && {db_dirs}',
                    hide=True,
                )
                conn.put(_cfg(PathMaker.committee_file()), '.')
                conn.put(_cfg(PathMaker.parameters_file()), '.')
                conn.put(_cfg(PathMaker.key_file(i)), '.')

            def _upload_executor(i):
                conn = self._ssh(e_ssh[i])
                exec_db_dirs = ' '.join(
                    f'.db-exec-{i}-{e}' for e in range(self.num_executors)
                )
                conn.run(
                    f'{CommandMaker.remote_cleanup()} || true && mkdir -p logs && mkdir -p {exec_db_dirs}',
                    hide=True,
                )
                conn.put(_cfg(PathMaker.committee_file()), '.')
                conn.put(_cfg(PathMaker.parameters_file()), '.')
                conn.put(_cfg(PathMaker.key_file(i)), '.')

            with ThreadPoolExecutor(max_workers=nodes + len(e_ssh)) as pool:
                futures = [pool.submit(_upload_validator, i) for i in range(nodes)]
                for i in range(len(e_ssh)):
                    futures.append(pool.submit(_upload_executor, i))
                for f in as_completed(futures):
                    f.result()

            # Build per-validator client commands
            workers_addresses = committee.workers_addresses(self.faults)
            weights = (self.rate_weights or [1] * nodes)[:good_nodes]
            total_weight = sum(weights)

            all_worker_addrs = [addr for addresses in workers_addresses for _, addr in addresses]

            rr_flag = ' --round-robin' if self.round_robin else ''
            no_send_flag = ' --no-send-payment' if self.no_send_payment else ''
            zipf_flag = f' --zipf-exponent {self.zipf_exponent}' if self.zipf_exponent > 0 else ''
            e_skew_flag = ''
            if self.e_skew_weights:
                e_skew_weights_str = ','.join(str(w) for w in self.e_skew_weights)
                e_skew_flag = f' --executor-skew-weights {e_skew_weights_str}'

            vi_rates = []
            base_rate_sum = 0
            for vi in range(len(names)):
                if vi == len(names) - 1:
                    vi_rates.append(rate - base_rate_sum)
                else:
                    r = int(rate * weights[vi] / total_weight)
                    vi_rates.append(r)
                    base_rate_sum += r

            client_commands = []
            for vi, name in enumerate(names):
                auth = committee.json['authorities'][name]
                vw = '--validator-workers {}:{}'.format(
                    name,
                    '+'.join(auth['workers'][wid]['transactions']
                             for wid in sorted(auth['workers'].keys()))
                )
                start, count = account_ranges[name]
                ar = f'--account-ranges {name}:{start}:{count}'
                vi_rate = vi_rates[vi]
                local_addrs = ' '.join(addr for _, addr in workers_addresses[vi])
                reply = f' --reply-addr {auth["client_reply"]}'
                exec_reply = ''
                if self.num_executors > 0:
                    exec_reply = f' --execution-reply-addr {v_ips[vi]}:{self.BASE_PORT + 9000}'
                cmd = (
                    f'ulimit -n 65536 && ./benchmark_client --size {self.tx_size} '
                    f'--rate {vi_rate} --nodes {local_addrs} '
                    f'{ar} --rate-weights 1 '
                    f'--own-validator {name} '
                    f'{vw}{reply}{rr_flag}{no_send_flag}{zipf_flag}{exec_reply}{e_skew_flag}'
                    f' --rampup-secs {self.warmup}'
                    f' --client-id {vi}'
                )
                client_commands.append(cmd)

            # Apply QoS TC shaping BEFORE starting processes so TCP connections
            # are established with the correct RTT from the start
            self._apply_tc_shaping(v_ssh, e_ssh, nodes, committee)

            # Start primaries
            Print.info('Starting primaries...')
            for i in range(good_nodes):
                primary_cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    debug=debug,
                    in_memory_store=self.in_memory_store,
                )
                self._background_run(
                    v_ssh[i], primary_cmd, PathMaker.primary_log_file(i),
                )

            # Start workers
            Print.info('Starting workers...')
            for i, addresses in enumerate(workers_addresses):
                for wid, _ in addresses:
                    worker_cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, wid),
                        PathMaker.parameters_file(),
                        wid,
                        debug=debug,
                        in_memory_store=self.in_memory_store,
                    )
                    self._background_run(
                        v_ssh[i], worker_cmd, PathMaker.worker_log_file(i, wid),
                    )

            # Start executors (on dedicated machines)
            if self.num_executors > 0:
                Print.info('Starting executors...')
                for i in range(nodes):
                    for e in range(self.num_executors):
                        executor_cmd = CommandMaker.run_executor(
                            PathMaker.key_file(i),
                            PathMaker.committee_file(),
                            f'.db-exec-{i}-{e}',
                            PathMaker.parameters_file(),
                            e,
                            list(zip(acct_starts, acct_counts)),
                            debug=debug,
                        )
                        self._background_run(
                            e_ssh[i], executor_cmd, PathMaker.executor_log_file(i, e),
                        )

            # Wait for worker ports to be ready
            Print.info('Waiting for workers to be ready...')
            per_validator_checks = {}
            for addr in all_worker_addrs:
                host, port = addr.split(':')
                per_validator_checks.setdefault(host, []).append(port)

            ip_to_ssh = dict(zip(v_ips, v_ssh))

            deadline = _now() + 120
            while True:
                all_ready = True
                for ip, ports in per_validator_checks.items():
                    ssh_host = ip_to_ssh[ip]
                    checks = ' && '.join(
                        f'bash -c "echo >/dev/tcp/{ip}/{p}"' for p in ports
                    )
                    c = self._ssh(ssh_host)
                    result = c.run(checks, hide=True, warn=True)
                    if result.failed:
                        all_ready = False
                        break
                if all_ready:
                    break
                assert _now() <= deadline, 'Workers did not become ready within 120s'
                sleep(0.5)
            Print.info('All workers ready.')

            Print.info(f'Starting {good_nodes} clients (one per validator)...')
            for i in range(good_nodes):
                self._background_run(
                    v_ssh[i], client_commands[i], PathMaker.client_log_file(i, 0),
                )

            # Run benchmark
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)

            # Stop
            Print.info('Stopping processes...')
            self._kill(all_ssh, delete_logs=False)
            sleep(1)

            # Download logs (parallel)
            Print.info('Downloading logs...')
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
                for f in os.listdir(log_dir):
                    if f.endswith('.log'):
                        os.remove(os.path.join(log_dir, f))
            else:
                cmd = CommandMaker.clean_logs()
                subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
                subprocess.run(['mkdir', '-p', PathMaker.logs_path()])

            def _local(remote_path):
                return os.path.join(log_dir, os.path.basename(remote_path)) if log_dir else remote_path

            def _download_validator(i):
                conn = self._ssh(v_ssh[i])
                conn.get(PathMaker.primary_log_file(i), local=_local(PathMaker.primary_log_file(i)))
                for w in range(self.workers):
                    conn.get(
                        PathMaker.worker_log_file(i, w),
                        local=_local(PathMaker.worker_log_file(i, w)),
                    )

            def _download_executor(i):
                conn = self._ssh(e_ssh[i])
                for e in range(self.num_executors):
                    conn.get(
                        PathMaker.executor_log_file(i, e),
                        local=_local(PathMaker.executor_log_file(i, e)),
                    )

            def _download_client(i):
                conn = self._ssh(v_ssh[i])
                conn.get(
                    PathMaker.client_log_file(i, 0),
                    local=_local(PathMaker.client_log_file(i, 0)),
                )

            with ThreadPoolExecutor(max_workers=nodes + len(e_ssh) + good_nodes) as pool:
                futures = [pool.submit(_download_validator, i) for i in range(nodes)]
                for i in range(len(e_ssh)):
                    futures.append(pool.submit(_download_executor, i))
                for i in range(good_nodes):
                    futures.append(pool.submit(_download_client, i))
                for f in as_completed(futures):
                    f.result()

            # Save bench params for log parsing (after download, since clean_logs wipes the dir)
            params_path = os.path.join(log_dir, 'bench-params.json') if log_dir else PathMaker.bench_params_file()
            with open(params_path, 'w') as f:
                json.dump({
                    'duration': self.duration,
                    'warmup': self.warmup,
                    'faults': self.faults,
                }, f)

            # Parse logs
            Print.info('Parsing logs...')
            return LogParser.process(
                log_dir if log_dir else PathMaker.logs_path(),
                faults=self.faults,
                duration=self.duration,
                warmup=self.warmup,
                verbose=debug,
            )

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill(all_ssh, delete_logs=False)
            raise BenchError('Failed to run benchmark', e)


class CloudLabReplayBench:
    """Deploys the replay benchmark on CloudLab physical nodes.

    Node layout (all from the manifest's node-X list):
        node[0]           -> primary (runs ReplaySequencer, no workers)
        node[1..W]        -> one worker per node
        node[W+1..W+E]    -> one executor per node

    TC shaping: LAN bandwidth cap on worker->executor and executor->executor
    egress. No netem latency shaping.
    """
    BASE_PORT = 5000

    def __init__(
        self,
        manifest_file,
        username,
        replay_csv,
        replay_tx_size,
        num_workers,
        num_executors,
        num_accounts,
        duration,
        in_memory_store,
        use_writeback_executor,
        node_parameters_dict,
        executor_bw_kbps,
        node_offset,
        executor_skew_weights,
        distributed_tx_rate,
        no_send_payment,
        use_new_scheduler,
    ):
        assert os.path.exists(replay_csv), f'Replay CSV not found: {replay_csv}'
        assert num_workers >= 1
        assert num_executors >= 1
        assert node_offset >= 0

        self.username = username
        self.replay_csv = replay_csv
        self.replay_tx_size = replay_tx_size
        self.num_workers = num_workers
        self.num_executors = num_executors
        self.num_accounts = num_accounts
        self.duration = duration
        self.in_memory_store = in_memory_store
        self.use_writeback_executor = use_writeback_executor
        self.executor_bw_kbps = executor_bw_kbps
        self.node_offset = node_offset
        self.executor_skew_weights = executor_skew_weights
        self.distributed_tx_rate = distributed_tx_rate
        self.no_send_payment = no_send_payment
        self.use_new_scheduler = use_new_scheduler

        try:
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid node parameters', e)

        self.manager = CloudLabInstanceManager.make(manifest_file, username)
        required = node_offset + 1 + num_workers + num_executors
        assert required <= self.manager.num_validators(), (
            f'Need {required} node-X machines (offset={node_offset}, 1 primary + {num_workers} workers + '
            f'{num_executors} executors) but manifest has {self.manager.num_validators()}'
        )

    # --- SSH helpers (mirrors CloudLabBench) ---

    def _ssh(self, host, retries=3, delay=2):
        for attempt in range(retries):
            try:
                conn = Connection(
                    host, user=self.username,
                    connect_kwargs={'timeout': 15},
                )
                conn.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                conn.open()
                return conn
            except socket.gaierror as e:
                if attempt == retries - 1:
                    raise
                Print.warn(f'DNS resolution failed for {host}: {e}, retrying in {delay}s...')
                sleep(delay)

    def _background_run(self, ssh_host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        self._ssh(ssh_host).run(cmd, hide=True)

    def _parallel_ssh(self, hosts, fn):
        failed = []
        with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
            futures = {pool.submit(fn, h): h for h in hosts}
            for future in as_completed(futures):
                host = futures[future]
                try:
                    future.result()
                except Exception as e:
                    failed.append((host, e))
        assert not failed, f'Parallel SSH failed on: {failed}'

    def _kill(self, ssh_hosts, delete_logs=False):
        delete_cmd = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = f'{delete_cmd} && ({CommandMaker.kill()} || true)'
        def _kill_one(host):
            try:
                self._ssh(host).run(cmd, hide=True)
            except Exception:
                pass
        self._parallel_ssh(ssh_hosts, _kill_one)

    def _reset_tcp_buffers(self, ssh_hosts):
        sysctl_cmd = (
            'sysctl -w '
            'net.core.rmem_max=8388608 '
            'net.core.wmem_max=8388608 '
            'net.ipv4.tcp_rmem="4096 131072 6291456" '
            'net.ipv4.tcp_wmem="4096 16384 4194304" '
            'net.core.netdev_max_backlog=2048'
        )
        def _tune(host):
            self._ssh(host).run(f'sudo {sysctl_cmd}', hide=True)
        self._parallel_ssh(ssh_hosts, _tune)

    @staticmethod
    def _detect_iface():
        return (
            'IFACE=$(ip -o addr show | grep " 10\\\\." | awk "{print \\$2}" | head -1)\n'
            '[ -z "$IFACE" ] && echo "FATAL: no 10.x interface" && exit 1'
        )

    @staticmethod
    def _strip_all_tc():
        return (
            'for dev in $(ls /sys/class/net/ | grep -v lo); do '
            'tc qdisc del dev $dev root 2>/dev/null || true; '
            'done'
        )

    def _apply_tc_shaping(self, worker_ssh, executor_ssh, executor_ips):
        """Rate-cap worker->executor and executor->executor egress at executor_bw_kbps.
        No netem latency — pure bandwidth shaping only.
        """
        if not executor_ips:
            return
        Print.info(f'Applying TC shaping: executor_bw={self.executor_bw_kbps}kbps...')
        bw = self.executor_bw_kbps
        # Use 100 Gbit as root rate so the default class is effectively uncapped.
        root_rate = 100_000_000

        def _build_script(target_ips):
            ip_filters = [
                f'tc filter add dev $IFACE parent 1:0 protocol ip prio 1 '
                f'u32 match ip dst {ip}/32 flowid 1:10'
                for ip in target_ips
            ]
            return '\n'.join([
                'set -e',
                self._detect_iface(),
                self._strip_all_tc(),
                f'tc qdisc add dev $IFACE root handle 1: htb default 99',
                f'tc class add dev $IFACE parent 1: classid 1:1 htb rate {root_rate}kbit',
                f'tc class add dev $IFACE parent 1:1 classid 1:10 htb rate {bw}kbit ceil {bw}kbit',
                f'tc class add dev $IFACE parent 1:1 classid 1:99 htb rate {root_rate}kbit',
                *ip_filters,
            ])

        def _shape_worker(host):
            # Shape egress from this worker to all executor nodes.
            script = _build_script(executor_ips)
            self._ssh(host).run(f'sudo bash -c \'{script}\'', hide=True)

        def _shape_executor(i):
            # Shape egress from this executor to all OTHER executor nodes.
            others = [ip for j, ip in enumerate(executor_ips) if j != i]
            if not others:
                return
            script = _build_script(others)
            self._ssh(executor_ssh[i]).run(f'sudo bash -c \'{script}\'', hide=True)

        with ThreadPoolExecutor(max_workers=len(worker_ssh) + len(executor_ssh)) as pool:
            futures = [pool.submit(_shape_worker, h) for h in worker_ssh]
            futures += [pool.submit(_shape_executor, i) for i in range(len(executor_ssh))]
            failed = []
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    failed.append(e)
            assert not failed, f'TC shaping failed: {failed}'

        Print.info('TC shaping applied.')

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting CloudLab replay benchmark')

        # --- Node allocation ---
        all_ssh = self.manager.validator_ssh_hosts()[self.node_offset:]
        all_ips = self.manager.validator_ips()[self.node_offset:]

        primary_ssh = all_ssh[0]
        worker_ssh  = all_ssh[1 : 1 + self.num_workers]
        executor_ssh = all_ssh[1 + self.num_workers : 1 + self.num_workers + self.num_executors]
        executor_ips = all_ips[1 + self.num_workers : 1 + self.num_workers + self.num_executors]
        worker_ips   = all_ips[1 : 1 + self.num_workers]
        primary_ip   = all_ips[0]

        all_hosts = [primary_ssh] + worker_ssh + executor_ssh
        Print.info(f'primary={primary_ssh}, workers={worker_ssh}, executors={executor_ssh}')

        try:
            # --- Kill previous + reset TCP ---
            Print.info('Killing previous processes...')
            self._kill(all_hosts, delete_logs=True)
            self._reset_tcp_buffers(all_hosts)

            # --- Local cleanup + compile ---
            subprocess.run(
                [f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'],
                shell=True, stderr=subprocess.DEVNULL,
            )
            Print.info('Compiling...')
            subprocess.run(
                CommandMaker.compile().split(), check=True,
                cwd=PathMaker.node_crate_path(),
            )
            subprocess.run([CommandMaker.alias_binaries(PathMaker.binary_path())], shell=True)

            # --- Rsync binary to all nodes ---
            Print.info(f'Distributing binary to {len(all_hosts)} nodes...')
            binary_path = PathMaker.binary_path()

            extra_libs = [
                '/lib/x86_64-linux-gnu/libc.so.6',
                '/lib64/ld-linux-x86-64.so.2',
            ]

            def _distribute(host):
                subprocess.run(
                    f'rsync -azL -e "ssh -o StrictHostKeyChecking=no" '
                    f'{binary_path}/node {" ".join(extra_libs)} '
                    f'{self.username}@{host}:~/',
                    shell=True, check=True,
                )
                self._ssh(host).run(
                    'sudo apt-get install -y -q patchelf && '
                    'patchelf --set-interpreter ~/ld-linux-x86-64.so.2 '
                    '         --set-rpath ~ ~/node',
                    hide=True,
                )
            self._parallel_ssh(all_hosts, _distribute)

            # --- Generate key + build committee ---
            Print.info('Generating configuration...')
            key_file = PathMaker.key_file(0)
            subprocess.run(CommandMaker.generate_key(key_file).split(), check=True)
            key = Key.from_file(key_file)
            names = [key.name]

            committee = CloudLabReplayCommittee(
                names, self.BASE_PORT, self.num_workers,
                primary_ip, worker_ips, executor_ips,
            )
            committee.set_account_ranges({key.name: (0, self.num_accounts)})
            committee.print(PathMaker.committee_file())

            # --- Node parameters ---
            remote_csv = os.path.basename(self.replay_csv)
            self.node_parameters.set_replay_params(
                remote_csv, self.replay_tx_size, self.num_workers,
                self.executor_skew_weights, self.distributed_tx_rate,
            )
            self.node_parameters.set_executor_params(
                num_executors=self.num_executors,
                num_accounts=self.num_accounts,
                min_balance=10_000,
                max_balance=100_000,
                sharding_strategy='range',
                no_send_payment_tx=self.no_send_payment,
                use_new_scheduler=self.use_new_scheduler,
            )
            self.node_parameters.print(PathMaker.parameters_file())

            # --- Upload configs to every node ---
            Print.info('Uploading configs...')

            def _upload_primary():
                conn = self._ssh(primary_ssh)
                conn.run(
                    f'{CommandMaker.remote_cleanup()} || true && mkdir -p logs .db-0',
                    hide=True,
                )
                for f in [PathMaker.committee_file(), PathMaker.parameters_file(), key_file]:
                    conn.put(f, '.')

            def _upload_worker(w):
                conn = self._ssh(worker_ssh[w])
                conn.run(
                    f'{CommandMaker.remote_cleanup()} || true && mkdir -p logs .db-0-{w}',
                    hide=True,
                )
                for f in [PathMaker.committee_file(), PathMaker.parameters_file(), key_file]:
                    conn.put(f, '.')

            def _upload_executor(e):
                conn = self._ssh(executor_ssh[e])
                conn.run(
                    f'{CommandMaker.remote_cleanup()} || true && mkdir -p logs .db-exec-0-{e}',
                    hide=True,
                )
                for f in [PathMaker.committee_file(), PathMaker.parameters_file(), key_file]:
                    conn.put(f, '.')

            total = 1 + self.num_workers + self.num_executors
            with ThreadPoolExecutor(max_workers=total) as pool:
                futures = [pool.submit(_upload_primary)]
                futures += [pool.submit(_upload_worker, w) for w in range(self.num_workers)]
                futures += [pool.submit(_upload_executor, e) for e in range(self.num_executors)]
                for f in as_completed(futures):
                    f.result()

            # --- Upload replay CSV to primary + all workers ---
            Print.info('Uploading replay CSV...')
            csv_hosts = [primary_ssh] + worker_ssh

            def _upload_csv(host):
                self._ssh(host).put(self.replay_csv, '.')
            self._parallel_ssh(csv_hosts, _upload_csv)

            # --- TC shaping: bandwidth cap on worker->executor and executor->executor ---
            self._apply_tc_shaping(worker_ssh, executor_ssh, executor_ips)

            # --- Start executors ---
            Print.info('Starting executors...')
            validator_ranges = [(0, self.num_accounts)]
            for e in range(self.num_executors):
                cmd = CommandMaker.run_executor(
                    PathMaker.key_file(0),
                    PathMaker.committee_file(),
                    f'.db-exec-0-{e}',
                    PathMaker.parameters_file(),
                    e,
                    validator_ranges,
                    debug=debug,
                )
                if self.in_memory_store:
                    cmd = f'IN_MEMORY_STORE=1 {cmd}'
                self._background_run(executor_ssh[e], cmd, PathMaker.executor_log_file(0, e))

            sleep(1)

            # --- Start primary ---
            Print.info('Starting primary...')
            primary_cmd = CommandMaker.run_primary(
                PathMaker.key_file(0),
                PathMaker.committee_file(),
                PathMaker.db_path(0),
                PathMaker.parameters_file(),
                debug=debug,
                in_memory_store=self.in_memory_store,
            )
            self._background_run(primary_ssh, primary_cmd, PathMaker.primary_log_file(0))

            # --- Wait for primary's worker_to_primary port ---
            w2p_addr = committee.json['authorities'][key.name]['primary']['worker_to_primary']
            w2p_host, w2p_port = w2p_addr.rsplit(':', 1)
            Print.info(f'Waiting for primary port {w2p_host}:{w2p_port}...')
            deadline = _now() + 120
            while True:
                result = self._ssh(primary_ssh).run(
                    f'bash -c "echo >/dev/tcp/{w2p_host}/{w2p_port}"',
                    hide=True, warn=True,
                )
                if not result.failed:
                    break
                assert _now() <= deadline, 'Primary did not become ready within 120s'
                sleep(0.5)
            Print.info('Primary ready.')

            # --- Start workers ---
            Print.info('Starting workers...')
            for w in range(self.num_workers):
                cmd = CommandMaker.run_worker(
                    PathMaker.key_file(0),
                    PathMaker.committee_file(),
                    PathMaker.db_path(0, w),
                    PathMaker.parameters_file(),
                    w,
                    debug=debug,
                    in_memory_store=self.in_memory_store,
                )
                self._background_run(worker_ssh[w], cmd, PathMaker.worker_log_file(0, w))

            # --- Run ---
            Print.info(f'Running replay ({self.duration} sec)...')
            sleep(self.duration)

            # --- Kill ---
            Print.info('Stopping processes...')
            self._kill(all_hosts, delete_logs=False)
            sleep(1)

            # --- Download logs ---
            Print.info('Downloading logs...')
            subprocess.run([CommandMaker.clean_logs()], shell=True, stderr=subprocess.DEVNULL)
            subprocess.run(['mkdir', '-p', PathMaker.logs_path()])

            def _dl_primary():
                self._ssh(primary_ssh).get(
                    PathMaker.primary_log_file(0),
                    local=PathMaker.primary_log_file(0),
                )

            def _dl_worker(w):
                self._ssh(worker_ssh[w]).get(
                    PathMaker.worker_log_file(0, w),
                    local=PathMaker.worker_log_file(0, w),
                )

            def _dl_executor(e):
                self._ssh(executor_ssh[e]).get(
                    PathMaker.executor_log_file(0, e),
                    local=PathMaker.executor_log_file(0, e),
                )

            with ThreadPoolExecutor(max_workers=total) as pool:
                futures = [pool.submit(_dl_primary)]
                futures += [pool.submit(_dl_worker, w) for w in range(self.num_workers)]
                futures += [pool.submit(_dl_executor, e) for e in range(self.num_executors)]
                for f in as_completed(futures):
                    f.result()

            # --- Parse logs ---
            Print.info('Parsing logs...')
            from benchmark.replay_logs import ReplayLogParser, ReplayParseError
            try:
                result = ReplayLogParser.process(
                    PathMaker.logs_path(), tx_size=self.replay_tx_size,
                )
                return result.result()
            except ReplayParseError as e:
                Print.warn(f'Failed to parse replay logs: {e}')
                return ''

        except (subprocess.SubprocessError, Exception) as e:
            self._kill(all_hosts, delete_logs=False)
            raise BenchError('Failed to run CloudLab replay benchmark', e)


class CloudLabInstaller:
    """Install dependencies on CloudLab machines."""

    def __init__(self, manifest_file, username):
        from benchmark.manifest import Manifest
        self._hosts = Manifest.load_ssh_only(manifest_file, username)
        self.username = username

    def _ssh(self, host):
        return Connection(host, user=self.username)

    def install(self):
        """Install Rust and build dependencies on all machines in parallel."""
        hosts = self._hosts
        Print.info(f'Installing dependencies on {len(hosts)} machines in parallel...')
        cmd = ' && '.join([
            'sudo apt-get update',
            'sudo apt-get -y install build-essential cmake clang',
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',
            # Install Docker
            'sudo apt-get -y install ca-certificates curl gnupg lsb-release',
            'sudo install -m 0755 -d /etc/apt/keyrings',
            'curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg',
            'sudo chmod a+r /etc/apt/keyrings/docker.gpg',
            'echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null',
            'sudo apt-get update',
            'sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin',
            f'sudo usermod -aG docker $USER',
        ])

        def _install_one(host):
            c = self._ssh(host)
            c.run(cmd, hide=True)
            return host

        failed = []
        with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
            futures = {pool.submit(_install_one, h): h for h in hosts}
            for future in as_completed(futures):
                host = futures[future]
                try:
                    future.result()
                    Print.info(f'  Done: {host}')
                except Exception as e:
                    Print.warn(f'  FAILED: {host}: {e}')
                    failed.append(host)

        assert not failed, f'Installation failed on: {", ".join(failed)}'
        Print.heading(f'Installed on {len(hosts)} machines')
