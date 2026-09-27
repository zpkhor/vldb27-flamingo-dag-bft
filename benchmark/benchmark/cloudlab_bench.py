# CloudLab benchmark orchestrator.
# Deploys Narwhal on CloudLab physical machines using SSH + tmux.
import json
import os
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from os.path import basename, splitext
from time import sleep, time as _now

from fabric import Connection

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    CloudLabCommittee,
    NodeParameters,
    BenchParameters,
    ConfigError,
)
from benchmark.instance import CloudLabInstanceManager
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker
from benchmark.docker_bench import egress_bw_capacity_tps


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
        local_orch=False,
        node_offset=0,
        pin_cpus=False,
    ):
        self.username = username
        self.latency_ms = latency_ms
        self.in_memory_store = in_memory_store
        self.num_executors = num_executors
        self.no_send_payment = no_send_payment
        self.zipf_exponent = zipf_exponent
        self.new_scheduler = new_scheduler
        self.executor_bw_kbps = executor_bw_kbps
        self.local_orch = local_orch
        self.node_offset = node_offset
        self.pin_cpus = pin_cpus

        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        if local_orch:
            self.manager = CloudLabInstanceManager.make(local_orch=True)
        else:
            self.manager = CloudLabInstanceManager.make(manifest_file, username)
        nodes = self.bench_parameters.nodes[0]
        # Validators use node-off..off+nodes-1, executors use node-off+nodes..off+2*nodes-1
        required_machines = node_offset + nodes + (nodes if num_executors > 0 else 0)
        assert required_machines <= self.manager.num_validators(), (
            f'Need {required_machines} node-X machines '
            f'(offset={node_offset} + {nodes} validators + {nodes if num_executors > 0 else 0} executor hosts) '
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

    @staticmethod
    def _pin_wrap(cmd, cpu_range, tokio_threads):
        """Wrap cmd with taskset + TOKIO_WORKER_THREADS via bash -c.

        Using bash -c so that inline env assignments (e.g. IN_MEMORY_STORE=1)
        in cmd are evaluated by bash, not passed as args to taskset.
        export ensures TOKIO_WORKER_THREADS is inherited by the child binary.
        """
        return (
            f"taskset -c {cpu_range} bash -c "
            f"'export TOKIO_WORKER_THREADS={tokio_threads}; {cmd}'"
        )

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
        """Egress-only HTB + netem on validators/executors."""
        Print.info('Applying TC QoS shaping...')

        primary_bw = self.primary_bw_kbps
        half_lat = self.latency_ms // 2  # egress-only delay per hop

        # Primary-to-primary ports for traffic classification
        primary_ports = [
            auth['primary']['primary_to_primary'].split(':')[1]
            for auth in committee.json['authorities'].values()
        ]

        # Worker transaction ports (where clients connect) — exempt from worker BW cap
        # so migrated client traffic doesn't consume worker egress bandwidth.
        tx_ports = [
            worker['transactions'].split(':')[1]
            for auth in committee.json['authorities'].values()
            for worker in auth['workers'].values()
        ]

        # Executor IPs for traffic exemption on validator machines
        e_ip_set = set(self.manager.validator_ips()[nodes:2 * nodes]) if e_ssh else set()

        def _shape_validator(i):
            worker_bw = self.worker_bws_kbps[i]
            client_bw = 1_000_000   # 1 Gbit — uncapped client sends
            executor_bw = 5_000_000  # 5 Gbit — co-located, no real limit
            total_bw = primary_bw + worker_bw + client_bw + (executor_bw if e_ssh else 0)
            # Port filters: classify primary traffic into class 1:10
            port_filters = []
            for p in primary_ports:
                port_filters.append(f'tc filter add dev $IFACE parent 1:0 protocol ip prio 2 u32 match ip sport {p} 0xffff flowid 1:10')
                port_filters.append(f'tc filter add dev $IFACE parent 1:0 protocol ip prio 2 u32 match ip dport {p} 0xffff flowid 1:10')

            # Port filters: classify client→worker transaction traffic into class 1:40
            client_filters = []
            for p in tx_ports:
                client_filters.append(f'tc filter add dev $IFACE parent 1:0 protocol ip prio 2 u32 match ip dport {p} 0xffff flowid 1:40')

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
                # HTB: primary (1:10) + worker (1:20, default) + client (1:40) + executor (1:30, no delay)
                f'tc qdisc add dev $IFACE root handle 1: htb default 20',
                f'tc class add dev $IFACE parent 1: classid 1:1 htb rate {total_bw}kbit',
                f'tc class add dev $IFACE parent 1:1 classid 1:10 htb rate {primary_bw}kbit ceil {primary_bw}kbit prio 0',
                f'tc class add dev $IFACE parent 1:1 classid 1:20 htb rate {worker_bw}kbit ceil {worker_bw}kbit prio 1',
                # Client class: uncapped, with netem delay (traffic crosses the real network)
                f'tc class add dev $IFACE parent 1:1 classid 1:40 htb rate {client_bw}kbit ceil {client_bw}kbit prio 2',
                *executor_class_lines,
                f'tc qdisc add dev $IFACE parent 1:10 handle 10: netem delay {half_lat}ms limit 10000',
                f'tc qdisc add dev $IFACE parent 1:20 handle 20: netem delay {half_lat}ms limit 10000',
                f'tc qdisc add dev $IFACE parent 1:40 handle 40: netem delay {half_lat}ms limit 10000',
                *executor_filters,
                *port_filters,
                *client_filters,
            ])
            self._ssh(v_ssh[i]).run(f'sudo bash -c \'{script}\'', hide=True)

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
            f'worker_bws={[self.worker_bws_kbps[i] for i in range(nodes)]}kbps'
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

        # Slice to requested node count (offset allows parallel runs on disjoint nodes)
        off = self.node_offset
        v_ssh = self.manager.validator_ssh_hosts()[off:off + nodes]
        v_ips = self.manager.validator_ips()[off:off + nodes]

        # Executor machines: next `nodes` machines after validators
        if self.num_executors > 0:
            e_ssh = self.manager.validator_ssh_hosts()[off + nodes:off + 2 * nodes]
            e_ips = self.manager.validator_ips()[off + nodes:off + 2 * nodes]
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
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path(), env={**os.environ, 'RUSTFLAGS': '-Awarnings'})

            # Create local symlinks
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Rsync binaries and required shared libs to all machines (parallel)
            Print.info(f'Distributing binaries to {len(all_ssh)} machines...')
            binary_path = PathMaker.binary_path()
            libs_to_ship = []
            for lib_name in ['libstdc++.so.6', 'libgcc_s.so.1']:
                result = subprocess.run(
                    f'ldd {binary_path}/node | grep {lib_name}',
                    shell=True, capture_output=True, text=True,
                )
                for line in result.stdout.strip().split('\n'):
                    parts = line.strip().split('=>')
                    if len(parts) == 2:
                        path = parts[1].strip().split('(')[0].strip()
                        if path and '/lib/x86_64-linux-gnu/' not in path:
                            libs_to_ship.append(path)

            def _distribute_one(host):
                subprocess.run(
                    f'rsync -azL {binary_path}/node {binary_path}/benchmark_client '
                    f'{" ".join(libs_to_ship)} '
                    f'{self.username}@{host}:~/',
                    shell=True, check=True,
                )
                if libs_to_ship:
                    self._ssh(host).run(
                        'sudo cp ~/libstdc++.so.6 ~/libgcc_s.so.1 '
                        '/usr/lib/x86_64-linux-gnu/ && sudo ldconfig',
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

            # Build committee (co-located clients on validator machines)
            committee = CloudLabCommittee(
                names, self.BASE_PORT, self.workers, v_ips,
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
                    capacity = egress_bw_capacity_tps(
                        self.worker_bws_kbps[i] / 1000, self.tx_size, nodes - 1
                    )
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
                committee.set_client_reply_addresses({i: f'{v_ips[i]}:{exec_reply_port}' for i in range(nodes)})

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

            # Build per-validator client commands (co-located)
            workers_addresses = committee.workers_addresses(self.faults)

            # --validator-workers args (same for all clients)
            vw_args_parts = []
            for name in names:
                auth = committee.json['authorities'][name]
                waddrs = '+'.join(
                    auth['workers'][wid]['transactions']
                    for wid in sorted(auth['workers'].keys())
                )
                vw_args_parts.append(f'--validator-workers {name}:{waddrs}')
            vw_args = ' '.join(vw_args_parts)

            # --account-ranges args (same for all clients)
            ar_args_parts = []
            for name in names:
                start, count = account_ranges[name]
                ar_args_parts.append(f'--account-ranges {name}:{start}:{count}')
            ar_args = ' '.join(ar_args_parts)

            # All worker transaction addresses
            all_worker_addrs = [addr for addresses in workers_addresses for _, addr in addresses]
            all_nodes_arg = ' '.join(all_worker_addrs)

            rr_flag = ' --round-robin' if self.round_robin else ''
            no_send_flag = ' --no-send-payment' if self.no_send_payment else ''
            zipf_flag = f' --zipf-exponent {self.zipf_exponent}' if self.zipf_exponent > 0 else ''

            e_skew_flag = ''
            if self.e_skew_weights:
                e_skew_weights_str = ','.join(str(w) for w in self.e_skew_weights)
                e_skew_flag = f' --executor-skew-weights {e_skew_weights_str}'

            # Per-validator rate splitting
            import math
            weights = (self.rate_weights or [1] * nodes)[:good_nodes]
            weight_sum = sum(weights)
            balanced_rate = math.ceil(rate / good_nodes)

            client_commands = []
            for i in range(good_nodes):
                validator_rate = math.ceil(rate * weights[i] / weight_sum)
                reply_addr = committee.json['authorities'][names[i]]['client_reply']

                exec_reply_flag = ''
                if self.num_executors > 0:
                    exec_reply_addr = f'{v_ips[i]}:{self.BASE_PORT + 9000}'
                    exec_reply_flag = f' --execution-reply-addr {exec_reply_addr}'

                cmd = (
                    f'ulimit -n 65536 && ./benchmark_client --size {self.tx_size} '
                    f'--rate {validator_rate} --balanced-rate {balanced_rate} '
                    f'--nodes {all_nodes_arg} '
                    f'{ar_args} '
                    f'--reply-addr {reply_addr} --own-validator {names[i]} '
                    f'{vw_args}{rr_flag}{no_send_flag}{zipf_flag}{exec_reply_flag}{e_skew_flag}'
                    f' --rampup-secs {self.warmup}'
                )
                client_commands.append(cmd)

            # Apply QoS TC shaping BEFORE starting processes so TCP connections
            # are established with the correct RTT from the start
            self._apply_tc_shaping(v_ssh, e_ssh, nodes, committee)

            # Start primaries and workers (all nodes in parallel)
            Print.info('Starting primaries and workers...')
            CPU_OS_RESERVED = 2
            WORKER_CORES    = 10  # cores 2-11
            PRIMARY_CORES   = 6   # cores 12-17
            CLIENT_CORES    = 2   # cores 18-19
            worker_cpu_start  = CPU_OS_RESERVED
            worker_cpu_end    = worker_cpu_start + WORKER_CORES - 1    # 11
            primary_cpu_start = CPU_OS_RESERVED + WORKER_CORES         # 12
            primary_cpu_end   = primary_cpu_start + PRIMARY_CORES - 1  # 17
            client_cpu_start  = primary_cpu_end + 1                    # 18
            client_cpu_end    = client_cpu_start + CLIENT_CORES - 1    # 19
            tasks = []
            for i in range(good_nodes):
                primary_cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    debug=debug,
                    in_memory_store=self.in_memory_store,
                )
                if self.pin_cpus:
                    primary_cmd = self._pin_wrap(primary_cmd, f'{primary_cpu_start}-{primary_cpu_end}', PRIMARY_CORES)
                tasks.append((v_ssh[i], primary_cmd, PathMaker.primary_log_file(i)))
                for wid, _ in workers_addresses[i]:
                    worker_cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, wid),
                        PathMaker.parameters_file(),
                        wid,
                        debug=debug,
                        in_memory_store=self.in_memory_store,
                    )
                    if self.pin_cpus:
                        worker_cmd = self._pin_wrap(worker_cmd, f'{worker_cpu_start}-{worker_cpu_end}', WORKER_CORES)
                    tasks.append((v_ssh[i], worker_cmd, PathMaker.worker_log_file(i, wid)))

            with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
                futures = [pool.submit(self._background_run, host, cmd, log) for host, cmd, log in tasks]
                failed = []
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        failed.append(e)
            assert not failed, f'Failed to start primaries/workers: {failed}'

            # Start executors (on dedicated machines)
            if self.num_executors > 0:
                Print.info('Starting executors...')
                cpus_per_exec = 6
                cpu_offset = 2  # CPUs 0-1 reserved for OS
                for i in range(nodes):
                    for e in range(self.num_executors):
                        cpu_start = cpu_offset + e * cpus_per_exec
                        cpu_end = cpu_start + cpus_per_exec - 1
                        executor_cmd = (
                            f'taskset -c {cpu_start}-{cpu_end} '
                            + CommandMaker.run_executor(
                                PathMaker.key_file(i),
                                PathMaker.committee_file(),
                                f'.db-exec-{i}-{e}',
                                PathMaker.parameters_file(),
                                e,
                                list(zip(acct_starts, acct_counts)),
                                debug=debug,
                            )
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

            # Start co-located clients (one per validator) with synchronized start time
            Print.info('Starting clients...')
            import time
            start_at = time.time() + 10  # 10s buffer for SSH stagger + client init
            with ThreadPoolExecutor(max_workers=good_nodes) as pool:
                futures = []
                for i in range(good_nodes):
                    cmd = client_commands[i] + f' --start-at {start_at}'
                    if self.pin_cpus:
                        cmd = self._pin_wrap(cmd, f'{client_cpu_start}-{client_cpu_end}', CLIENT_CORES)
                    futures.append(pool.submit(
                        self._background_run,
                        v_ssh[i], cmd, PathMaker.client_log_file(i, 0),
                    ))
                for f in as_completed(futures):
                    f.result()

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
                # Download co-located client log
                if i < good_nodes:
                    conn.get(
                        PathMaker.client_log_file(i, 0),
                        local=_local(PathMaker.client_log_file(i, 0)),
                    )

            def _download_executor(i):
                conn = self._ssh(e_ssh[i])
                for e in range(self.num_executors):
                    conn.get(
                        PathMaker.executor_log_file(i, e),
                        local=_local(PathMaker.executor_log_file(i, e)),
                    )

            with ThreadPoolExecutor(max_workers=nodes + len(e_ssh)) as pool:
                futures = [pool.submit(_download_validator, i) for i in range(nodes)]
                for i in range(len(e_ssh)):
                    futures.append(pool.submit(_download_executor, i))
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


class CloudLabInstaller:
    """Install dependencies on CloudLab machines."""

    def __init__(self, manifest_file, username, local_orch=False):
        from benchmark.manifest import Manifest
        if local_orch:
            self._hosts = Manifest.load_ssh_only_from_etc_hosts()
        else:
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
