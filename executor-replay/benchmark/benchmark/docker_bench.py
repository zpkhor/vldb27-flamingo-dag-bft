# Copyright(C) Facebook, Inc. and its affiliates.
import json
import os
import socket
import subprocess
import sys
from time import sleep, time as _now

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    DockerCommittee,
    NodeParameters,
    BenchParameters,
    ConfigError,
)
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


def max_capacity_tps_by_ingress(bw_mbit, tx_size):
    """Max TPS the system can sustain given ingress bandwidth.

    bw_mbit: a single int (2f+1)-th slowest node is used as the system-wide bottleneck (quorum threshold).
    tx_size: transaction payload size in bytes
    """
    bw_bytes = bw_mbit * 1_000_000 / 8
    return int(bw_bytes / (tx_size + 44))


def egress_bw_capacity_tps(bw_mbit, tx_size, num_peers):
    """Max sustainable TPS a node can broadcast to num_peers given egress bandwidth.

    bw_mbit: egress bandwidth in megabits/s
    tx_size: transaction payload size in bytes
    num_peers: number of peer validators to fan-out to (i.e. nodes - 1)
    """
    bw_bytes = bw_mbit * 1_000_000 / 8
    # 40 TCP/IP + 4 length-prefix codec overhead per transaction
    return int(bw_bytes / (tx_size + 44) / num_peers * 0.9)


def _port_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=0.3):
            return True
    except OSError:
        return False


class DockerBench:
    BASE_PORT = 5000
    NETWORK_SUBNET = "172.20.0.0/16"
    NETWORK_NAME = "narwhal-net"
    IMAGE_NAME = "narwhal-bench"
    CONTAINER_PREFIX = "narwhal-validator"

    def __init__(
        self,
        bench_parameters_dict,
        node_parameters_dict,
        worker_bws,
        latency="0ms",
        jitter="0ms",
        cpus_per_validator=0,
        lan_bandwidth="100gbit",
        check_mismatch=False,
        primary_bw="500mbit",
        baseline=False,
        tc_netem_limit=0,
        tc_netem_limit_client=0,
        round_robin=False,
        num_executors=0,
        no_send_payment=False,
        zipf_exponent=0.0,
        in_memory_store=False,
    ):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)

        self.latency = latency
        self.jitter = jitter
        self.cpus_per_validator = cpus_per_validator
        self.lan_bandwidth = lan_bandwidth
        self.check_mismatch = check_mismatch
        self.tc_netem_limit = tc_netem_limit
        self.tc_netem_limit_client = tc_netem_limit_client

        # QoS bandwidth allocation
        nodes = self.bench_parameters.nodes[0]
        assert len(worker_bws) == nodes, (
            f"worker_bws has {len(worker_bws)} entries but nodes={nodes}"
        )

        self.baseline = baseline
        self.round_robin = round_robin
        self.primary_bw = primary_bw
        self.num_executors = num_executors
        self.no_send_payment = no_send_payment
        self.zipf_exponent = zipf_exponent
        self.in_memory_store = in_memory_store
        primary_mbit = self._parse_bw_mbit(primary_bw)
        self.worker_bws = []
        self.total_bws = []
        for i, wbw in enumerate(worker_bws):
            worker_mbit = self._parse_bw_mbit(wbw)
            assert worker_mbit > 0, (
                f"worker_bw for validator {i} ({wbw}={worker_mbit}mbit) must be > 0"
            )
            self.worker_bws.append(self._format_bw(worker_mbit))
            executor_mbit = 5000 if num_executors > 0 else 0  # 5Gbit for co-located executor traffic
            self.total_bws.append(self._format_bw(primary_mbit + worker_mbit + executor_mbit))

    @staticmethod
    def _parse_bw_mbit(bw_str):
        """Parse a TC bandwidth string (e.g., '10gbit', '500mbit') to megabits."""
        bw_str = bw_str.strip().lower()
        if bw_str.endswith('gbit'):
            return int(float(bw_str[:-4]) * 1000)
        elif bw_str.endswith('mbit'):
            return int(float(bw_str[:-4]))
        elif bw_str.endswith('kbit'):
            return max(1, int(float(bw_str[:-4]) / 1000))
        raise ValueError(f'Cannot parse bandwidth: {bw_str}')

    @staticmethod
    def _format_bw(mbit):
        """Format megabits as a TC bandwidth string."""
        if mbit >= 1000 and mbit % 1000 == 0:
            return f'{mbit // 1000}gbit'
        return f'{mbit}mbit'

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _container_ip(self, i):
        return f"172.20.0.{10 + i}"

    @staticmethod
    def _docker_down():
        try:
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    "docker-compose.yml",
                    "down",
                    "--remove-orphans",
                ],
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
        except subprocess.SubprocessError:
            pass

    @staticmethod
    def _entrypoint_hash():
        import hashlib
        h = hashlib.sha256()
        for path in ['docker/entrypoint.sh', 'docker/client-entrypoint.sh', 'docker/executor-entrypoint.sh']:
            with open(path, 'rb') as f:
                h.update(f.read())
        return h.hexdigest()

    @staticmethod
    def _image_exists():
        result = subprocess.run(
            ['docker', 'image', 'inspect', DockerBench.IMAGE_NAME],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0

    def _build_image(self):
        cmd = [
            "docker",
            "build",
            "-t",
            self.IMAGE_NAME,
            "-f",
            "docker/Dockerfile",
            "docker/",
        ]
        subprocess.run(cmd, check=True)

    def _generate_compose(self, nodes, commands_per_validator, wait_ports_per_validator, client_command, client_ip, container_ips, client_wait_ports, primary_ports_str, executor_commands=None, executor_ips=None):
        """Generate docker-compose.yml programmatically."""
        # Halve latency for netem: egress-only delay on both endpoints means
        # each side contributes half the RTT (self.latency is the target RTT).
        if self.latency not in ('0ms', ''):
            half_lat_ms = int(self.latency.rstrip('ms')) // 2
            tc_latency = f'{half_lat_ms}ms'
        else:
            tc_latency = self.latency

        services = []
        for i in range(nodes):
            ip = self._container_ip(i)
            cmds = commands_per_validator[i]

            cpuset = ""
            if self.cpus_per_validator > 0:
                slot = self.cpus_per_validator
                start = i * slot
                end = start + self.cpus_per_validator - 1
                cpuset = f'\n    cpuset: "{start}-{end}"'

            if self.cpus_per_validator > 0:
                tokio_threads = self.cpus_per_validator // (1 + self.workers)
            else:
                tokio_threads = 0

            service = f"""  validator-{i}:
    image: {self.IMAGE_NAME}
    container_name: {self.CONTAINER_PREFIX}-{i}
    working_dir: /app
    cap_add:
      - NET_ADMIN{cpuset}
    networks:
      {self.NETWORK_NAME}:
        ipv4_address: {ip}
    volumes:
      - ./node:/app/node:ro
      - ./.node-{i}.json:/app/.node-{i}.json:ro
      - ./.committee.json:/app/.committee.json:ro
      - ./.parameters.json:/app/.parameters.json:ro
      - ./logs:/logs:rw
      - ./.db-{i}:/app/.db-{i}:rw"""

            # Mount worker db dirs
            for w in range(self.workers):
                service += f"""
      - ./.db-{i}-{w}:/app/.db-{i}-{w}:rw"""

            tokio_env = f"\n      - TOKIO_WORKER_THREADS={tokio_threads}" if tokio_threads > 0 else ""
            netem_limit_env = f"\n      - TC_NETEM_LIMIT={self.tc_netem_limit}" if self.tc_netem_limit > 0 else ""
            # Executor IPs for this validator (for TC bypass)
            executor_ips_env = ""
            if executor_ips and self.num_executors > 0:
                v_exec_ips = executor_ips[i * self.num_executors:(i + 1) * self.num_executors]
                executor_ips_env = f"\n      - EXECUTOR_IPS={' '.join(v_exec_ips)}"
            service += f"""
    environment:
      - VALIDATOR_ID={i}
      - TC_BANDWIDTH={self.total_bws[i]}
      - TC_PRIMARY_BW={self.primary_bw}
      - TC_WORKER_BW={self.worker_bws[i]}
      - TC_LATENCY={tc_latency}
      - TC_JITTER={self.jitter}
      - TC_LAN_BANDWIDTH={self.lan_bandwidth}
      - OWN_CLIENT_IP={client_ip}
      - PRIMARY_PORTS={primary_ports_str}{tokio_env}{netem_limit_env}{executor_ips_env}
      - PRIMARY_CMD={cmds['primary']}"""

            # Join worker commands with semicolons
            worker_cmd = ";".join(cmds["workers"])
            service += f"""
      - WORKER_CMD={worker_cmd}"""

            service += f"""
      - WAIT_PORTS={wait_ports_per_validator[i]}"""

            services.append(service)

        # Executor containers
        if executor_commands and executor_ips:
            exec_idx = 0
            for i in range(nodes):
                for e in range(self.num_executors):
                    e_ip = executor_ips[exec_idx]
                    e_cmd = executor_commands[(i, e)]

                    e_cpuset = ""
                    if self.cpus_per_validator > 0:
                        # Executors get CPU after validators and client
                        slot = self.cpus_per_validator
                        # validators: [0, nodes*slot)
                        # client: [nodes*slot, nodes*slot + 2*nodes)
                        # executors: after client
                        client_cpus = 2 * nodes
                        exec_cpus_per = 8  # TODO: make configurable
                        e_start = nodes * slot + client_cpus + exec_idx * exec_cpus_per
                        e_end = e_start + exec_cpus_per - 1
                        e_cpuset = f'\n    cpuset: "{e_start}-{e_end}"'

                    service = f"""  executor-{i}-{e}:
    image: {self.IMAGE_NAME}
    container_name: narwhal-executor-{i}-{e}
    working_dir: /app
    cap_add:
      - NET_ADMIN{e_cpuset}
    entrypoint: ["/executor-entrypoint.sh"]
    networks:
      {self.NETWORK_NAME}:
        ipv4_address: {e_ip}
    volumes:
      - ./node:/app/node:ro
      - ./.node-{i}.json:/app/.node-{i}.json:ro
      - ./.committee.json:/app/.committee.json:ro
      - ./.parameters.json:/app/.parameters.json:ro
      - ./logs:/logs:rw
    environment:
      - EXECUTOR_CMD={e_cmd}"""

                    services.append(service)
                    exec_idx += 1

        # Single client container
        max_bw = -1
        for bw in self.total_bws:
            max_bw = max(max_bw, self._parse_bw_mbit(bw))
        max_bw = self._format_bw(max_bw)

        client_cpuset = ""
        if self.cpus_per_validator > 0:
            slot = self.cpus_per_validator
            c_start = nodes * slot
            client_cpuset = f'\n    cpuset: "{c_start}-{c_start + 2 * nodes - 1}"'

        netem_limit_client_env = f"\n      - TC_NETEM_LIMIT_CLIENT={self.tc_netem_limit_client}" if self.tc_netem_limit_client > 0 else ""
        validator_ips_str = " ".join(container_ips)
        service = f"""  client-0:
    image: {self.IMAGE_NAME}
    container_name: narwhal-client-0
    working_dir: /app
    cap_add:
      - NET_ADMIN{client_cpuset}
    entrypoint: ["/client-entrypoint.sh"]
    networks:
      {self.NETWORK_NAME}:
        ipv4_address: {client_ip}
    volumes:
      - ./benchmark_client:/app/benchmark_client:ro
      - ./logs:/logs:rw
    environment:
      - CLIENT_CMD={client_command}
      - NUM_REGIONS={nodes}
      - VALIDATOR_IPS={validator_ips_str}
      - TC_LATENCY={tc_latency}
      - TC_JITTER={self.jitter}
      - TC_BANDWIDTH={max_bw}
      - WAIT_PORTS={client_wait_ports}{netem_limit_client_env}"""

        services.append(service)

        compose = f"""services:
{chr(10).join(services)}

networks:
  {self.NETWORK_NAME}:
    driver: bridge
    ipam:
      config:
        - subnet: {self.NETWORK_SUBNET}
"""
        with open("docker-compose.yml", "w") as f:
            f.write(compose)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading("Starting Docker benchmark")

        self._docker_down()

        try:
            Print.info("Setting up testbed...")
            nodes, rate = self.nodes[0], self.rate[0]

            # Clean root-owned files from previous Docker run, then do regular cleanup.
            subprocess.run(
                'docker run --rm -v "$PWD":/work -w /work ubuntu:24.04 '
                'sh -c "rm -rf logs .db-*"',
                shell=True,
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
            cmd = f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            container_ips = [self._container_ip(i) for i in range(nodes)]

            # Executor IPs: after validators, before client
            executor_ips = []
            for i in range(nodes):
                for e in range(self.num_executors):
                    executor_ips.append(f"172.20.0.{10 + nodes + 1 + i * self.num_executors + e}")
            client_ip = f"172.20.0.{10 + nodes + 1 + nodes * self.num_executors}"

            committee = DockerCommittee(
                names, self.BASE_PORT, self.workers, container_ips, client_ip,
                num_executors=self.num_executors, executor_ips=executor_ips,
            )
            
            if not self.baseline:
                capacities = [] # max tps/validator
                for i in range(nodes):
                    capacity = egress_bw_capacity_tps(
                        self._parse_bw_mbit(self.worker_bws[i]), self.tx_size, nodes - 1
                    )
                    capacities.append(capacity)
                committee.set_capacities(capacities)
            latency_ms = (int(self.latency.rstrip('ms')) if self.latency not in ('0ms', '') else 0) // 2
            latency_matrix = {
                name: {other: (0 if name == other else latency_ms) for other in names}
                for name in names
            }
            committee.set_latency_matrix(latency_matrix)

            if self.baseline:
                self.node_parameters.json['baseline_mode'] = True

            num_accounts = self.bench_parameters.num_accounts

            # Set executor parameters in node params
            if self.num_executors > 0:
                self.node_parameters.set_executor_params(
                    num_executors=self.num_executors,
                    num_accounts=num_accounts,
                    min_balance=10_000,
                    max_balance=100_000,
                    sharding_strategy='range',
                    no_send_payment_tx=self.no_send_payment,
                    use_new_scheduler=False,
                )

            self.node_parameters.print(PathMaker.parameters_file())

            # Create db directories
            for i in range(nodes):
                subprocess.run(f"mkdir -p .db-{i}", shell=True)
                for w in range(self.workers):
                    subprocess.run(f"mkdir -p .db-{i}-{w}", shell=True)

            # Build commands for each validator (paths relative to /app/ inside container)
            workers_addresses = committee.workers_addresses(self.faults)

            v = "-vvv" if debug else "-vv"

            account_weights = self.bench_parameters.account_weights or [1] * nodes
            total_aw = sum(account_weights)
            acct_counts = [num_accounts * w // total_aw for w in account_weights]
            remainder = num_accounts - sum(acct_counts)
            for k in range(remainder):
                acct_counts[k] += 1
            acct_starts = []
            s = 0
            for c in acct_counts:
                acct_starts.append(s)
                s += c

            # Set account ranges in committee (keyed by public key)
            account_ranges = {
                name: (acct_starts[i], acct_counts[i])
                for i, name in enumerate(names)
            }
            committee.set_account_ranges(account_ranges)

            # Set client_reply_addresses for executor -> client replies (e2e latency)
            if self.num_executors > 0:
                # Use a port on the client container for execution replies
                exec_reply_port = self.BASE_PORT + 9000  # high enough to not conflict
                committee.set_client_reply_addresses({0: f'{client_ip}:{exec_reply_port}'})

            committee.print(PathMaker.committee_file())

            # Exclude faulty validators from running processes
            good_nodes = nodes - self.faults
            names = names[:good_nodes]
            container_ips = container_ips[:good_nodes]

            # Build --validator-workers args for all validators
            vw_args_parts = []
            for name in names:
                auth = committee.json['authorities'][name]
                waddrs = "+".join(
                    auth['workers'][wid]['transactions']
                    for wid in sorted(auth['workers'].keys())
                )
                vw_args_parts.append(f"--validator-workers {name}:{waddrs}")
            vw_args = " ".join(vw_args_parts)

            # Build --account-ranges args for all validators
            ar_args_parts = []
            for name in names:
                start, count = account_ranges[name]
                ar_args_parts.append(f"--account-ranges {name}:{start}:{count}")
            ar_args = " ".join(ar_args_parts)

            # Build --rate-weights aligned with sorted validator order
            weights = (self.rate_weights or [1] * nodes)[:good_nodes]
            name_to_idx = {name: i for i, name in enumerate(names)}
            sorted_names = [n for n in committee.sorted_authority_names() if n in name_to_idx]
            sorted_weights = [weights[name_to_idx[n]] for n in sorted_names]
            rate_weights_str = ",".join(str(w) for w in sorted_weights)

            # All worker transaction addresses (for single client --nodes and WAIT_PORTS)
            all_worker_addrs = [addr for addresses in workers_addresses for _, addr in addresses]
            all_nodes_arg = " ".join(all_worker_addrs)

            reply_addr = list(committee.json['authorities'].values())[0]['client_reply']

            rr_flag = " --round-robin" if self.round_robin else ""
            no_send_flag = " --no-send-payment" if self.no_send_payment else ""
            zipf_flag = f" --zipf-exponent {self.zipf_exponent}" if self.zipf_exponent > 0 else ""

            exec_reply_flag = ""
            if self.num_executors > 0:
                exec_reply_addr = f"{client_ip}:{self.BASE_PORT + 9000}"
                exec_reply_flag = f" --execution-reply-addr {exec_reply_addr}"

            e_skew_flag = ""
            if self.e_skew_weights:
                e_skew_weights_str = ",".join(str(w) for w in self.e_skew_weights)
                e_skew_flag = f" --executor-skew-weights {e_skew_weights_str}"

            client_command = (
                f"./benchmark_client --size {self.tx_size} "
                f"--rate {rate} --nodes {all_nodes_arg} "
                f"{ar_args} --rate-weights {rate_weights_str} "
                f"--reply-addr {reply_addr} --own-validator {names[0]} "
                f"{vw_args}{rr_flag}{no_send_flag}{zipf_flag}{exec_reply_flag}{e_skew_flag}"
                f" --rampup-secs {self.warmup}"
                f" 2> /logs/client-0-0.log"
            )

            commands_per_validator = {}
            ims_prefix = "IN_MEMORY_STORE=1 " if self.in_memory_store else ""
            for i, addresses in enumerate(workers_addresses):
                primary_cmd = (
                    f"{ims_prefix}./node {v} run --keys .node-{i}.json --committee .committee.json "
                    f"--store .db-{i} --parameters .parameters.json primary"
                    f" 2> /logs/primary-{i}.log"
                )

                worker_cmds = []
                for id, _ in addresses:
                    w_cmd = (
                        f"{ims_prefix}./node {v} run --keys .node-{i}.json --committee .committee.json "
                        f"--store .db-{i}-{id} --parameters .parameters.json worker --id {id}"
                    )
                    w_cmd += f" 2> /logs/worker-{i}-{id}.log"
                    worker_cmds.append(w_cmd)

                commands_per_validator[i] = {
                    "primary": primary_cmd,
                    "workers": worker_cmds,
                }

            # Build executor commands (separate containers)
            executor_commands = {}  # (validator_idx, executor_id) -> cmd
            for i in range(good_nodes):
                for e in range(self.num_executors):
                    range_args = ' '.join(
                        f'--validator-range {s}:{c}'
                        for s, c in zip(acct_starts, acct_counts)
                    )
                    e_cmd = (
                        f"./node {v} run --keys .node-{i}.json --committee .committee.json "
                        f"--store .db-{i} --parameters .parameters.json executor --id {e} "
                        f"{range_args}"
                        f" 2> /logs/executor-{i}-{e}.log"
                    )
                    executor_commands[(i, e)] = e_cmd

            # Compute remote wait ports for each validator.
            wait_ports_per_validator = {}
            for i, name in enumerate(names):
                wait_ports_per_validator[i] = " ".join(
                    committee.remote_addresses(name)
                )

            client_wait_ports = all_nodes_arg

            # Extract all primary_to_primary ports for QoS classification
            primary_ports = []
            for auth in committee.json['authorities'].values():
                addr = auth['primary']['primary_to_primary']
                primary_ports.append(addr.split(':')[1])
            primary_ports_str = " ".join(primary_ports)

            # Build Docker image.
            HASH_FILE = '.docker-image-hash'
            current_hash = self._entrypoint_hash()
            stored_hash = open(HASH_FILE).read().strip() if os.path.exists(HASH_FILE) else ''
            if not self._image_exists() or current_hash != stored_hash:
                Print.info("Building Docker image...")
                self._build_image()
                with open(HASH_FILE, 'w') as f:
                    f.write(current_hash)
            else:
                Print.info("Docker image up to date, skipping build.")

            # Generate docker-compose.yml.
            self._generate_compose(good_nodes, commands_per_validator, wait_ports_per_validator, client_command, client_ip, container_ips, client_wait_ports, primary_ports_str, executor_commands=executor_commands, executor_ips=executor_ips)

            # Start containers.
            Print.info("Starting containers...")
            _devnull = subprocess.DEVNULL if not sys.stdout.isatty() else None
            subprocess.run(
                ["docker", "compose", "-f", "docker-compose.yml", "up", "-d"],
                check=True,
                stdout=_devnull,
                stderr=_devnull,
            )

            with open(PathMaker.bench_params_file(), 'w') as f:
                json.dump({'duration': self.duration, 'warmup': self.warmup, 'faults': self.faults}, f)

            Print.info("Waiting for all containers to be ready...")
            addrs = [(a.split(':')[0], int(a.split(':')[1])) for a in client_wait_ports.split()]
            deadline = _now() + 120
            while True:
                if all(_port_open(h, p) for h, p in addrs):
                    break
                if _now() > deadline:
                    raise BenchError("Containers did not become ready within 120s", Exception())
                sleep(0.5)
            Print.info("All containers ready.")

            # Wait for benchmark duration.
            Print.info(f"Running benchmark ({self.duration} sec)...")
            sleep(self.duration)

            # Stop containers.
            Print.info("Stopping containers...")
            self._docker_down()
            sleep(1)

            # Parse logs and return the parser.
            Print.info("Parsing logs...")
            return LogParser.process(
                PathMaker.logs_path(),
                faults=self.faults,
                duration=self.duration,
                warmup=self.warmup,
                verbose=debug,
            )

        except (subprocess.SubprocessError, ParseError) as e:
            self._docker_down()
            raise BenchError("Failed to run benchmark", e)
