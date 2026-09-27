import json
import os
from fabric import task, Connection

from benchmark.docker_bench import DockerBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError
from benchmark.cloudlab_bench import CloudLabBench, CloudLabInstaller, CloudLabReplayBench

@task
def docker(ctx, debug=False, worker_bw='75mbit', latency='0ms', jitter='0ms',
           cpus_per_validator=0, lan_bandwidth='100gbit', primary_bw='25mbit'):
    ''' Run benchmarks in Docker containers with tc bandwidth shaping.
        latency: target RTT between validators (e.g. '100ms'), not one-way delay.
    '''
    nodes = int(os.environ.get('NODES', 4))
    rate = int(os.environ.get('RATE', 50_000))
    duration = int(os.environ.get('DURATION', 20))
    warmup = int(os.environ.get('WARMUP', 0))
    rate_weights_raw = os.environ.get('RATE_WEIGHTS')
    rate_weights = [float(w) for w in rate_weights_raw.split(',')] if rate_weights_raw else None
    e_skew_weights_raw = os.environ.get('E_SKEW_WEIGHTS')
    e_skew_weights = [float(w) for w in e_skew_weights_raw.split(',')] if e_skew_weights_raw else None
    account_weights_raw = os.environ.get('ACCOUNT_WEIGHTS')
    account_weights = [int(w) for w in account_weights_raw.split(',')] if account_weights_raw else None
    worker_bws_raw = os.environ.get('WORKER_BANDWIDTHS_MBPS')
    worker_bws = [f"{int(v)}mbit" for v in worker_bws_raw.split(',')] if worker_bws_raw else [worker_bw] * nodes
    num_accounts = int(os.environ.get('NUM_ACCOUNTS', 1_000_000))
    routing_mode = os.environ.get('ROUTING_MODE', '').lower()
    baseline = os.environ.get('BASELINE', '0') == '1'
    round_robin = routing_mode == 'round-robin'
    if round_robin or routing_mode == 'baseline':
        baseline = True
    check_mismatch = os.environ.get('CHECK_MISMATCH', '0') == '1'
    tc_netem_limit = int(os.environ.get('TC_NETEM_LIMIT', 0))
    tc_netem_limit_client = int(os.environ.get('TC_NETEM_LIMIT_CLIENT', 0))
    num_executors = int(os.environ.get('NUM_EXECUTORS', 0))
    no_send_payment = os.environ.get('NO_SEND_PAYMENT', '0') == '1'
    zipf_exponent = float(os.environ.get('ZIPF_EXPONENT', 0.0))
    in_memory_store = os.environ.get('IN_MEMORY_STORE', '0') == '1'
    use_writeback_executor = os.environ.get('WRITEBACK_EXECUTOR', 'false').lower() in ('1', 'true', 'yes')
    bench_params = {
        'faults': 0,
        'nodes': nodes,
        'workers': 1,
        'rate': rate,
        'tx_size': 512,
        'duration': duration,
        'rate_weights': rate_weights,
        'e_skew_weights': e_skew_weights,
        'account_weights': account_weights,
        'warmup': warmup,
        'num_accounts': num_accounts,
    }
    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 50,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200,
        'use_writeback_executor': use_writeback_executor,
    }
    try:
        ret = DockerBench(
            bench_params, node_params,
            worker_bws,
            latency=latency, jitter=jitter,
            cpus_per_validator=int(cpus_per_validator),
            lan_bandwidth=lan_bandwidth,
            check_mismatch=check_mismatch,
            primary_bw=primary_bw,
            baseline=baseline,
            tc_netem_limit=tc_netem_limit,
            tc_netem_limit_client=tc_netem_limit_client,
            round_robin=round_robin,
            num_executors=num_executors,
            no_send_payment=no_send_payment,
            zipf_exponent=zipf_exponent,
            in_memory_store=in_memory_store,
        ).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def docker_down(ctx):
    ''' Tear down Docker containers from a previous docker benchmark run '''
    DockerBench._docker_down()


@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=False):
    ''' Run benchmarks on AWS '''
    bench_params = {
        'faults': 3,
        'nodes': [10],
        'workers': 1,
        'collocate': True,
        'rate': [10_000, 110_000],
        'tx_size': 512,
        'duration': 300,
        'runs': 2,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200  # ms
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [10, 20, 50],
        'workers': [1],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [3_500, 4_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        params_file = os.path.join('logs', 'bench-params.json')
        if os.path.exists(params_file):
            with open(params_file) as f:
                p = json.load(f)
            duration, warmup, faults = p['duration'], p['warmup'], p['faults']
        else:
            duration, warmup, faults = 60, 5, '?'
        print(LogParser.process('./logs', faults=faults, duration=duration, warmup=warmup, verbose=True).result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))


@task
def replay_logs(ctx):
    ''' Print replay benchmark results from existing logs '''
    from benchmark.replay_logs import ReplayLogParser, ReplayParseError
    tx_size = int(os.environ.get('REPLAY_TX_SIZE', 512))
    try:
        result = ReplayLogParser.process('./logs', tx_size=tx_size)
        print(result.result())
    except ReplayParseError as e:
        Print.error(BenchError('Failed to parse replay logs', e))


@task
def cloudlab(ctx, debug=False,
             manifest='manifest.xml', username='anonuser', latency='100ms', worker_bw='75mbit',
             primary_bw='25mbit', log_dir=''):
    ''' Run benchmarks on CloudLab physical machines '''
    nodes = int(os.environ.get('NODES', 4))
    rate = int(os.environ.get('RATE', 8300))
    duration = int(os.environ.get('DURATION', 50))
    warmup = int(os.environ.get('WARMUP', 5))
    rate_weights_raw = os.environ.get('RATE_WEIGHTS')
    rate_weights = [float(w) for w in rate_weights_raw.split(',')] if rate_weights_raw else None
    e_skew_weights_raw = os.environ.get('E_SKEW_WEIGHTS')
    e_skew_weights = [float(w) for w in e_skew_weights_raw.split(',')] if e_skew_weights_raw else None
    account_weights_raw = os.environ.get('ACCOUNT_WEIGHTS')
    account_weights = [int(w) for w in account_weights_raw.split(',')] if account_weights_raw else None
    num_accounts = int(os.environ.get('NUM_ACCOUNTS', 1_000_000))
    assert latency.endswith('ms'), f"latency must be in ms format (e.g. '100ms'), got: {latency!r}"
    latency_ms = int(latency[:-2])
    assert primary_bw.endswith('mbit'), f"primary_bw must be in mbit format (e.g. '25mbit'), got: {primary_bw!r}"
    primary_bw_kbps = int(primary_bw[:-4]) * 1000
    assert worker_bw.endswith('mbit'), f"worker_bw must be in mbit format (e.g. '75mbit'), got: {worker_bw!r}"
    worker_bws_raw = os.environ.get('WORKER_BANDWIDTHS_MBPS')
    routing_mode = os.environ.get('ROUTING_MODE', '').lower()
    baseline = os.environ.get('BASELINE', '0') == '1'
    round_robin = routing_mode == 'round-robin'
    if round_robin or routing_mode == 'baseline':
        baseline = True
    in_memory_store = os.environ.get('IN_MEMORY_STORE', '0') == '1'
    bench_params = {
        'faults': 0,
        'nodes': nodes,
        'workers': int(os.environ.get('WORKER', 1)),
        'rate': rate,
        'tx_size': 512,
        'duration': duration,
        'rate_weights': rate_weights,
        'e_skew_weights': e_skew_weights,
        'account_weights': account_weights,
        'warmup': warmup,
        'num_accounts': num_accounts,
    }
    num_executors = int(os.environ.get('NUM_EXECUTORS', 0))
    no_send_payment = os.environ.get('NO_SEND_PAYMENT', '0') == '1'
    zipf_exponent = float(os.environ.get('ZIPF_EXPONENT', 0.0))
    new_scheduler = os.environ.get('NEW_SCHEDULER', '0') == '1'
    use_writeback_executor = os.environ.get('WRITEBACK_EXECUTOR', 'false').lower() in ('1', 'true', 'yes')
    worker_bws_kbps = [int(v) * 1000 for v in worker_bws_raw.split(',')] if worker_bws_raw else [int(worker_bw[:-4]) * 1000] * bench_params['nodes']
    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 50,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200,
        'use_writeback_executor': use_writeback_executor,
    }
    try:
        ret = CloudLabBench(
            bench_params, node_params,
            manifest_file=manifest,
            username=username,
            baseline=baseline,
            round_robin=round_robin,
            latency_ms=latency_ms,
            primary_bw_kbps=primary_bw_kbps,
            worker_bws_kbps=worker_bws_kbps,
            in_memory_store=in_memory_store,
            num_executors=num_executors,
            no_send_payment=no_send_payment,
            zipf_exponent=zipf_exponent,
            new_scheduler=new_scheduler,
        ).run(debug, log_dir=log_dir if log_dir else None)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def replay(ctx, debug=False):
    ''' Run replay benchmark: single validator replaying batches from CSV '''
    import subprocess
    from time import sleep
    from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters
    from benchmark.commands import CommandMaker
    from benchmark.utils import PathMaker

    replay_csv = os.path.abspath(os.environ.get('REPLAY_CSV', 'benchmark/record_rate25k.csv'))
    replay_tx_size = int(os.environ.get('REPLAY_TX_SIZE', 512))
    num_workers = int(os.environ.get('WORKERS', 1))
    num_executors = int(os.environ.get('NUM_EXECUTORS', 1))
    _skew_str = os.environ.get('EXECUTOR_SKEW_WEIGHTS', '')
    executor_skew_weights = [float(w) for w in _skew_str.split(',') if w.strip()] if _skew_str.strip() else []
    distributed_tx_rate = float(os.environ.get('DISTRIBUTED_TX_RATE', 0.0))
    num_accounts = int(os.environ.get('NUM_ACCOUNTS', 1_000_000))
    duration = int(os.environ.get('DURATION', 120))
    in_memory_store = os.environ.get('IN_MEMORY_STORE', '1') == '1'
    use_writeback_executor = os.environ.get('WRITEBACK_EXECUTOR', 'false').lower() in ('1', 'true', 'yes')
    port = int(os.environ.get('PORT', 15000))

    assert os.path.exists(replay_csv), f"Replay CSV not found: {replay_csv}"
    processes = []

    bench_params = {
        'faults': 0,
        'nodes': 1,
        'workers': num_workers,
        'rate': 1,  # unused in replay mode
        'tx_size': replay_tx_size,
        'duration': duration,
        'num_accounts': num_accounts,
    }
    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 50,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200,
        'use_writeback_executor': use_writeback_executor,
    }

    try:
        Print.info('Setting up replay benchmark...')
        BenchParameters(bench_params)  # validate
        node_parameters = NodeParameters(node_params)
        node_parameters.set_replay_params(replay_csv, replay_tx_size, num_workers, executor_skew_weights, distributed_tx_rate)
        node_parameters.set_executor_params(
            num_executors=num_executors,
            num_accounts=num_accounts,
            min_balance=10_000,
            max_balance=100_000,
            sharding_strategy='range',
            no_send_payment_tx=False,
            use_new_scheduler=False,
        )

        # Kill leftover processes from previous runs
        subprocess.run('pkill -f "./node.*run"', shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        sleep(1)

        # Clean up
        cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Compile
        Print.info('Compiling...')
        cmd = CommandMaker.compile().split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        # Create alias for binaries
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

        # Generate key
        key_file = PathMaker.key_file(0)
        cmd = CommandMaker.generate_key(key_file).split()
        subprocess.run(cmd, check=True)
        key = Key.from_file(key_file)

        # Committee (single validator, all local)
        committee = LocalCommittee([key.name], port, num_workers, num_executors)
        committee.set_account_ranges({key.name: (0, num_accounts)})
        committee.print(PathMaker.committee_file())
        node_parameters.print(PathMaker.parameters_file())

        # Create db directories
        subprocess.run('mkdir -p .db-0', shell=True)
        for w in range(num_workers):
            subprocess.run(f'mkdir -p .db-0-{w}', shell=True)

        v = '-vvv' if debug else '-vv'
        ims = 'IN_MEMORY_STORE=1 ' if in_memory_store else ''

        # Start executors first (workers connect to them)
        for e in range(num_executors):
            cmd = f'{ims}./node {v} run --keys {key_file} --committee {PathMaker.committee_file()} --store .db-0 --parameters {PathMaker.parameters_file()} executor --id {e} 2> {PathMaker.executor_log_file(0, e)}'
            Print.info(f'Starting executor {e}: {cmd}')
            p = subprocess.Popen(cmd, shell=True)
            processes.append(p)

        sleep(1)

        # Start primary before workers (workers send ReplayBatchReady to primary,
        # SimpleSender drops messages on connection failure without retry)
        cmd = f'{ims}./node {v} run --keys {key_file} --committee {PathMaker.committee_file()} --store .db-0 --parameters {PathMaker.parameters_file()} primary 2> {PathMaker.primary_log_file(0)}'
        Print.info(f'Starting primary: {cmd}')
        p = subprocess.Popen(cmd, shell=True)
        processes.append(p)

        # Wait for primary's worker_to_primary port before starting workers
        import socket
        w2p_port = int(committee.json['authorities'][key.name]['primary']['worker_to_primary'].split(':')[1])
        Print.info(f'Waiting for primary port {w2p_port}...')
        for _ in range(60):
            try:
                with socket.create_connection(('127.0.0.1', w2p_port), timeout=0.3):
                    break
            except OSError:
                sleep(0.5)
        else:
            assert False, f'Primary port {w2p_port} not ready after 30s'
        Print.info('Primary ready.')

        # Start workers (they generate batches and notify primary)
        for w in range(num_workers):
            cmd = f'{ims}./node {v} run --keys {key_file} --committee {PathMaker.committee_file()} --store .db-0-{w} --parameters {PathMaker.parameters_file()} worker --id {w} 2> {PathMaker.worker_log_file(0, w)}'
            Print.info(f'Starting worker {w}: {cmd}')
            p = subprocess.Popen(cmd, shell=True)
            processes.append(p)

        Print.info(f'Running replay ({duration} sec)...')
        sleep(duration)

        Print.info('Stopping processes...')
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait(timeout=5)

        # Parse and print results
        Print.info('Parsing logs...')
        from benchmark.replay_logs import ReplayLogParser, ReplayParseError
        try:
            result = ReplayLogParser.process(PathMaker.logs_path(), tx_size=replay_tx_size)
            print(result.result())
        except ReplayParseError as e:
            Print.warn(f'Failed to parse replay logs: {e}')

        Print.heading(f'Logs in {os.path.abspath(PathMaker.logs_path())}/')

    except subprocess.SubprocessError as e:
        Print.error(BenchError('Failed to run replay benchmark', e))
    except Exception as e:
        # Kill any remaining processes
        for p in processes:
            try:
                p.terminate()
            except Exception:
                pass
        if isinstance(e, BenchError):
            Print.error(e)
        else:
            Print.error(BenchError('Replay benchmark failed', e))


@task
def cloudlab_replay(ctx, debug=False, manifest='manifest.xml', username='anonuser'):
    ''' Run replay benchmark on CloudLab: primary + each worker + each executor on its own node.
        TC shaping: LAN bandwidth cap on worker->executor and executor->executor traffic.

        Env vars:
          REPLAY_CSV         path to recorded batch CSV (default: benchmark/record_rate25k.csv)
          REPLAY_TX_SIZE     transaction size in bytes (default: 512)
          NUM_WORKERS        number of worker nodes (default: 1)
          NUM_EXECUTORS      number of executor nodes (default: 1)
          NUM_ACCOUNTS       total SmallBank accounts (default: 1_000_000)
          DURATION           benchmark duration in seconds (default: 120)
          EXECUTOR_BW_MBPS   LAN bandwidth cap for executor traffic in Mbit/s (default: 10000)
          IN_MEMORY_STORE       use in-memory store, 1=yes (default: 1)
          WRITEBACK_EXECUTOR    use writeback executor path, 1=yes (default: 0)
          NODE_OFFSET           skip first N nodes in the manifest (default: 0)
          EXECUTOR_SKEW_WEIGHTS comma-separated weights per executor shard, e.g. "3,1,1" (default: uniform)
          DISTRIBUTED_TX_RATE   fraction of txs that are cross-executor SendPayment, 0.0..1.0 (default: 0.0)
          NO_SEND_PAYMENT       disable SendPayment txs in executor, 1=yes (default: 0)
          NEW_SCHEDULER         use new dynamic load-aware scheduler, 1=yes (default: 0)
    '''
    replay_csv = os.path.abspath(os.environ.get('REPLAY_CSV', 'benchmark/record_rate25k.csv'))
    replay_tx_size = int(os.environ.get('REPLAY_TX_SIZE', 512))
    num_workers = int(os.environ.get('NUM_WORKERS', 1))
    num_executors = int(os.environ.get('NUM_EXECUTORS', 1))
    num_accounts = int(os.environ.get('NUM_ACCOUNTS', 1_000_000))
    duration = int(os.environ.get('DURATION', 120))
    executor_bw_kbps = int(os.environ.get('EXECUTOR_BW_MBPS', 10_000)) * 1000
    in_memory_store = os.environ.get('IN_MEMORY_STORE', '1') == '1'
    use_writeback_executor = os.environ.get('WRITEBACK_EXECUTOR', '0') == '1'
    node_offset = int(os.environ.get('NODE_OFFSET', 0))
    _skew_str = os.environ.get('EXECUTOR_SKEW_WEIGHTS', '')
    executor_skew_weights = [float(w) for w in _skew_str.split(',') if w.strip()] if _skew_str.strip() else []
    distributed_tx_rate = float(os.environ.get('DISTRIBUTED_TX_RATE', 0.0))
    no_send_payment = os.environ.get('NO_SEND_PAYMENT', '0') == '1'
    use_new_scheduler = os.environ.get('NEW_SCHEDULER', '0') == '1'

    assert os.path.exists(replay_csv), f'Replay CSV not found: {replay_csv}'

    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 50,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200,
        'use_writeback_executor': use_writeback_executor,
    }
    try:
        ret = CloudLabReplayBench(
            manifest_file=manifest,
            username=username,
            replay_csv=replay_csv,
            replay_tx_size=replay_tx_size,
            num_workers=num_workers,
            num_executors=num_executors,
            num_accounts=num_accounts,
            duration=duration,
            in_memory_store=in_memory_store,
            use_writeback_executor=use_writeback_executor,
            node_parameters_dict=node_params,
            executor_bw_kbps=executor_bw_kbps,
            node_offset=node_offset,
            executor_skew_weights=executor_skew_weights,
            distributed_tx_rate=distributed_tx_rate,
            no_send_payment=no_send_payment,
            use_new_scheduler=use_new_scheduler,
        ).run(debug)
        print(ret)
    except BenchError as e:
        Print.error(e)


@task
def cloudlab_install(ctx, manifest='manifest.xml', username='anonuser'):
    ''' Install Rust and dependencies on all CloudLab machines '''
    try:
        CloudLabInstaller(manifest, username).install()
    except BenchError as e:
        Print.error(e)


@task
def cloudlab_check(ctx, manifest='manifest.xml', username='anonuser', timeout=10):
    ''' Probe all CloudLab nodes via SSH, then ping LAN IPs from client.
        Writes unreachable nodes to cloudlab_ban.txt. '''
    from benchmark.instance import CloudLabInstanceManager
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import datetime

    timeout = int(timeout)
    try:
        mgr = CloudLabInstanceManager.make(manifest, username, ban_file=None)

        # --- Stage 1: SSH reachability from this machine ---
        all_entries = [(v, 'validator') for v in mgr.manifest.validators]
        Print.info(f'Stage 1: SSH probe {len(all_entries)} nodes (timeout={timeout}s)...')

        def _ssh_probe(entry):
            client_id = entry['client_id']
            ssh_host = entry['ssh_host']
            ssh_ok = False
            ssh_err = ''
            try:
                result = Connection(ssh_host, user=username).run(
                    'echo ok', hide=True, timeout=timeout,
                )
                ssh_ok = result.stdout.strip() == 'ok'
            except Exception as e:
                ssh_err = str(e)
            return client_id, ssh_host, ssh_ok, ssh_err

        ssh_results = {}
        with ThreadPoolExecutor(max_workers=min(len(all_entries), 32)) as pool:
            futures = {pool.submit(_ssh_probe, e): (e, role) for e, role in all_entries}
            for f in as_completed(futures):
                entry, role = futures[f]
                client_id, ssh_host, ssh_ok, ssh_err = f.result()
                ssh_results[client_id] = (role, ssh_host, entry['ip'], ssh_ok, ssh_err)

        banned = []
        for client_id in sorted(ssh_results):
            role, ssh_host, ip, ssh_ok, ssh_err = ssh_results[client_id]
            status = 'OK' if ssh_ok else 'SSH_FAIL'
            detail = f'  {status:12s} {role:10s} {client_id:12s} {ssh_host:40s} {ip}'
            if not ssh_ok:
                detail += f'  err={ssh_err}'
                banned.append(client_id)
            Print.info(detail)

        # --- Stage 2: Ping LAN IPs from the first reachable validator ---
        reachable_validators = [
            v for v in mgr.manifest.validators
            if ssh_results[v['client_id']][3]  # ssh_ok
        ]
        assert len(reachable_validators) > 0, 'No reachable validator node to run ping from'
        ping_client = reachable_validators[0]
        ping_ssh_host = ping_client['ssh_host']
        Print.info(f'\nStage 2: Ping LAN IPs from {ping_client["client_id"]} ({ping_ssh_host})...')

        # Only ping nodes that passed SSH (no point pinging already-banned nodes)
        ssh_ok_entries = [
            (e, role) for e, role in all_entries
            if e['client_id'] not in banned and e['client_id'] != ping_client['client_id']
        ]

        ping_results = {}  # client_id -> avg_rtt_ms; absent = FAIL
        ip_to_cid = {}
        if ssh_ok_entries:
            ip_to_cid = {e['ip']: e['client_id'] for e, _ in ssh_ok_entries}
            ping_cmds = ' & '.join(
                f'(ping -c 5 -q {ip} 2>/dev/null | awk \'/avg/{{print "{ip}", $4}}\')'
                for ip in ip_to_cid
            )
            result = Connection(ping_ssh_host, user=username).run(
                f'{ping_cmds} & wait', hide=True, timeout=30,
            )
            for line in result.stdout.splitlines():
                parts = line.strip().split()
                if len(parts) == 2:
                    ip, rtt_str = parts
                    if ip in ip_to_cid:
                        try:
                            avg_ms = float(rtt_str.split('/')[1])
                            ping_results[ip_to_cid[ip]] = avg_ms
                        except (ValueError, IndexError):
                            pass

        for client_id in sorted(ip_to_cid.values()):
            role, ssh_host, ip, _, _ = ssh_results[client_id]
            if client_id in ping_results:
                rtt = ping_results[client_id]
                Print.info(f'  {"OK":12s} {role:10s} {client_id:12s} {ip}  rtt={rtt:.1f}ms')
            else:
                Print.info(f'  {"PING_FAIL":12s} {role:10s} {client_id:12s} {ip}')
                banned.append(client_id)

        # --- Write ban file ---
        ban_file = 'cloudlab_ban.txt'
        if banned:
            with open(ban_file, 'w') as f:
                f.write(f'# Generated by: fab cloudlab-check\n')
                f.write(f'# {datetime.datetime.now().isoformat()}\n')
                for cid in sorted(set(banned)):
                    f.write(f'{cid}\n')
            Print.warn(f'Wrote {len(set(banned))} banned nodes to {os.path.abspath(ban_file)}')
        else:
            if os.path.exists(ban_file):
                os.remove(ban_file)
            Print.heading(f'All {len(all_entries)} nodes reachable. No ban file needed.')

    except Exception as e:
        Print.error(BenchError('CloudLab check failed', e))


@task
def cloudlab_kill(ctx, manifest='manifest.xml', username='anonuser'):
    ''' Kill all processes on CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    from concurrent.futures import ThreadPoolExecutor
    try:
        mgr = CloudLabInstanceManager.make(manifest, username)
        hosts = mgr.all_ssh_hosts()
        Print.info(f'Killing processes on {len(hosts)} machines...')
        def _kill_one(host):
            try:
                Connection(host, user=username).run('tmux kill-server || true', hide=True)
            except Exception:
                pass
        with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
            list(pool.map(_kill_one, hosts))
        Print.heading('Done.')
    except Exception as e:
        Print.error(BenchError('Failed to kill processes', e))

@task
def cloudlab_get_ssh(ctx, manifest='manifest.xml', username='anonuser'):
    ''' Print SSH connection information for all CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    try:
        mgr = CloudLabInstanceManager.make(manifest, username)
        clients = mgr.client_ssh_hosts()
        for client in clients:
            Print.info(f"Client: {username}@{client}")
        validators = mgr.validator_ssh_hosts()
        for validator in validators:
            Print.info(f"Validator: {username}@{validator}")
    except Exception as e:
        Print.error(BenchError('Failed to get SSH information', e))


@task
def cloudlab_nettest(ctx, manifest='manifest.xml', username='anonuser'):
    ''' Test bandwidth (iperf3) on CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    from concurrent.futures import ThreadPoolExecutor
    import json as _json
    import time
    try:
        mgr = CloudLabInstanceManager.make(manifest, username)
        v_ssh = mgr.validator_ssh_hosts()
        v_ips = mgr.validator_ips()
        c_ssh = mgr.client_ssh_hosts()
        all_ssh = v_ssh + c_ssh
        all_ips = v_ips + mgr.client_ips()
        n = len(all_ssh)
        labels = [f'node-{i}' for i in range(len(v_ssh))] + [f'client-{i}' for i in range(len(c_ssh))]

        def ssh(host):
            return Connection(host, user=username)

        # Kill any leftover iperf3 servers
        def _kill_iperf(host):
            try:
                ssh(host).run('killall iperf3 2>/dev/null || true', hide=True)
            except Exception:
                pass
        with ThreadPoolExecutor(max_workers=n) as pool:
            list(pool.map(_kill_iperf, all_ssh))

        # === Pairwise egress bandwidth test ===
        # Start one iperf3 server per machine (sequential tests, no port conflicts)
        Print.info('Starting iperf3 servers...')
        for host in all_ssh:
            ssh(host).run('iperf3 -s -p 5201 -D', hide=True)
        time.sleep(1)

        Print.info('Testing pairwise egress bandwidth (iperf3, 5s each)...')
        bw_matrix = [[None] * n for _ in range(n)]
        for i in range(n):
            for j in range(n):
                if i == j:
                    bw_matrix[i][j] = 0.0
                    continue
                result = ssh(all_ssh[i]).run(
                    f'iperf3 -c {all_ips[j]} -p 5201 -t 5 -w 4M -J',
                    hide=True, warn=True,
                )
                try:
                    data = _json.loads(result.stdout)
                    bps = data['end']['sum_sent']['bits_per_second']
                except (KeyError, _json.JSONDecodeError, ValueError):
                    bps = 0
                bw_matrix[i][j] = bps / 1_000_000

        Print.info('')
        Print.info('Pairwise egress BW (Mbps, sender=row, receiver=col):')
        header = '{:<12s}'.format('') + ''.join('{:>12s}'.format(l) for l in labels)
        Print.info(header)
        for i in range(n):
            row = '{:<12s}'.format(labels[i])
            row += ''.join(
                '{:>12s}'.format('-') if i == j else '{:>12.1f}'.format(bw_matrix[i][j])
                for j in range(n)
            )
            Print.info(row)

        # Kill servers before ingress test (need multiple servers per target)
        with ThreadPoolExecutor(max_workers=n) as pool:
            list(pool.map(_kill_iperf, all_ssh))
        time.sleep(1)

        # === Total ingress bandwidth test ===
        # For each target, all other nodes send simultaneously
        Print.info('')
        Print.info('Testing total ingress bandwidth (all senders -> one target, 5s)...')
        for target in range(n):
            senders = [s for s in range(n) if s != target]
            # Start one iperf3 server per sender on the target (different ports)
            for k, _ in enumerate(senders):
                ssh(all_ssh[target]).run(
                    f'iperf3 -s -p {5201 + k} -D', hide=True,
                )
            time.sleep(0.5)
            # All senders blast simultaneously
            with ThreadPoolExecutor(max_workers=len(senders)) as pool:
                def _send(args):
                    k, s = args
                    r = ssh(all_ssh[s]).run(
                        f'iperf3 -c {all_ips[target]} -p {5201 + k} -t 5 -w 4M -J',
                        hide=True, warn=True,
                    )
                    try:
                        data = _json.loads(r.stdout)
                        return data['end']['sum_sent']['bits_per_second']
                    except (KeyError, _json.JSONDecodeError, ValueError):
                        return 0
                results = list(pool.map(_send, enumerate(senders)))
            total_mbps = sum(results) / 1_000_000
            per_sender = ', '.join(
                f'{labels[senders[k]]}={results[k]/1e6:.1f}'
                for k in range(len(senders))
            )
            Print.info(f'  {labels[target]}: {total_mbps:.1f} Mbps total ingress ({per_sender})')
            # Kill servers on target
            _kill_iperf(all_ssh[target])
            time.sleep(0.5)

        Print.heading('Network test complete.')
    except Exception as e:
        # Clean up iperf3
        for host in all_ssh:
            try:
                Connection(host, user=username).run('killall iperf3 2>/dev/null || true', hide=True)
            except Exception:
                pass
        Print.error(BenchError('Network test failed', e))


@task
def cloudlab_pingtest(ctx, manifest='manifest.xml', username='anonuser'):
    ''' Test pairwise latency (ping RTT) on CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    from concurrent.futures import ThreadPoolExecutor
    try:
        mgr = CloudLabInstanceManager.make(manifest, username)
        v_ssh = mgr.validator_ssh_hosts()
        v_ips = mgr.validator_ips()
        c_ssh = mgr.client_ssh_hosts()
        all_ssh = v_ssh + c_ssh
        all_ips = v_ips + mgr.client_ips()
        n = len(all_ssh)
        labels = [f'node-{i}' for i in range(len(v_ssh))] + [f'client-{i}' for i in range(len(c_ssh))]

        def ssh(host):
            return Connection(host, user=username)

        # One SSH session per source node; all target pings run as background jobs
        # to avoid opening n simultaneous SSH connections to the same host.
        Print.info('Testing pairwise latency (ping, 5 probes each)...')
        rtt_matrix = [[None] * n for _ in range(n)]

        def _ping_row(i):
            targets = [(j, all_ips[j]) for j in range(n) if j != i]
            ping_cmds = ' & '.join(
                f'(ping -c 5 -q {ip} 2>/dev/null | awk -v j={j} \'/avg/{{split($4,a,"/"); print j,a[2]}}\')'
                for j, ip in targets
            )
            result = ssh(all_ssh[i]).run(f'{ping_cmds} & wait', hide=True, warn=True)
            row = {i: 0.0}
            for line in result.stdout.splitlines():
                parts = line.strip().split()
                if len(parts) == 2:
                    j, avg_ms = int(parts[0]), float(parts[1])
                    row[j] = avg_ms
            missing = [j for j in range(n) if j not in row]
            if missing:
                Print.warn(f'ping row {i}: no reply from j={missing} (100% loss?)')
            return i, row

        with ThreadPoolExecutor(max_workers=n) as pool:
            for i, row in pool.map(_ping_row, range(n)):
                for j, rtt in row.items():
                    rtt_matrix[i][j] = rtt

        Print.info('')
        Print.info('RTT matrix (ms):')
        header = '{:<12s}'.format('') + ''.join('{:>12s}'.format(l) for l in labels)
        Print.info(header)
        for i in range(n):
            row = '{:<12s}'.format(labels[i])
            row += ''.join(
                '{:>12s}'.format('-') if i == j
                else '{:>12s}'.format('FAIL') if rtt_matrix[i][j] is None
                else '{:>12.1f}'.format(rtt_matrix[i][j])
                for j in range(n)
            )
            Print.info(row)

        Print.heading('Ping test complete.')
    except Exception as e:
        Print.error(BenchError('Ping test failed', e))