import json
import os
from fabric import task, Connection

from benchmark.docker_bench import DockerBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError
from benchmark.cloudlab_bench import CloudLabBench, CloudLabInstaller


def _is_local_orch():
    return os.environ.get('LOCAL_ORCH', '0') == '1'

@task
def docker(ctx, debug=False, worker_bw='600mbit', latency='100ms', jitter='0ms',
           cpus_per_validator=8, primary_bw='300mbit', log_dir=''):
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
    zipf_exponent = float(os.environ.get('ZIPF_EXPONENT', 0.0)) # not used anymore, please use E_SKEW_WEIGHTS
    in_memory_store = os.environ.get('IN_MEMORY_STORE', '0') == '1'
    use_writeback_executor = os.environ.get('WRITEBACK_EXECUTOR', 'false').lower() in ('1', 'true', 'yes')
    new_scheduler = os.environ.get('NEW_SCHEDULER', '0') == '1'
    numa_node_cpus = int(os.environ.get('NUMA_NODE_CPUS', 32))
    num_numa_nodes = int(os.environ.get('NUM_NUMA_NODES', 8)) # 8 for hilbit1; 4 for hilbit2
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
            new_scheduler=new_scheduler,
            numa_node_cpus=numa_node_cpus,
            num_numa_nodes=num_numa_nodes,
        ).run(debug, log_dir=log_dir if log_dir else None)
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
def cloudlab(ctx, debug=False,
             manifest='manifest.xml', username='zpkhor', latency='100ms', worker_bw='600mbit',
             primary_bw='300mbit', log_dir='logs-cloudlab'):
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
    assert num_executors <= 3, (
        f'CloudLab executor machines have nproc=20; NUM_EXECUTORS={num_executors} > 3 '
        f'would leave fewer than 6 CPUs per executor'
    )
    no_send_payment = os.environ.get('NO_SEND_PAYMENT', '0') == '1'
    zipf_exponent = float(os.environ.get('ZIPF_EXPONENT', 0.0)) # not used anymore, please use E_SKEW_WEIGHTS
    new_scheduler = os.environ.get('NEW_SCHEDULER', '0') == '1'
    use_writeback_executor = os.environ.get('WRITEBACK_EXECUTOR', 'false').lower() in ('1', 'true', 'yes')
    node_offset = int(os.environ.get('NODE_OFFSET', 0))
    pin_cpus = os.environ.get('PIN_CPUS', '1') == '1'
    worker_bws_kbps = [int(v) * 1000 for v in worker_bws_raw.split(',')] if worker_bws_raw else [int(worker_bw[:-4]) * 1000] * bench_params['nodes']
    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 600,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200,
        'use_writeback_executor': use_writeback_executor,
    }
    local_orch = _is_local_orch()
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
            local_orch=local_orch,
            node_offset=node_offset,
            pin_cpus=pin_cpus,
        ).run(debug, log_dir=log_dir if log_dir else None)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def cloudlab_install(ctx, manifest='manifest.xml', username='zpkhor'):
    ''' Install Rust and dependencies on all CloudLab machines '''
    try:
        CloudLabInstaller(manifest, username, local_orch=_is_local_orch()).install()
    except BenchError as e:
        Print.error(e)


@task
def cloudlab_check(ctx, manifest='manifest.xml', username='zpkhor', timeout=10):
    ''' Probe all CloudLab nodes via SSH, then ping LAN IPs from client.
        Writes unreachable nodes to cloudlab_ban.txt. '''
    from benchmark.instance import CloudLabInstanceManager
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import datetime

    local_orch = _is_local_orch()
    timeout = int(timeout)
    try:
        ssh_config_path = os.path.expanduser('~/.ssh/config')
        cloudlab_entry = 'Host *.cloudlab.us\n    StrictHostKeyChecking no\n    UserKnownHostsFile /dev/null\n'
        existing = open(ssh_config_path).read() if os.path.exists(ssh_config_path) else ''
        if '*.cloudlab.us' not in existing:
            with open(ssh_config_path, 'a') as f:
                if existing and not existing.endswith('\n'):
                    f.write('\n')
                f.write(cloudlab_entry)
            os.chmod(ssh_config_path, 0o600)
            Print.info(f'Added *.cloudlab.us StrictHostKeyChecking=no to {ssh_config_path}')

        mgr = CloudLabInstanceManager.make(manifest, username, ban_file=None, local_orch=local_orch)

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
def cloudlab_kill(ctx, manifest='manifest.xml', username='zpkhor'):
    ''' Kill all processes on CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    from concurrent.futures import ThreadPoolExecutor
    try:
        mgr = CloudLabInstanceManager.make(manifest, username, local_orch=_is_local_orch())
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
def cloudlab_get_ssh(ctx, manifest='manifest.xml', username='zpkhor'):
    ''' Print SSH connection information for all CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    try:
        mgr = CloudLabInstanceManager.make(manifest, username, local_orch=_is_local_orch())
        for client in mgr.manifest.clients:
            Print.info(f"Client: {username}@{client['ssh_host']} ({client['ip']})")
        for validator in mgr.manifest.validators:
            Print.info(f"Validator: {username}@{validator['ssh_host']} ({validator['ip']})")
    except Exception as e:
        Print.error(BenchError('Failed to get SSH information', e))


@task
def cloudlab_nettest(ctx, manifest='manifest.xml', username='zpkhor'):
    ''' Test bandwidth (iperf3) on CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    from concurrent.futures import ThreadPoolExecutor
    import json as _json
    import time
    try:
        mgr = CloudLabInstanceManager.make(manifest, username, local_orch=_is_local_orch())
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
def cloudlab_pingtest(ctx, manifest='manifest.xml', username='zpkhor'):
    ''' Test pairwise latency (ping RTT) on CloudLab machines '''
    from benchmark.instance import CloudLabInstanceManager
    from concurrent.futures import ThreadPoolExecutor
    try:
        mgr = CloudLabInstanceManager.make(manifest, username, local_orch=_is_local_orch())
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