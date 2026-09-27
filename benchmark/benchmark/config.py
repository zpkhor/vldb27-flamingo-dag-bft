# Copyright(C) Facebook, Inc. and its affiliates.
from json import dump, load
from collections import OrderedDict
import base64


class ConfigError(Exception):
    pass


class Key:
    def __init__(self, name, secret):
        self.name = name
        self.secret = secret

    @classmethod
    def from_file(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)
        return cls(data['name'], data['secret'])


class Committee:
    ''' The committee looks as follows:
        "authorities: {
            "name": {
                "stake": 1,
                "primary: {
                    "primary_to_primary": x.x.x.x:x,
                    "worker_to_primary": x.x.x.x:x,
                },
                "workers": {
                    "0": {
                        "primary_to_worker": x.x.x.x:x,
                        "worker_to_worker": x.x.x.x:x,
                        "transactions": x.x.x.x:x
                    },
                    ...
                },
                "client_reply": x.x.x.x:x
            },
            ...
        }
    '''

    def __init__(self, addresses, base_port, num_executors=0, executor_hosts=None):
        ''' The `addresses` field looks as follows:
            {
                "name": ["host", "host", ...],
                ...
            }
            executor_hosts: optional dict {name: [host]*num_executors}
        '''
        assert isinstance(addresses, OrderedDict)
        assert all(isinstance(x, str) for x in addresses.keys())
        assert all(
            isinstance(x, list) and len(x) > 1 for x in addresses.values()
        )
        assert all(
            isinstance(x, str) for y in addresses.values() for x in y
        )
        assert len({len(x) for x in addresses.values()}) == 1
        assert isinstance(base_port, int) and base_port > 1024

        port = base_port
        self.json = {'authorities': OrderedDict()}
        for name, hosts in addresses.items():
            host = hosts.pop(0)
            primary_addr = {
                'primary_to_primary': f'{host}:{port}',
                'worker_to_primary': f'{host}:{port + 1}',
                'executor_to_primary': f'{host}:{port + 2}',
            }
            port += 3

            workers_addr = OrderedDict()
            for j, host in enumerate(hosts):
                workers_addr[j] = {
                    'primary_to_worker': f'{host}:{port}',
                    'transactions': f'{host}:{port + 1}',
                    'worker_to_worker': f'{host}:{port + 2}',
                }
                if j == 0:
                    client_reply_addr = f'{host}:{port + 3}'
                port += 4

            # Allocate executor addresses
            executors_addr = OrderedDict()
            for e in range(num_executors):
                e_host = host  # default: same host as last worker
                if executor_hosts and name in executor_hosts:
                    e_host = executor_hosts[name][e]
                executors_addr[e] = {
                    'worker_to_executor': f'{e_host}:{port}',
                    'executor_to_executor': f'{e_host}:{port + 1}',
                }
                port += 2

            self.json['authorities'][name] = {
                'stake': 1,
                'primary': primary_addr,
                'workers': workers_addr,
                'executors': executors_addr,
                'client_reply': client_reply_addr,
                'capacity_by_bw': 0,
            }

    def sorted_authority_names(self):
        ''' Authority names in Rust BTreeMap byte order (sorted by decoded bytes, not base64 string). '''
        return sorted(self.json['authorities'].keys(),
                       key=lambda n: base64.b64decode(n))

    def set_latency_matrix(self, matrix):
        ''' matrix is dict: {pk_base64: {pk_base64: latency_ms, ...}, ...} '''
        names = set(self.json['authorities'].keys())
        assert set(matrix.keys()) == names
        for pk in matrix:
            assert set(matrix[pk].keys()) == names
        self.json['latency_matrix'] = matrix

    def set_account_ranges(self, ranges):
        ''' ranges is dict: {pk_base64: [start, count], ...} '''
        names = set(self.json['authorities'].keys())
        assert set(ranges.keys()) == names
        self.json['account_ranges'] = {pk: list(v) for pk, v in ranges.items()}

    def set_client_reply_addresses(self, addresses):
        ''' Set client reply addresses. addresses is dict: {client_id_int: "host:port", ...} '''
        # JSON keys must be strings
        self.json['client_reply_addresses'] = {str(k): v for k, v in addresses.items()}

    def executors_addresses(self, faults=0):
        ''' Returns an ordered list of list of (id, worker_to_executor_addr) per authority. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            authority_addresses = []
            for id, executor in authority.get('executors', {}).items():
                authority_addresses.append((id, executor['worker_to_executor']))
            addresses.append(authority_addresses)
        return addresses

    def set_capacities(self, capacities):
        ''' Set per-validator capacity (requests/sec). capacities is a list aligned
            with the authority insertion order. '''
        assert len(capacities) == len(self.json['authorities'])
        for name, cap in zip(self.json['authorities'], capacities):
            self.json['authorities'][name]['capacity_by_bw'] = cap

    def primary_addresses(self, faults=0):
        ''' Returns an ordered list of primaries' addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            addresses += [authority['primary']['primary_to_primary']]
        return addresses

    def workers_addresses(self, faults=0):
        ''' Returns an ordered list of list of workers' addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            authority_addresses = []
            for id, worker in authority['workers'].items():
                authority_addresses += [(id, worker['transactions'])]
            addresses.append(authority_addresses)
        return addresses

    def ips(self, name=None):
        ''' Returns all the ips associated with an authority (in any order). '''
        if name is None:
            names = list(self.json['authorities'].keys())
        else:
            names = [name]

        ips = set()
        for name in names:
            addresses = self.json['authorities'][name]['primary']
            ips.add(self.ip(addresses['primary_to_primary']))
            ips.add(self.ip(addresses['worker_to_primary']))

            for worker in self.json['authorities'][name]['workers'].values():
                ips.add(self.ip(worker['primary_to_worker']))
                ips.add(self.ip(worker['worker_to_worker']))
                ips.add(self.ip(worker['transactions']))

            ips.add(self.ip(self.json['authorities'][name]['client_reply'])) # TODO: to be used by workers

        return list(ips)

    def remove_nodes(self, nodes):
        ''' remove the `nodes` last nodes from the committee. '''
        assert nodes < self.size()
        for _ in range(nodes):
            self.json['authorities'].popitem()

    def size(self):
        ''' Returns the number of authorities. '''
        return len(self.json['authorities'])

    def workers(self):
        ''' Returns the total number of workers (all authorities altogether). '''
        return sum(len(x['workers']) for x in self.json['authorities'].values())

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

    @staticmethod
    def ip(address):
        assert isinstance(address, str)
        return address.split(':')[0]


class LocalCommittee(Committee):
    def __init__(self, names, port, workers):
        assert isinstance(names, list)
        assert all(isinstance(x, str) for x in names)
        assert isinstance(port, int)
        assert isinstance(workers, int) and workers > 0
        addresses = OrderedDict((x, ['127.0.0.1']*(1+workers)) for x in names)
        super().__init__(addresses, port)


class DockerCommittee(Committee):
    def __init__(self, names, port, workers, container_ips, client_ip,
                 num_executors=0, executor_ips=None):
        assert isinstance(names, list)
        assert all(isinstance(x, str) for x in names)
        assert isinstance(port, int)
        assert isinstance(workers, int) and workers > 0
        assert isinstance(container_ips, list)
        assert len(container_ips) == len(names)
        assert isinstance(client_ip, str)

        # Build executor_hosts mapping: {name: [executor_ip]*num_executors}
        executor_hosts = None
        if num_executors > 0 and executor_ips:
            executor_hosts = {}
            idx = 0
            for name in names:
                executor_hosts[name] = executor_ips[idx:idx + num_executors]
                idx += num_executors

        # Build addresses with container IPs for all hosts (primary + workers)
        addresses = OrderedDict(
            (name, [ip] * (1 + workers))
            for name, ip in zip(names, container_ips)
        )
        super().__init__(addresses, port, num_executors=num_executors,
                         executor_hosts=executor_hosts)

        # All authorities share a single client_reply address
        first_reply_port = list(self.json['authorities'].values())[0]['client_reply'].split(':')[1]

        # Override intra-validator addresses to 127.0.0.1
        for name in names:
            auth = self.json['authorities'][name]
            # worker_to_primary is intra-validator (same container)
            addr = auth['primary']['worker_to_primary']
            auth['primary']['worker_to_primary'] = f'127.0.0.1:{addr.split(":")[1]}'

            # executor_to_primary: executors are in SEPARATE containers,
            # so they must reach the validator via its container IP (NOT 127.0.0.1)

            for worker in auth['workers'].values():
                # primary_to_worker is intra-validator
                addr = worker['primary_to_worker']
                worker['primary_to_worker'] = f'127.0.0.1:{addr.split(":")[1]}'

            # client_reply points to single client container
            auth['client_reply'] = f'{client_ip}:{first_reply_port}'

    def remote_addresses(self, name):
        ''' Returns host:port pairs for all listening ports of other validators
            (excludes 127.0.0.1 addresses and the given validator). '''
        addrs = []
        for auth_name, auth in self.json['authorities'].items():
            if auth_name == name:
                continue
            for addr in [auth['primary']['primary_to_primary'],
                         auth['primary']['worker_to_primary']]:
                if not addr.startswith('127.0.0.1:'):
                    addrs.append(addr)
            for worker in auth['workers'].values():
                for key in ['primary_to_worker', 'worker_to_worker']:
                    addr = worker[key]
                    if not addr.startswith('127.0.0.1:'):
                        addrs.append(addr)
        return addrs


class CloudLabCommittee(Committee):
    """Committee for CloudLab deployments (single shared LAN, co-located clients).

    Address assignment:
        primary_to_primary:   validator IP (inter-validator)
        worker_to_primary:    127.0.0.1 (intra-validator, same machine)
        executor_to_primary:  validator IP (executor on dedicated machine)
        primary_to_worker:    127.0.0.1 (intra-validator, same machine)
        transactions:         validator IP (client connects over LAN)
        worker_to_worker:     validator IP (inter-validator)
        worker_to_executor:   executor IP (dedicated machine)
        executor_to_executor: executor IP (dedicated machine)
        client_reply:         validator IP (co-located client)
    """
    def __init__(self, names, port, workers, validator_ips,
                 num_executors=0, executor_ips=None):
        assert isinstance(names, list)
        assert all(isinstance(x, str) for x in names)
        assert isinstance(port, int)
        assert isinstance(workers, int) and workers > 0
        n = len(names)
        assert len(validator_ips) == n

        # Build executor_hosts mapping: {name: [executor_ip]*num_executors}
        executor_hosts = None
        if num_executors > 0:
            assert executor_ips is not None and len(executor_ips) == n, (
                f'Need {n} executor IPs (one per validator), got {executor_ips}'
            )
            executor_hosts = {
                name: [executor_ips[i]] * num_executors
                for i, name in enumerate(names)
            }

        addresses = OrderedDict(
            (name, [v_ip] * (1 + workers))
            for name, v_ip in zip(names, validator_ips)
        )
        super().__init__(addresses, port, num_executors=num_executors,
                         executor_hosts=executor_hosts)

        # Override intra-validator to 127.0.0.1 (client_reply keeps validator IP from base class)
        for name in names:
            auth = self.json['authorities'][name]

            addr = auth['primary']['worker_to_primary']
            auth['primary']['worker_to_primary'] = f'127.0.0.1:{addr.split(":")[1]}'

            for worker in auth['workers'].values():
                addr = worker['primary_to_worker']
                worker['primary_to_worker'] = f'127.0.0.1:{addr.split(":")[1]}'

    def remote_addresses(self, name):
        """Returns host:port pairs for all listening ports of other validators
        (excludes 127.0.0.1 addresses and the given validator)."""
        addrs = []
        for auth_name, auth in self.json['authorities'].items():
            if auth_name == name:
                continue
            for addr in [auth['primary']['primary_to_primary'],
                         auth['primary']['worker_to_primary']]:
                if not addr.startswith('127.0.0.1:'):
                    addrs.append(addr)
            for worker in auth['workers'].values():
                for key in ['primary_to_worker', 'worker_to_worker']:
                    addr = worker[key]
                    if not addr.startswith('127.0.0.1:'):
                        addrs.append(addr)
        return addrs


class NodeParameters:
    def __init__(self, json):
        inputs = []
        try:
            inputs += [json['header_size']]
            inputs += [json['max_header_delay']]
            inputs += [json['gc_depth']]
            inputs += [json['sync_retry_delay']]
            inputs += [json['sync_retry_nodes']]
            inputs += [json['batch_size']]
            inputs += [json['max_batch_delay']]
        except KeyError as e:
            raise ConfigError(f'Malformed parameters: missing key {e}')

        if not all(isinstance(x, int) for x in inputs):
            raise ConfigError('Invalid parameters type')

        self.json = json

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

    def set_executor_params(self, num_executors, num_accounts, min_balance, max_balance,
                            no_send_payment_tx, use_new_scheduler):
        self.json['num_executors'] = num_executors
        self.json['num_accounts'] = num_accounts
        self.json['min_balance'] = min_balance
        self.json['max_balance'] = max_balance
        self.json['no_send_payment_tx'] = no_send_payment_tx
        self.json['use_new_scheduler'] = use_new_scheduler


class BenchParameters:
    def __init__(self, json):
        try:
            self.faults = int(json['faults'])

            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes or any(x <= 1 for x in nodes):
                raise ConfigError('Missing or invalid number of nodes')
            self.nodes = [int(x) for x in nodes]

            rate = json['rate']
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError('Missing input rate')
            self.rate = [int(x) for x in rate]

            
            self.workers = int(json['workers'])

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])
           
            self.duration = int(json['duration'])

            self.runs = int(json['runs']) if 'runs' in json else 1

            self.rate_weights = json.get('rate_weights', None)
            if self.rate_weights is not None:
                if len(self.rate_weights) != self.nodes[0]:
                    raise ConfigError(
                        f'rate_weights length ({len(self.rate_weights)}) '
                        f'must match nodes ({self.nodes[0]})'
                    )
            self.account_weights = json.get('account_weights', None)
            if self.account_weights is not None:
                if len(self.account_weights) != self.nodes[0]:
                    raise ConfigError(
                        f'account_weights length ({len(self.account_weights)}) '
                        f'must match nodes ({self.nodes[0]})'
                    )
            self.e_skew_weights = json.get('e_skew_weights', None)
            self.warmup = int(json.get('warmup', 0))
            self.num_accounts = int(json.get('num_accounts', 1_000_000))
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if min(self.nodes) <= self.faults:
            raise ConfigError('There should be more nodes than faults')


class PlotParameters:
    def __init__(self, json):
        try:
            faults = json['faults']
            faults = faults if isinstance(faults, list) else [faults]
            self.faults = [int(x) for x in faults] if faults else [0]

            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes:
                raise ConfigError('Missing number of nodes')
            self.nodes = [int(x) for x in nodes]

            workers = json['workers']
            workers = workers if isinstance(workers, list) else [workers]
            if not workers:
                raise ConfigError('Missing number of workers')
            self.workers = [int(x) for x in workers]

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])

            max_lat = json['max_latency']
            max_lat = max_lat if isinstance(max_lat, list) else [max_lat]
            if not max_lat:
                raise ConfigError('Missing max latency')
            self.max_latency = [int(x) for x in max_lat]

        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if len(self.nodes) > 1 and len(self.workers) > 1:
            raise ConfigError(
                'Either the "nodes" or the "workers can be a list (not both)'
            )

    def scalability(self):
        return len(self.workers) > 1
