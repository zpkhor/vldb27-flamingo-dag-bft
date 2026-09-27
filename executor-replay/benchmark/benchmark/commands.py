# Copyright(C) Facebook, Inc. and its affiliates.
from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -rf .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -rf {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_keys --filename {filename}'

    @staticmethod
    def run_primary(keys, committee, store, parameters, debug=False, in_memory_store=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        env = 'IN_MEMORY_STORE=1 ' if in_memory_store else ''
        return (f'{env}./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} primary')

    @staticmethod
    def run_worker(keys, committee, store, parameters, id, debug=False, in_memory_store=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        env = 'IN_MEMORY_STORE=1 ' if in_memory_store else ''
        return (f'{env}./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} worker --id {id}')

    @staticmethod
    def run_client(addresses, size, rate, nodes, account_start=None, num_accounts=None, client_id=None, reply_port=None, num_validators=None):
        # TODO: reply_port and num_validators are for closed-loop client (reply requires) and round-robin
        assert isinstance(addresses, list) and all(isinstance(x, str) for x in addresses)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        account_args = ''
        if account_start is not None and num_accounts is not None:
            account_args = f'--account-start {account_start} --num-accounts {num_accounts}'
        client_id_arg = f'--client-id {client_id}' if client_id is not None else ''
        return f'./benchmark_client {" ".join(addresses)} --size {size} --rate {rate} {nodes} {account_args} {client_id_arg}'

    @staticmethod
    def run_executor(keys, committee, store, parameters, executor_id, validator_ranges, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        range_args = ' '.join(f'--validator-range {start}:{count}' for start, count in validator_ranges)
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} executor --id {executor_id} '
                f'{range_args}')

    @staticmethod
    def remote_cleanup():
        """Cleanup for remote machines (home dir). Only removes narwhal-specific
        files, not all .*.json (which would nuke ~/.claude.json etc.)."""
        return (
            'rm -rf .db-* ; rm -f .node-*.json .committee.json .parameters.json'
        )

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'
