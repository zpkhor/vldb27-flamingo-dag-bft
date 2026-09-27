import os
import re
import xml.etree.ElementTree as ET


class ManifestError(Exception):
    pass


class Manifest:
    """Parses a CloudLab RSpec manifest XML to extract validator and client
    node information including LAN IPs and SSH hostnames."""

    NS = '{http://www.geni.net/resources/rspec/3}'

    def __init__(self, validators, clients):
        assert len(validators) > 0, 'No validator nodes found'
        self.validators = validators
        self.clients = clients

    @classmethod
    def load(cls, filename, username):
        """Parse manifest XML and return a Manifest.

        Each validator/client entry is a dict with keys:
            client_id, ssh_host, ip
        Validators are 'node-X' in the manifest; clients are 'client-X'.
        Nodes are sorted by their numeric index (node-0, node-1, ...).
        """
        assert isinstance(filename, str)
        assert isinstance(username, str)

        try:
            tree = ET.parse(filename)
        except FileNotFoundError:
            raise ManifestError(f'Manifest file not found: {filename}')
        except ET.ParseError as e:
            raise ManifestError(f'Failed to parse manifest XML: {e}')

        root = tree.getroot()
        ns = cls.NS
        validators = []
        clients = []

        for node in root.iter(f'{ns}node'):
            client_id = node.attrib.get('client_id', '')
            if not client_id:
                continue

            # Extract SSH hostname for the given username
            ssh_host = None
            for login in node.iter(f'{ns}login'):
                if login.attrib.get('username') == username:
                    ssh_host = login.attrib['hostname']
                    break
            assert ssh_host is not None, (
                f'No SSH login found for username {username!r} on node {client_id}'
            )

            # Extract LAN IP from the single interface
            lan_ip = None
            for iface in node.iter(f'{ns}interface'):
                ip_elem = iface.find(f'{ns}ip')
                if ip_elem is None:
                    continue
                ip_addr = ip_elem.attrib.get('address')
                if ip_addr:
                    lan_ip = ip_addr
                    break
            assert lan_ip is not None, f'No interface IP found for node {client_id}'

            entry = {
                'client_id': client_id,
                'ssh_host': ssh_host,
                'ip': lan_ip,
            }

            if client_id.startswith('node-'):
                validators.append(entry)
            elif client_id.startswith('client-'):
                clients.append(entry)

        assert len(validators) > 0, 'No validator nodes found in manifest'

        # Sort by numeric index
        def _index(entry):
            m = re.search(r'-(\d+)$', entry['client_id'])
            assert m, f'Cannot parse index from {entry["client_id"]}'
            return int(m.group(1))

        validators.sort(key=_index)
        clients.sort(key=_index)

        return cls(validators, clients)

    @classmethod
    def load_ssh_only(cls, filename, username):
        """Parse manifest XML and return SSH hostnames for all nodes.
        Does not require interface IPs or any naming convention."""
        assert isinstance(filename, str)
        assert isinstance(username, str)
        try:
            tree = ET.parse(filename)
        except FileNotFoundError:
            raise ManifestError(f'Manifest file not found: {filename}')
        except ET.ParseError as e:
            raise ManifestError(f'Failed to parse manifest XML: {e}')

        root = tree.getroot()
        ns = cls.NS
        hosts = []
        for node in root.iter(f'{ns}node'):
            client_id = node.attrib.get('client_id', '')
            if not client_id:
                continue
            ssh_host = None
            for login in node.iter(f'{ns}login'):
                if login.attrib.get('username') == username:
                    ssh_host = login.attrib['hostname']
                    break
            assert ssh_host is not None, (
                f'No SSH login found for username {username!r} on node {client_id}'
            )
            hosts.append(ssh_host)
        assert hosts, 'No nodes found in manifest'
        return hosts

    @staticmethod
    def load_ban_list(ban_file):
        """Load banned client_ids from a ban file.
        Returns empty set if file doesn't exist.
        Lines starting with '#' and empty lines are ignored.
        """
        assert isinstance(ban_file, str)
        if not os.path.exists(ban_file):
            return set()
        banned = set()
        with open(ban_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    banned.add(line)
        return banned
