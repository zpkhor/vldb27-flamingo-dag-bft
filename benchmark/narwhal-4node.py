#!/usr/bin/env python
"""xl170 Narwhal WAN topology: single shared LAN, one client per validator.

N validators on one broadcast domain. Each validator also runs its own
benchmark_client process (multi-client mode). Validator interfaces are
shaped (25ms per interface -> 100ms RTT between validators).

Latency breakdown:
  node-i <-> node-j RTT: 25ms(i) + 25ms(i) + 25ms(j) + 25ms(j) = 100ms
  client on node-i -> own node-i: localhost (no LAN traversal)
"""

import geni.portal as portal
import geni.rspec.pg as RSpec

pc = portal.Context()
pc.defineParameter(
    "num_validators",
    "Number of validator nodes",
    portal.ParameterType.INTEGER,
    4,
    longDescription="Total number of BFT validator nodes. Each validator runs its own client."
)
params = pc.bindParameters()

NUM_VALIDATORS = params.num_validators
PRIMARY_BW = 250000   # Kbps, consensus traffic (same for all validators)
WORKER_BW = 500000    # Kbps, batch traffic (same for all validators)
NODE_BW = PRIMARY_BW + WORKER_BW  # total per validator
NODE_LAT = 25     # 25ms per interface -> 50ms one-way -> 100ms RTT between nodes
DISK_IMAGE = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"
APT_PACKAGES = "sudo apt update && sudo apt install python3-pip iperf3 -y"
HARDWARE_TYPE = "xl170"

rspec = RSpec.Request()

lan = RSpec.LAN("wan-lan")
rspec.addResource(lan)

for i in range(NUM_VALIDATORS):
    node = RSpec.RawPC("node-%d" % i)
    node.disk_image = DISK_IMAGE
    node.hardware_type = HARDWARE_TYPE
    rspec.addResource(node)
    node.addService(RSpec.Execute(shell="sh", command=APT_PACKAGES))

    iface = node.addInterface("if-lan")
    iface.bandwidth = NODE_BW
    iface.latency = NODE_LAT
    lan.addInterface(iface)

pc.printRequestRSpec(rspec)
