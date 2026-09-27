import argparse
from benchmark.docker_bench import egress_bw_capacity_tps, max_capacity_tps_by_ingress


def main():
    parser = argparse.ArgumentParser(description='Calculate node capacity limits based on bandwidth.')
    parser.add_argument('bw_mbit', nargs='+', type=float, metavar='BW_MBIT',
                        help='Bandwidth in mbit/s; one value per node or a single shared value')
    parser.add_argument('--num-peers', type=int, required=True)
    parser.add_argument('--tx-size', type=int, default=512)

    args = parser.parse_args()
    bw = args.bw_mbit if len(args.bw_mbit) > 1 else args.bw_mbit[0]

    n = args.num_peers
    f = (n - 1) // 3
    quorum = 2 * f + 1
    bw_list = bw if isinstance(bw, list) else [bw] * n
    assert len(bw_list) == n, f'Expected {n} bw values, got {len(bw_list)}'
    bottleneck_bw = sorted(bw_list, reverse=True)[quorum - 1]

    ingress = max_capacity_tps_by_ingress(bottleneck_bw, args.tx_size)
    print(f'Whole system tps (capped by ingress): {ingress} tps')

    print(f'Per node tps (capped by egress):')
    for i, b in enumerate(bw_list):
        egress = egress_bw_capacity_tps(b, args.tx_size, args.num_peers)
        print(f'  node {i}: {egress} tps')


if __name__ == '__main__':
    main()
