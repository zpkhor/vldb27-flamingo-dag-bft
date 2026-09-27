#!/usr/bin/env python3
"""Plot TPS/migration timeline from pasted log blocks.

Paste the LB run log into CSV_BLOCK. Optionally paste a baseline (no-LB) run's
tps_timeline.py output into CSV_BLOCK_BASELINE and pass --baseline to overlay
it as a reference line on the same TPS panel.

Replot benchmark/hotspot_shift_repro.pdf from the blocks pasted below (they are
the Aug 15 repro run, so no results dir is needed):

    python benchmark/exp/hotspot.py \
        -o /home/zpkhor/narwhal-validator/benchmark/hotspot_shift_repro.pdf

Or replot it from the run dir, the way run_hotspot_shift.sh does. --run-dir wins
over the pasted blocks, and needs tps_timeline.csv next to output.log:

    RUN_DIR=/mnt/data/results/tps_timeline_lb_cloud_hotspot_20260815_163205/n4_v_rate_imb90_r110000_run_1
    python benchmark/tps_timeline.py $RUN_DIR > $RUN_DIR/tps_timeline.csv
    python benchmark/exp/hotspot.py --run-dir $RUN_DIR \
        -o /home/zpkhor/narwhal-validator/benchmark/hotspot_shift_repro.pdf
"""
import argparse
import csv
from pathlib import Path

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator
from plot_lb_configs import (
    TPS_OUTLIER,
    _smoothed_series,
)

# Local x-axis trim (independent of plot_lb_configs.py — tuned for hotspot timeline).
TRIM_LEFT_DUR  = 20
TRIM_RIGHT_DUR = 520

# Paper palette — teal, coral, blue, orange (order matches plot_paper_configs.py)
COLORS = ["#2A9D8F", "#E76F51", "#1f77b4", "#ff7f0e"]

# Panel geometry. Height per panel in inches; Y_MARGIN is the fraction of the data
# range padded above/below the curve (matplotlib's default is 0.05).
PANEL_HEIGHT_IN = 0.62
Y_MARGIN = 0.03
HSPACE = 0.10


def _apply_sigmod_style():
    plt.rcParams.update({
        "font.family":       "sans-serif",
        "font.size":         8,
        "axes.titlesize":    8,
        "axes.labelsize":    8,
        "xtick.labelsize":   7,
        "ytick.labelsize":   7,
        "legend.fontsize":   7,
        "lines.linewidth":   1.0,
        "axes.linewidth":    0.6,
        "grid.linewidth":    0.4,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "pdf.fonttype":      42,
        "ps.fonttype":       42,
        "grid.linestyle":    "--",
        "grid.color":        "lightgray",
        "grid.alpha":        0.8,
    })

# Hardcoded phase markers for the fig. 3 timeline experiments:
# warmup end at 240s and hotspot shift at 600s.
IMBALANCE_MARKERS_S = (220.0, 580.0)

# /home/zpkhor/narwhal-validator/benchmark/exp/results/tps_timeline_lb_cloud_hotspot_20260815_163205/n4_v_rate_imb90_r110000_run_1
CSV_BLOCK = """
timestamp_s,tps,n_batches,n_txs,lat_mean,lat_p50,lat_p90,lat_p95
1786826106.207,63114.2,646,631142,3012.4,3409.0,3998.0,4043.0
1786826116.207,99947.1,1023,999471,1965.2,1963.0,2326.0,2460.0
1786826126.207,105027.5,1075,1050275,1943.6,1933.0,2280.0,2425.0
1786826136.207,79527.8,814,795278,1864.2,1852.0,2154.0,2202.0
1786826146.207,84412.8,864,844128,1873.6,1867.0,2173.0,2220.0
1786826156.207,84022.0,860,840220,1879.6,1884.0,2188.0,2235.0
1786826166.207,79234.7,811,792347,1862.1,1861.0,2162.0,2216.0
1786826176.207,224319.2,2296,2243192,13721.0,5595.0,37349.0,41449.0
1786826186.207,79332.4,812,793324,1870.8,1861.0,2167.0,2214.0
1786826196.207,143032.8,1464,1430328,3937.2,2062.0,11148.0,13748.0
1786826206.207,112648.1,1153,1126481,1965.1,1961.0,2313.0,2419.0
1786826216.207,105516.0,1080,1055160,1985.5,1977.0,2339.0,2476.0
1786826226.207,112843.5,1155,1128435,1990.0,1990.0,2350.0,2491.0
1786826236.207,112648.1,1153,1126481,1953.0,1956.0,2318.0,2458.0
1786826246.207,105516.0,1080,1055160,1958.0,1967.0,2322.0,2427.0
1786826256.207,112355.0,1150,1123550,1959.8,1951.0,2297.0,2437.0
1786826266.207,112452.7,1151,1124527,1961.8,1956.0,2311.0,2449.0
1786826276.207,105516.0,1080,1055160,1973.8,1970.0,2333.0,2470.0
1786826286.207,112355.0,1150,1123550,1988.2,1987.0,2343.0,2485.0
1786826296.207,112550.4,1152,1125504,1981.6,1993.0,2350.0,2453.0
1786826306.207,105906.8,1084,1059068,1945.5,1930.0,2318.0,2423.0
1786826316.207,112159.6,1148,1121596,1956.5,1943.0,2295.0,2434.0
1786826326.207,105711.4,1082,1057114,1962.7,1953.0,2304.0,2446.0
1786826336.207,74906.0,789,749060,2091.2,1958.0,2466.0,3506.0
1786826346.207,61483.8,638,614838,2797.7,1930.0,7117.0,9572.0
1786826356.207,55208.4,566,552084,3346.0,1939.0,11994.0,14402.0
1786826366.207,74935.9,767,749359,4189.1,1993.0,17827.0,19800.0
1786826376.207,83045.0,850,830450,5356.3,2166.0,23381.0,24814.0
1786826386.207,96332.2,986,963322,4878.3,1970.0,27219.0,29525.0
1786826396.207,96723.0,990,967230,6497.1,1989.0,33184.0,34791.0
1786826406.207,110205.6,1128,1102056,5927.5,1959.0,36808.0,38813.0
1786826416.207,112648.1,1153,1126481,7075.2,1966.0,41769.0,43936.0
1786826426.207,101314.9,1037,1013149,8109.3,2038.0,46597.0,47136.0
1786826436.207,91642.6,938,916426,10104.7,2249.0,49052.0,49702.0
1786826446.207,92717.3,949,927173,11082.9,2262.0,51257.0,51919.0
1786826456.207,106004.5,1085,1060045,10563.2,2223.0,52529.0,52881.0
1786826466.207,91349.5,935,913495,10036.0,2231.0,53041.0,53340.0
1786826476.207,94866.7,971,948667,9375.8,2205.0,52547.0,52953.0
1786826486.207,111475.7,1141,1114757,10420.4,2220.0,50274.0,51368.0
1786826496.207,97797.7,1001,977977,9700.0,2275.0,47484.0,48279.0
1786826506.207,97504.6,998,975046,8151.0,2242.0,43043.0,43892.0
1786826516.207,113136.6,1158,1131366,8211.9,2253.0,37811.0,39905.0
1786826526.207,393242.5,4025,3932425,39824.4,34869.0,86846.0,93094.0
1786826536.207,130820.3,1339,1308203,3902.0,1966.0,15316.0,20710.0
1786826546.207,136096.1,1393,1360961,2516.9,1949.0,2535.0,8483.0
1786826556.207,111671.1,1143,1116711,1927.6,1912.0,2291.0,2402.0
1786826566.207,106102.2,1086,1061022,2001.7,1964.0,2413.0,2606.0
1786826576.207,112452.7,1151,1124527,2359.3,2191.0,3340.0,3832.0
1786826586.207,82165.7,841,821657,2121.3,2112.0,2642.0,2770.0
1786826596.207,116946.9,1197,1169469,4366.8,2416.0,11112.0,12862.0
1786826606.207,131601.9,1347,1316019,3012.1,2048.0,7097.0,8896.0
1786826616.207,105516.0,1080,1055160,1897.0,1897.0,2213.0,2313.0
1786826626.207,112159.6,1148,1121596,1922.3,1917.0,2270.0,2415.0
1786826636.207,112941.2,1156,1129412,1917.2,1894.0,2281.0,2427.0
1786826646.207,105906.8,1084,1059068,1922.0,1904.0,2259.0,2404.0
1786826656.207,112061.9,1147,1120619,1919.1,1913.0,2271.0,2418.0
1786826666.207,111866.5,1145,1118665,1992.0,1946.0,2480.0,2630.0
1786826676.207,105711.4,1082,1057114,1955.5,1955.0,2355.0,2450.0
1786826686.207,96234.5,985,962345,2060.9,2039.0,2542.0,2688.0
1786826696.207,85776.4,911,857764,5258.9,2754.0,12770.0,14564.0
1786826706.207,57838.0,616,578380,3168.8,2028.0,8269.0,9324.0
1786826716.207,63127.1,664,631271,4303.2,2046.0,13902.0,15290.0
1786826726.207,68096.5,698,680965,5091.8,2209.0,19023.0,20375.0
1786826736.207,73763.5,755,737635,6809.3,2232.0,23802.0,25239.0
1786826746.207,75717.5,775,757175,9138.3,1990.0,29993.0,31197.0
1786826756.207,82458.8,844,824588,10376.6,1974.0,35450.0,36506.0
1786826766.207,83533.5,855,835335,11686.6,2022.0,39613.0,40968.0
1786826776.207,91447.2,936,914472,11753.8,2015.0,43323.0,44828.0
1786826786.207,85585.2,876,855852,11698.9,1960.0,48391.0,49745.0
1786826796.207,95062.1,973,950621,13239.9,2010.0,51456.0,52011.0
1786826806.207,100142.5,1025,1001425,13611.8,2026.0,53523.0,54323.0
1786826816.207,94378.2,966,943782,13368.5,1996.0,55826.0,56039.0
1786826826.207,99654.0,1020,996540,12978.7,1991.0,56301.0,56646.0
1786826836.207,101412.6,1038,1014126,12660.0,1979.0,55854.0,56159.0
1786826846.207,95355.2,976,953552,13014.7,1986.0,54678.0,54981.0
1786826856.207,101314.9,1037,1013149,13158.1,1991.0,53336.0,53730.0
1786826866.207,100924.1,1033,1009241,12217.2,1975.0,50998.0,51847.0
1786826876.207,98872.4,1012,988724,12188.2,1993.0,47368.0,48618.0
1786826886.207,107763.1,1103,1077631,10679.4,2004.0,40746.0,42893.0
1786826896.207,109521.7,1121,1095217,10084.9,2018.0,31408.0,34503.0
1786826906.207,104539.0,1070,1045390,8201.8,2018.0,22677.0,24918.0
1786826916.207,569102.5,5825,5691025,81713.9,79711.0,160867.0,171516.0
1786826926.207,111182.6,1138,1111826,1942.6,1939.0,2253.0,2391.0
1786826936.207,104929.8,1074,1049298,1992.1,1998.0,2343.0,2467.0
1786826946.207,113820.5,1165,1138205,1979.5,1983.0,2334.0,2446.0
1786826956.207,111378.0,1140,1113780,1988.4,1992.0,2349.0,2455.0
1786826966.207,105516.0,1080,1055160,1980.4,1967.0,2357.0,2469.0
1786826976.207,112648.1,1153,1126481,1993.4,2000.0,2344.0,2485.0
1786826986.207,111573.4,1142,1115734,1990.0,1982.0,2361.0,2471.0
1786826996.207,106102.2,1086,1061022,2092.1,2067.0,2624.0,2813.0
1786827006.207,112355.0,1150,1123550,2008.8,2013.0,2406.0,2490.0
1786827016.207,106199.9,1087,1061999,2017.9,2021.0,2411.0,2487.0
1786827026.207,112257.3,1149,1122573,2045.3,2034.0,2452.0,2584.0
1786827036.207,113038.9,1157,1130389,2021.6,2039.0,2420.0,2509.0
1786827046.207,105711.4,1082,1057114,2013.0,2030.0,2415.0,2479.0
1786827056.207,111866.5,1145,1118665,2018.9,2038.0,2415.0,2490.0
1786827066.207,106493.0,1090,1064930,2020.8,1953.0,2333.0,2530.0
1786827076.207,89004.7,911,890047,2033.5,1913.0,2238.0,2444.0
1786827086.207,110010.2,1126,1100102,2275.2,1922.0,2398.0,5399.0
1786827096.207,137463.9,1407,1374639,3227.3,1986.0,8247.0,12338.0
1786827106.207,105320.6,1078,1053206,2038.5,2031.0,2477.0,2583.0
1786827116.207,111866.5,1145,1118665,1954.0,1934.0,2277.0,2383.0
1786827126.207,113038.9,1157,1130389,1983.6,1985.0,2336.0,2447.0
1786827136.207,105711.4,1082,1057114,1997.0,1999.0,2347.0,2453.0
1786827146.207,111768.8,1144,1117688,1973.5,1958.0,2306.0,2443.0
1786827156.207,106004.5,1085,1060045,1947.8,1947.0,2257.0,2346.0
1786827166.207,112843.5,1155,1128435,1974.0,1973.0,2318.0,2442.0
1786827176.207,111768.8,1144,1117688,1994.5,1989.0,2369.0,2471.0
1786827186.207,105613.7,1081,1056137,1980.1,1987.0,2341.0,2441.0
1786827196.207,113136.6,1158,1131366,1978.8,1968.0,2338.0,2458.0
1786827206.207,111475.7,1141,1114757,1951.3,1939.0,2269.0,2385.0
1786827216.207,81091.0,830,810910,1915.9,1916.0,2215.0,2270.0
1786827226.207,121831.9,1247,1218319,2477.9,1940.0,2617.0,7809.0
1786827236.207,120659.5,1235,1206595,2726.3,1954.0,5606.0,9089.0
1786827246.207,110694.1,1133,1106941,2749.4,1985.0,5168.0,9165.0
1786827256.207,114699.8,1174,1146998,2001.4,1979.0,2411.0,2521.0
1786827266.207,106688.4,1092,1066884,1974.8,1977.0,2332.0,2436.0
1786827276.207,110791.8,1134,1107918,1997.9,1987.0,2400.0,2516.0
1786827286.207,113038.9,1157,1130389,1988.8,1995.0,2349.0,2456.0
1786827296.207,111671.1,1143,1116711,1977.7,1967.0,2330.0,2464.0
1786827306.207,105125.2,1076,1051252,2027.7,1967.0,2326.0,2555.0
1786827316.207,112355.0,1150,1123550,1956.0,1942.0,2258.0,2353.0
1786827326.207,112061.9,1147,1120619,1952.8,1941.0,2269.0,2398.0
1786827336.207,105711.4,1082,1057114,1934.9,1930.0,2235.0,2332.0
1786827346.207,112745.8,1154,1127458,1940.7,1944.0,2251.0,2346.0
1786827356.207,111866.5,1145,1118665,1947.0,1952.0,2251.0,2316.0
1786827366.207,106004.5,1085,1060045,1947.8,1954.0,2260.0,2369.0
1786827376.207,112648.1,1153,1126481,1953.3,1939.0,2283.0,2419.0
1786827386.207,84315.1,863,843151,1928.0,1933.0,2234.0,2290.0
1786827396.207,75717.5,775,757175,1899.0,1901.0,2201.0,2250.0
1786827406.207,170584.2,1746,1705842,4351.9,2068.0,13138.0,18925.0
1786827416.207,105418.3,1079,1054183,1977.6,1978.0,2323.0,2433.0
1786827426.207,112061.9,1147,1120619,1981.6,1977.0,2330.0,2433.0
1786827436.207,112550.4,1152,1125504,1975.7,1979.0,2385.0,2487.0
1786827446.207,105418.3,1079,1054183,1968.9,1955.0,2341.0,2459.0
1786827456.207,98774.7,1011,987747,1943.7,1952.0,2266.0,2351.0
1786827466.207,80797.9,827,807979,1886.6,1881.0,2181.0,2230.0
1786827476.207,75424.4,772,754244,1886.8,1890.0,2189.0,2239.0
1786827486.207,187486.3,1919,1874863,6859.0,2061.0,23941.0,28759.0
1786827496.207,20028.5,205,200285,1955.2,1954.0,2254.0,2354.0
"""


# Paste migration events CSV here (format: timestamp_s,round,n_migrations).
CSV_BLOCK_MIGRATION = """
timestamp_s,round,n_migrations
1786826111.234,60,0
1786826129.336,120,0
1786826147.380,180,0
1786826165.424,240,0
1786826183.455,300,0
1786826201.411,360,0
1786826219.444,420,0
1786826237.465,480,0
1786826255.485,540,0
1786826273.525,600,0
1786826291.556,660,0
1786826309.579,720,0
1786826327.614,780,0
1786826346.022,840,11450
1786826364.067,900,38665
1786826381.817,960,48856
1786826399.753,1020,28929
1786826417.782,1080,18371
1786826435.744,1140,13305
1786826453.738,1200,8993
1786826471.719,1260,7807
1786826489.758,1320,8422
1786826507.769,1380,11039
1786826525.784,1440,5948
1786826543.736,1500,6677
1786826561.692,1560,0
1786826579.738,1620,0
1786826597.762,1680,0
1786826615.778,1740,0
1786826633.802,1800,0
1786826651.836,1860,0
1786826669.875,1920,0
1786826687.871,1980,0
1786826706.348,2040,7551
1786826724.420,2100,42044
1786826742.403,2160,48285
1786826760.317,2220,30900
1786826778.275,2280,18898
1786826796.243,2340,11859
1786826814.275,2400,11335
1786826832.268,2460,8568
1786826850.278,2520,8249
1786826868.265,2580,4064
1786826886.301,2640,7843
1786826904.318,2700,8701
1786826922.306,2760,7093
1786826940.145,2820,0
1786826958.140,2880,0
1786826976.135,2940,0
1786826994.209,3000,0
1786827012.164,3060,0
1786827030.227,3120,0
1786827048.266,3180,0
1786827066.291,3240,0
1786827084.316,3300,0
1786827102.356,3360,0
1786827120.330,3420,0
1786827138.336,3480,0
1786827156.388,3540,0
1786827174.407,3600,0
1786827192.437,3660,0
1786827210.401,3720,0
1786827228.465,3780,0
1786827246.503,3840,0
1786827264.519,3900,0
1786827282.545,3960,0
1786827300.551,4020,0
1786827318.482,4080,0
1786827336.510,4140,0
1786827354.542,4200,0
1786827372.546,4260,0
1786827390.598,4320,0
1786827408.615,4380,0
1786827426.612,4440,0
1786827444.634,4500,0
1786827462.650,4560,0
1786827480.682,4620,0
"""


# Paste the raw tps_timeline.csv content from the baseline (no-LB) run here.
# Format: timestamp_s, tps, n_batches, n_txs, lat_mean, lat_p50, lat_p90, lat_p95
CSV_BLOCK_BASELINE = """

"""


def parse_raw_csv(text):
    """Parse raw CSV text (no block marker) into a list of dicts with valid timestamp_s."""
    lines = [line for line in text.strip().splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    rows = []
    for row in csv.DictReader(lines):
        timestamp = (row.get("timestamp_s") or "").strip()
        if not timestamp:
            continue
        try:
            float(timestamp)
        except ValueError:
            continue
        rows.append(row)
    return rows or None


def load_run_csvs(run_dir):
    """Return (tps_rows, mig_rows) for one run dir, same shape as the pasted CSV blocks."""
    from parse_tps_migration_timeline import MIG_BLOCK_RE, parse_csv_block

    run_dir = Path(run_dir)
    tps_path = run_dir / "tps_timeline.csv"
    log_path = run_dir / "output.log"
    assert tps_path.exists(), (
        f"Missing {tps_path.resolve()}\n"
        f"  -> run: python benchmark/tps_timeline.py {run_dir.resolve()} > {tps_path.resolve()}"
    )
    assert log_path.exists(), f"Missing {log_path.resolve()}"

    tps_rows = parse_raw_csv(tps_path.read_text())
    assert tps_rows, f"No valid TPS rows in {tps_path.resolve()}"
    mig_rows = parse_csv_block(log_path.read_text(encoding="utf-8"), MIG_BLOCK_RE) or []
    return tps_rows, mig_rows


def _draw_imbalance_markers(axes):
    for ax in axes:
        for marker_s in IMBALANCE_MARKERS_S:
            ax.axvline(marker_s, color="black", linestyle="--", linewidth=0.8, alpha=0.7)


def plot_timeline(tps_rows, mig_rows, output, latency_col, baseline_rows):
    tps_rows = sorted(tps_rows, key=lambda row: float(row["timestamp_s"]))
    t0 = float(tps_rows[0]["timestamp_s"])

    assert "tps" in tps_rows[0], "CSV_BLOCK must have a 'tps' column"

    tps_rows = [row for row in tps_rows if float(row["tps"]) <= TPS_OUTLIER]
    assert tps_rows, f"All LB TPS rows filtered (threshold={TPS_OUTLIER})"
    tps_times = [float(row["timestamp_s"]) - t0 for row in tps_rows]

    # Pre-process baseline so baseline_times is available for the latency section
    baseline_times = []
    if baseline_rows:
        baseline_rows = sorted(baseline_rows, key=lambda row: float(row["timestamp_s"]))
        baseline_rows = [row for row in baseline_rows if float(row["tps"]) <= TPS_OUTLIER]
        assert baseline_rows, f"All baseline TPS rows filtered (threshold={TPS_OUTLIER})"
        baseline_t0 = float(baseline_rows[0]["timestamp_s"])
        baseline_times = [float(row["timestamp_s"]) - baseline_t0 for row in baseline_rows]

    # Determine latency data before building the figure so we know how many panels to create
    lat_data = []       # (times, values, linestyle) for each series
    if latency_col:
        col = f"lat_{latency_col}"
        lat_pairs = [
            (t, float(row[col]))
            for t, row in zip(tps_times, tps_rows)
            if row.get(col, "").strip()
        ]
        if lat_pairs:
            lt, lv = zip(*lat_pairs)
            lat_data.append((list(lt), list(lv), "-"))
        if baseline_rows:
            bl_lat_pairs = [
                (t, float(row[col]))
                for t, row in zip(baseline_times, baseline_rows)
                if row.get(col, "").strip()
            ]
            if bl_lat_pairs:
                blt, blv = zip(*bl_lat_pairs)
                lat_data.append((list(blt), list(blv), "--"))

    n_panels = 2 + (1 if lat_data else 0)
    height_ratios = [3, 2, 2] if lat_data else [3, 2]
    fig, axes = plt.subplots(
        n_panels, 1, figsize=(4.5, PANEL_HEIGHT_IN * n_panels + 0.1), sharex=True,
        gridspec_kw={"height_ratios": height_ratios},
    )
    ax_tps = axes[0]
    ax_lat = axes[1] if lat_data else None
    ax_mig = axes[-1]

    # --- TPS panel ---
    total_tps_vals = [float(row["tps"]) for row in tps_rows]
    pt, pv = _smoothed_series(tps_times, total_tps_vals)
    ax_tps.plot(pt, pv, color=COLORS[0], linestyle="-", linewidth=1.0)
    if baseline_rows:
        baseline_tps_vals = [float(row["tps"]) for row in baseline_rows]
        pt, pv = _smoothed_series(baseline_times, baseline_tps_vals)
        ax_tps.plot(pt, pv, color=COLORS[0], linestyle="--", linewidth=0.8)
    ax_tps.set_ylabel("Throughput\n[ktrans/s]", fontsize=6, linespacing=0.9)
    ax_tps.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}"))
    ax_tps.grid(True)

    # --- Latency panel ---
    if ax_lat is not None:
        for lt, lv, ls in lat_data:
            pt, pv = _smoothed_series(lt, lv)
            ax_lat.plot(pt, pv, color=COLORS[0], linestyle=ls, linewidth=1.0 if ls == "-" else 0.8)
        ax_lat.set_ylabel(f"{latency_col.capitalize()} Latency\n[s]", fontsize=6, linespacing=0.9)
        ax_lat.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}"))
        ax_lat.grid(True)

    # --- Migrations panel ---
    if mig_rows:
        mig_rows = sorted(mig_rows, key=lambda row: float(row["timestamp_s"]))
        mig_times = [float(row["timestamp_s"]) - t0 for row in mig_rows]
        mig_counts = [int(row["n_migrations"]) for row in mig_rows]
        cumulative = list(np.cumsum(mig_counts))
        ax_mig.step([0.0] + mig_times, [0] + cumulative,
                    where="post", color=COLORS[0], linewidth=1.0)
    else:
        ax_mig.text(
            0.5, 0.5, "no migrations", transform=ax_mig.transAxes,
            ha="center", va="center", fontsize=7, color="gray",
        )
    ax_mig.set_ylabel("Cumul.\nMigs", fontsize=6, linespacing=0.9)
    ax_mig.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"
    ))
    ax_mig.set_xlabel("Time (s)")
    ax_mig.grid(True)

    for ax in axes:
        ax.yaxis.set_major_locator(MaxNLocator(nbins=3))
        ax.xaxis.set_major_locator(MaxNLocator(nbins=6))
        ax.margins(y=Y_MARGIN)
        ax.tick_params(pad=1.5)

    max_t = tps_times[-1] if tps_times else 0.0
    if baseline_times:
        max_t = max(max_t, baseline_times[-1])
    left  = TRIM_LEFT_DUR
    right = max_t - TRIM_RIGHT_DUR
    if right > left:
        ax_tps.set_xlim(left=left, right=right)

    _draw_imbalance_markers(axes)

    fig.subplots_adjust(left=0.12, right=0.97, top=0.96, bottom=0.12, hspace=HSPACE)
    plt.savefig(output, dpi=200, bbox_inches="tight", pad_inches=0)
    pdf_path = Path(output).with_suffix(".pdf")
    plt.savefig(pdf_path, bbox_inches="tight", pad_inches=0)
    print(f"Saved: {pdf_path.resolve()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output PNG path.",
    )
    parser.add_argument(
        "--latency",
        choices=["mean", "p50", "p90", "p95"],
        default="mean",
        help="Latency metric to show in the latency panel (default: mean).",
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Overlay baseline (no-LB) TPS from CSV_BLOCK_BASELINE on the main LB TPS plot.",
    )
    parser.add_argument(
        "--run-dir",
        default=None,
        help=(
            "Single run dir holding tps_timeline.csv (from benchmark/tps_timeline.py) and "
            "output.log. Takes precedence over the pasted CSV blocks, so a new run can be "
            "plotted without overwriting the reference blocks."
        ),
    )
    args = parser.parse_args()

    output = args.output or "hotspot_shift.pdf"
    _apply_sigmod_style()

    if args.run_dir:
        tps_rows, mig_rows = load_run_csvs(args.run_dir)
        plot_timeline(tps_rows, mig_rows, output, args.latency, None)
        return

    assert CSV_BLOCK.strip(), "No log input provided in CSV_BLOCK"
    tps_rows = parse_raw_csv(CSV_BLOCK)
    mig_rows = parse_raw_csv(CSV_BLOCK_MIGRATION) or []
    assert tps_rows, "No valid TPS rows found in CSV_BLOCK"

    baseline_rows = None
    if args.baseline:
        assert CSV_BLOCK_BASELINE.strip(), "CSV_BLOCK_BASELINE is empty — paste baseline tps_timeline.py output into it"
        baseline_rows = parse_raw_csv(CSV_BLOCK_BASELINE)
        assert baseline_rows, "No valid rows found in CSV_BLOCK_BASELINE"

    plot_timeline(tps_rows, mig_rows, output, args.latency, baseline_rows)


if __name__ == "__main__":
    main()
