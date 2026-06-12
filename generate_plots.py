"""
Queue-Depth Visualizer.
Reads logs/queue_depths.log and generates matplotlib charts for each backpressure strategy.
Generates synthetic data if the log file is missing.

Usage:
    python generate_plots.py
"""

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

LOG_FILE   = "logs/queue_depths.log"
PLOTS_DIR  = "plots"
STRATEGIES = ["rate_limiting", "admission_control", "load_shedding"]


def load_log(path: str) -> dict:
    """Parse the queue depth log file."""
    data = defaultdict(lambda: defaultdict(list))

    try:
        with open(path) as fh:
            lines = [json.loads(l) for l in fh if l.strip()]
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:
        print(f"Warning: malformed log entry: {exc}")
        return {}

    if not lines:
        return {}

    # Normalize timestamps to seconds from the start of each strategy's run
    start_times: dict[str, float] = {}

    for entry in lines:
        strategy = entry.get("strategy", "unknown")
        ts       = entry["timestamp"]
        depths   = entry.get("depths", {})

        if strategy not in start_times:
            start_times[strategy] = ts

        relative_t = ts - start_times[strategy]

        for sub_id, depth in depths.items():
            label = sub_id[:8]
            data[strategy][label].append((relative_t, depth))

    return data


def make_synthetic_data() -> dict:
    """Generate sample queue-depth curves for visualization testing."""
    import math, random
    random.seed(42)
    T = 60

    def noisy(val, sigma=1.5):
        return max(0, val + random.gauss(0, sigma))

    data = defaultdict(lambda: defaultdict(list))

    for t in range(T):
        # Rate limiting simulation
        depth_fast   = noisy(min(8,  t * 0.4))
        depth_medium = noisy(min(10, t * 0.5))
        depth_slow   = noisy(min(10, t * 0.6))
        data["rate_limiting"]["fast-sub"].append((t, depth_fast))
        data["rate_limiting"]["medium-sub"].append((t, depth_medium))
        data["rate_limiting"]["slow-sub"].append((t, depth_slow))

        # Admission control simulation
        cycle = t % 20
        depth_ac = noisy(cycle * 5 if cycle < 10 else (20 - cycle) * 5)
        data["admission_control"]["fast-sub"].append((t, max(0, depth_ac * 0.3)))
        data["admission_control"]["medium-sub"].append((t, max(0, depth_ac * 0.7)))
        data["admission_control"]["slow-sub"].append((t, max(0, depth_ac)))

        # Load shedding simulation
        if t < 15:
            depth_ls_slow = noisy(t * 1.5)
        else:
            depth_ls_slow = noisy(max(0, 22 - (t - 15) * 1.0))
        data["load_shedding"]["fast-sub"].append((t, noisy(min(3, t * 0.15))))
        data["load_shedding"]["medium-sub"].append((t, noisy(min(6, t * 0.25))))
        data["load_shedding"]["slow-sub"].append((t, depth_ls_slow))

    return data


COLORS = ["#2196F3", "#4CAF50", "#FF5722", "#9C27B0", "#FF9800"]
LABELS = {
    "rate_limiting":    "Rate Limiting (Token Bucket)",
    "admission_control":"Admission Control (Queue Depth Gate)",
    "load_shedding":    "Load Shedding (Priority Drops)",
}


def plot_strategy(strategy: str, series: dict, output_path: str) -> None:
    """Generate a plot for a single strategy and save it to a PNG file."""
    fig, ax = plt.subplots(figsize=(10, 5))

    for i, (label, points) in enumerate(sorted(series.items())):
        if not points:
            continue
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        ax.plot(xs, ys, label=label, color=COLORS[i % len(COLORS)], linewidth=1.8)

    ax.set_title(f"Queue Depth Over Time\nStrategy: {LABELS.get(strategy, strategy)}", fontsize=13)
    ax.set_xlabel("Time (seconds)", fontsize=11)
    ax.set_ylabel("In-memory queue depth (events)", fontsize=11)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.legend(title="Subscriber", fontsize=9)
    ax.grid(True, linestyle="--", alpha=0.5)
    fig.tight_layout()
    plt.savefig(output_path, dpi=120)
    plt.close(fig)
    print(f"  Saved: {output_path}")


def plot_comparison(all_data: dict, output_path: str) -> None:
    """Generate a stacked subplot comparing all strategies side-by-side."""
    fig, axes = plt.subplots(len(STRATEGIES), 1, figsize=(10, 12), sharex=False)

    for ax, strategy in zip(axes, STRATEGIES):
        series = all_data.get(strategy, {})
        for i, (label, points) in enumerate(sorted(series.items())):
            if not points:
                continue
            xs = [p[0] for p in points]
            ys = [p[1] for p in points]
            ax.plot(xs, ys, label=label, color=COLORS[i % len(COLORS)], linewidth=1.6)

        ax.set_title(LABELS.get(strategy, strategy), fontsize=10)
        ax.set_ylabel("Queue depth", fontsize=9)
        ax.legend(fontsize=8, loc="upper right")
        ax.grid(True, linestyle="--", alpha=0.4)
        ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    axes[-1].set_xlabel("Time (seconds)", fontsize=10)
    fig.suptitle("Backpressure Strategy Comparison – Queue Depth", fontsize=13, y=1.01)
    fig.tight_layout()
    plt.savefig(output_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


def main():
    if not HAS_MATPLOTLIB:
        print("matplotlib is not installed. Run: pip install matplotlib")
        sys.exit(1)

    os.makedirs(PLOTS_DIR, exist_ok=True)

    data = load_log(LOG_FILE)
    if data:
        print(f"Loaded real data from '{LOG_FILE}'")
        strategies_found = list(data.keys())
        print(f"  Strategies in log: {strategies_found}")
    else:
        print(f"No data found in '{LOG_FILE}'. Using synthetic example data.")
        print("  (Run load_test.py for each strategy to generate real data.)")
        data = make_synthetic_data()

    print(f"\nGenerating plots in '{PLOTS_DIR}/'...")

    for strategy in STRATEGIES:
        if strategy in data:
            path = os.path.join(PLOTS_DIR, f"{strategy}.png")
            plot_strategy(strategy, data[strategy], path)
        else:
            print(f"  (No data for strategy '{strategy}' — skipping)")

    if data:
        plot_comparison(data, os.path.join(PLOTS_DIR, "comparison.png"))

    print("\nAll plots generated successfully.")
    print("Open the plots/ directory to view the PNG files.")


if __name__ == "__main__":
    main()
