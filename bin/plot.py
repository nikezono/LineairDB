#! /usr/bin/env python3

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from IPython import embed
import sys
import os

PROPOSAL = "OpenBw+PLI"
OPTIMIZED = "OpenBw+OPLI"
TARGET = "OpenBwTree"


def plot_config(key):
    if key == PROPOSAL:
        return {"label": PROPOSAL, "marker": ".", "linestyle": "solid"}
    if key == OPTIMIZED:
        return {"label": OPTIMIZED, "marker": "+", "linestyle": "solid"}
    if key == TARGET:
        return {"label": TARGET, "marker": "3", "linestyle": "solid"}
    return {}


def plot_config_abort(key):
    if key == PROPOSAL:
        return {"label": f"{PROPOSAL} (abort)", "marker": ".", "linestyle": "dashed"}
    if key == OPTIMIZED:
        return {"label": f"{OPTIMIZED} (abort)", "marker": ",", "linestyle": "dashed"}
    if key == TARGET:
        return {"label": f"{TARGET} (abort)", "marker": "3", "linestyle": "dashed"}
    return {}


def config():
    plt.rcParams['font.family'] = 'Times New Roman'
    plt.rcParams["font.size"] = 18
    plt.rcParams["legend.fontsize"] = 12
    plt.rcParams["lines.markersize"] = 10
    plt.rcParams["text.usetex"] = True
    plt.rcParams["figure.figsize"] = (7, 5)


def xlabel_gen(tag):
    if tag == "threads":
        return r"\# of threads"
    if tag == "scan":
        return r"Percentage of  $\texttt{\#Scan}$"
    if tag == "epoch":
        return r"Duration of synchronization (ms)"
    if tag == "logictime":
        return r"Duration of each transaction logic (ms)"
    if tag == "scanlimit":
        return r"\# of the limit of scan operations"
    return "Undefined"


def ylabel_gen(plot_target):
    if plot_target == "cps":
        return 'Throughput (commit/sec)'
    if plot_target == "aborts":
        return r"Abort Rate (\%)"
    return "Undefined"


def gen_data():
    return pd.read_csv(sys.argv[1])


def output_dir():
    dir = os.path.dirname(__file__) + "/working/" + sys.argv[2]
    os.makedirs(dir, exist_ok=True)
    return dir


def savePath(key):
    return f"{output_dir()}/{os.path.splitext(os.path.basename(sys.argv[1]))[0]}_{key}.pdf"


def main():
    data = gen_data()
    benchmarks = data["tag"].unique()

    for tag in benchmarks:
        for plot_target in ["cps", "aborts", "both"]:
            fig, ax = plt.subplots()
            if plot_target in ["both"]:
                ax2 = ax.twinx()

            df = data[data["tag"] == tag]
            df = data[data["threads"] < 81]

            for table in [PROPOSAL, TARGET]:
                filtered = df[df["name"] == table]
                grouped = filtered.groupby(by=tag)
                means = grouped.mean()

                if plot_target in ["both", "cps"]:
                    means.plot(ax=ax, y="cps", legend=False, yerr=grouped.std().cps,
                               **(plot_config(table)))
                    ax.set(xlabel=xlabel_gen(tag),
                           ylabel=ylabel_gen("cps"))
                    ax.set_xlim(df[tag].min(), df[tag].max())
                    ax.set_ylim(0, auto=True)
                    ax.set_yscale('linear')

                if plot_target in ["both", "aborts"]:
                    if plot_target == "aborts":
                        ax2 = ax
                    means.plot(ax=ax2, legend=False, y="abort_rate", yerr=grouped.std().abort_rate,
                               **(plot_config_abort(table)))
                    ax2.set(xlabel=xlabel_gen(tag))
                    ax2.set_ylabel(ylabel=ylabel_gen("aborts"))
                    ax2.set_ylim(0, 100)

            ax.grid()
            if (plot_target != "both"):
                ax.legend()
            else:
                h1, l1 = ax.get_legend_handles_labels()
                h2, l2 = ax2.get_legend_handles_labels()
                ax2.legend(h1+h2, l1+l2)
            fig.savefig(savePath(f"{tag}_{plot_target}"),
                        bbox_inches="tight", pad_inches=0.05)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise "Too few arguments. We require the followings: [0]: input_filename, [1]: output_directory"
    config()
    main()
    print(f"pdfs are generated in #{output_dir()} correctly.")
