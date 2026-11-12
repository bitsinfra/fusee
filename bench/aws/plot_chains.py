"""Draw the five observed trial percentiles from the audited comparison archives."""
import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

ROWS = [
    ("fusee-local", "Fusee local", "Embedded, local flush"),
    ("dbos-sqlite", "DBOS SQLite", "SQLite WAL, synchronous=FULL"),
    ("dbos-postgres", "DBOS PostgreSQL", "Committed PostgreSQL step"),
    ("fusee-server", "Fusee server", "Same-host server flush"),
    ("restate", "Restate", "Single-node journal"),
    ("absurd", "Absurd", "PostgreSQL checkpoint"),
    ("obelisk", "Obelisk", "WASM activity, SQLite FULL"),
    ("hatchet", "Hatchet", "PostgreSQL task DAG"),
    ("temporal", "Temporal", "Regular activity, PostgreSQL"),
    ("temporal-tuned", "Temporal tuned", "Regular activity, PostgreSQL"),
    ("temporal-local", "Temporal Local Activities", "Workflow-task commit"),
    ("inngest-buffered", "Inngest buffered", "Asynchronous checkpoint"),
    ("inngest", "Inngest standard", "Persistent dev server"),
]

def plot(manifest, path):
    attempts=json.loads(manifest.read_text())["attempts"]
    fig,ax=plt.subplots(figsize=(11.4,6.2))
    fig.subplots_adjust(left=.38,right=.98,top=.90,bottom=.13)
    ink="#292a31"; accent="#a63b24"; quiet="#656772"
    ax.set_xscale("log")
    ax.set_xlim(10,1_500_000)
    ax.set_ylim(len(ROWS)-.4,-.65)
    ax.set_yticks([])
    ax.set_xticks([10,100,1000,10000,100000,1000000],["10 µs","100 µs","1 ms","10 ms","100 ms","1 s"])
    ax.minorticks_off()
    for i,(system,name,scope) in enumerate(ROWS):
        ax.text(-.60,i,name,transform=ax.get_yaxis_transform(),va="center",fontsize=10,color=ink)
        ax.text(-.28,i,scope,transform=ax.get_yaxis_transform(),va="center",fontsize=7.6,color=quiet)
        chosen={a["trial"]:a for a in attempts if a["system"]==system and a["steps"]==1000 and a["status"] in ("measured","unsupported")}
        assert len(chosen)==5
        if all(a["status"]=="unsupported" for a in chosen.values()):
            ax.text(14,i,"Server step limit reached in all five trials",va="center",color=quiet,fontsize=9)
            continue
        for trial,a in sorted(chosen.items()):
            assert a["status"]=="measured"
            h=i+(trial-2)*.073
            ax.plot([a["p50_us"],a["p99_us"]],[h,h],color="#d4ccc9",lw=.9,zorder=1)
            ax.plot(a["p50_us"],h,"o",ms=3.8,color=accent)
            ax.plot(a["p99_us"],h,"o",ms=3.8,color="white",markeredgecolor=ink,markeredgewidth=.8)
    ax.grid(axis="x",color="#dedfe2",lw=.7)
    ax.set_axisbelow(True)
    for edge in ("left","top","right"):ax.spines[edge].set_visible(False)
    ax.spines["bottom"].set_color("#adaeb5")
    ax.set_xlabel("Time from one activity body returning to its dependent successor starting")
    fig.text(.02,.962,"Durable execution on i7i.2xlarge local NVMe",fontsize=16,color=ink,weight="semibold")
    handles=[Line2D([],[],marker="o",color="none",markerfacecolor=c,markeredgecolor=edge,label=label,markersize=5)
             for c,edge,label in [(accent,accent,"Trial p50"),("white",ink,"Trial p99")]]
    fig.legend(handles=handles,loc="upper right",bbox_to_anchor=(.985,.995),frameon=False,ncol=2)
    with plt.rc_context({"svg.fonttype":"none","svg.hashsalt":"fusee-comparison"}):
        fig.savefig(path,metadata={"Date":None})
    plt.close(fig)

if __name__=="__main__":
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument("manifest",type=Path)
    cli.add_argument("output",type=Path)
    args=cli.parse_args()
    plot(args.manifest,args.output)
