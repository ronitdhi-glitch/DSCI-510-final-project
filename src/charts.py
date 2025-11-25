import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os

def shorten(text, words=3):
    """Shorten long names for X-axis."""
    return " ".join(str(text).split()[:words])


# ------------------------------------------------------------
# NEW: BUBBLE CHART (Option #3)
# ------------------------------------------------------------
def generate_state_bubble_chart() -> None:
    """
    Bubble chart for states using automation_data_by_state.csv.

    X-axis  = automation risk per job (a 'risk'/'prob'/'automation' column)
    Y-axis  = total jobs/employment/population
    Size    = exposure = risk * jobs  (proxy for economic impact)
    Label   = state name
    """
    print("\nGenerating state automation exposure bubble chart...")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_dir, "..", "data", "automation_data_by_state.csv")

    if not os.path.exists(data_path):
        print(f"⚠️ {data_path} not found; skipping bubble chart.")
        return

    # --- FIX: robust CSV reading with fallback encoding ---
    try:
        df = pd.read_csv(data_path)
    except UnicodeDecodeError:
        print("⚠️ UTF-8 decode failed, retrying with 'latin1' and skipping bad lines...")
        df = pd.read_csv(
            data_path,
            encoding="latin1",
            engine="python",
            on_bad_lines="skip"
        )

    # 1) Find state column
    state_col = None
    for col in df.columns:
        if "state" in col.lower() or "jurisdiction" in col.lower():
            state_col = col
            break

    if state_col is None:
        print("⚠️ No valid state column found; skipping bubble chart.")
        return

    # 2) Numeric columns only
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        print("⚠️ No numeric columns; skipping bubble chart.")
        return

    # 3) Identify jobs + risk columns using keyword matching
    jobs_candidates = [
        c for c in num_cols
        if any(k in c.lower() for k in ["jobs", "employment", "total", "pop"])
    ]
    risk_candidates = [
        c for c in num_cols
        if any(k in c.lower() for k in ["risk", "prob", "automation"])
    ]

    if not jobs_candidates or not risk_candidates:
        print("⚠️ Could not find jobs/risk columns; skipping bubble chart.")
        print("    Numeric columns:", num_cols)
        return

    jobs_col = jobs_candidates[0]
    risk_col = risk_candidates[0]

    print(f"  Using jobs column: {jobs_col}")
    print(f"  Using risk column: {risk_col}")

    df_plot = df[[state_col, jobs_col, risk_col]].dropna()
    df_plot = df_plot[df_plot[jobs_col] > 0]

    if df_plot.empty:
        print("⚠️ No valid data for bubble chart.")
        return

    df_plot["exposure"] = df_plot[jobs_col] * df_plot[risk_col]

    # Top 15 most exposed states
    df_top = df_plot.nlargest(15, "exposure").copy()

    os.makedirs("reports", exist_ok=True)
    output_path = os.path.join("reports", "state_automation_bubble.png")

    # Bubble size scaling
    max_exp = df_top["exposure"].max()
    size_scale = 2000 / max_exp if max_exp > 0 else 1
    sizes = df_top["exposure"] * size_scale

    plt.figure(figsize=(12, 8))
    plt.scatter(
        df_top[risk_col],
        df_top[jobs_col],
        s=sizes,
        alpha=0.6,
        edgecolor="black"
    )

    # Add labels
    for _, row in df_top.iterrows():
        plt.text(
            row[risk_col],
            row[jobs_col],
            row[state_col],
            fontsize=8,
            ha="center",
            va="center"
        )

    plt.xlabel("Automation Risk per Job")
    plt.ylabel(jobs_col)
    plt.title("State Exposure to Automation (Bubble = Jobs × Risk)")

    ax = plt.gca()
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"  ✓ Saved state_automation_bubble.png to {output_path}")


# ------------------------------------------------------------
# ORIGINAL 4 GRAPHS (UNCHANGED)
# ------------------------------------------------------------

def generate_all_graphs(county_csv, risky_csv):
    os.makedirs("reports", exist_ok=True)

    # ---------- COUNTY PIE ----------
    county_df = pd.read_csv(county_csv)
    county_df = county_df.dropna(subset=["county_share"])
    county_top10_share = county_df.nlargest(10, "county_share")

    plt.figure(figsize=(10, 7))
    plt.pie(
        county_top10_share["county_share"],
        labels=county_top10_share["Geographic Area"].apply(shorten),
        autopct="%1.1f%%",
        startangle=90,
        colors=plt.cm.Set3.colors
    )
    plt.title("Top 10 Counties by Share of California Population")
    plt.savefig("reports/county_share_pie.png")
    plt.close()

    # ---------- COUNTY COST BAR ----------
    county_top10_cost = county_df.nlargest(10, "cost_county")
    county_top10_cost["short_label"] = county_top10_cost["Geographic Area"].apply(shorten)

    plt.figure(figsize=(12, 8))
    bars = plt.bar(
        county_top10_cost["short_label"],
        county_top10_cost["cost_county"],
        color=plt.cm.tab20.colors
    )
    plt.title("Top 10 Counties by Automation Cost Impact")
    plt.xticks(rotation=60)

    ax = plt.gca()
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))

    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            yval,
            f"{yval:,.0f}",
            ha='center',
            va='bottom',
            fontsize=8
        )

    plt.tight_layout()
    plt.savefig("reports/county_cost_bar.png")
    plt.close()

    # ---------- JOB RISK BAR ----------
    risky_df = pd.read_csv(risky_csv)
    risky_df["risk_score"] = risky_df["Probability"] * risky_df["California"]

    top10 = risky_df.nlargest(10, "risk_score")
    top10["short_label"] = top10["Occupation"].apply(shorten)

    plt.figure(figsize=(12, 8))
    bars = plt.bar(top10["short_label"], top10["risk_score"], color=plt.cm.Dark2.colors)
    plt.title("Top 10 Highest Job Loss Risk")
    plt.xticks(rotation=60)

    ax = plt.gca()
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))

    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            yval,
            f"{yval:,.0f}",
            ha='center',
            va='bottom',
            fontsize=8
        )

    plt.tight_layout()
    plt.savefig("reports/top10_risky_jobs.png")
    plt.close()

    # ---------- SCATTER ----------
    top10_scatter = risky_df.nlargest(10, "risk_score")
    top10_scatter["short_name"] = top10_scatter["Occupation"].apply(
        lambda x: x[:18] + "..." if len(str(x)) > 18 else str(x)
    )

    plt.figure(figsize=(15, 10))
    sizes = top10_scatter["California"] / 80

    scatter = plt.scatter(
        top10_scatter["short_name"],
        top10_scatter["Probability"],
        s=sizes,
        c=top10_scatter["Probability"],
        cmap="plasma",
        alpha=0.85
    )

    plt.title("Top 10 Tasks vs Automation Probability (Clear Visual)")
    plt.xlabel("Task Type (Shortened)")
    plt.ylabel("Probability of Automation")
    plt.xticks(rotation=45)

    plt.colorbar(scatter, label="Probability Level")
    plt.tight_layout()
    plt.savefig("reports/task_vs_probability_scatter.png")
    plt.close()

    # ---------- NEW: STATE BUBBLE CHART ----------
    generate_state_bubble_chart()

    print("\nGRAPH FILES GENERATED IN /reports:")
    print("  ✓ county_share_pie.png")
    print("  ✓ county_cost_bar.png")
    print("  ✓ top10_risky_jobs.png")
    print("  ✓ task_vs_probability_scatter.png")
    print("  ✓ state_automation_bubble.png")
