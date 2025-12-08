import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os

def shorten(text, words=3):
    """Shorten long names for X-axis."""
    return " ".join(str(text).split()[:words])

def generate_all_graphs(county_csv, risky_csv):
    os.makedirs("reports", exist_ok=True)

    # county pie chart
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

    # cost for counties bar graph
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

    # jobs at risk bar graph
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

    # scatter plot
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

    print("\nGRAPH FILES GENERATED IN /reports:")
    print("  ✓ county_share_pie.png")
    print("  ✓ county_cost_bar.png")
    print("  ✓ top10_risky_jobs.png")
    print("  ✓ task_vs_probability_scatter.png")
    print("  ✓ state_automation_bubble.png")

