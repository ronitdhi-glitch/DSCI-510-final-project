import os
import matplotlib.pyplot as plt
import seaborn as sns

from config import REPORTS_DIR

os.makedirs(REPORTS_DIR, exist_ok=True)

def plot_top_10(df):
    plt.figure(figsize=(12,6))
    plt.bar(df["state"], df["jobs_lost_risk"])
    plt.xticks(rotation=90)
    plt.title("Top 10 States at Highest AI Automation Risk")
    plt.xlabel("State")
    plt.ylabel("Estimated Job Loss Risk")

    plt.tight_layout()
    plt.savefig(os.path.join(REPORTS_DIR, "top_10_states.png"))
    plt.close()


def plot_bottom_10(df):
    plt.figure(figsize=(12,6))
    plt.bar(df["state"], df["jobs_lost_risk"])
    plt.xticks(rotation=90)
    plt.title("Bottom 10 States at Lowest AI Automation Risk")
    plt.xlabel("State")
    plt.ylabel("Estimated Job Loss Risk")

    plt.tight_layout()
    plt.savefig(os.path.join(REPORTS_DIR, "bottom_10_states.png"))
    plt.close()


def plot_distribution(df):
    plt.figure(figsize=(8,5))
    plt.hist(df["probability"], bins=10)
    plt.title("Distribution of Automation Probability")
    plt.xlabel("Probability")
    plt.ylabel("Number of Occupations")

    plt.savefig(os.path.join(REPORTS_DIR, "probability_distribution.png"))
    plt.close()
