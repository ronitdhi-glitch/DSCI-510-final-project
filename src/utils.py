import pandas as pd
import matplotlib.pyplot as plt

def plot_top_risk_jobs(risky_csv_path):
    df = pd.read_csv(risky_csv_path, encoding="latin1")
    top = df.nlargest(10, "California")

    
    plt.figure(figsize=(12, 8))

    plt.bar(top["Occupation"], top["California"])
    plt.xticks(rotation=75)

    plt.title("Top 10 High-Risk Jobs in California")

    
    plt.subplots_adjust(bottom=0.30)

    plt.tight_layout()
    plt.savefig("data/california_risk_chart.png")
