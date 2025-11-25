import pandas as pd

def calculate_county_cost(file_path, total_risky_jobs):
    
    df = pd.read_csv(file_path, encoding="latin1")

    year_row = df.iloc[0]

  
    new_cols = []
    for col, yr in zip(df.columns, year_row):
        yr = str(yr).strip()

        # If yr is a valid year value, use it
        if yr.isdigit() and yr in ["2020","2021","2022","2023","2024"]:
            new_cols.append(yr)
        else:
            new_cols.append(col)

    df.columns = new_cols

    # Remove the first row (since it only contained year labels)
    df = df.iloc[1:].reset_index(drop=True)

    if "Geographic Area" not in df.columns:
        # It is always the first column in your CSV
        df = df.rename(columns={df.columns[0]: "Geographic Area"})

    df["Geographic Area"] = (
        df["Geographic Area"]
        .astype(str)
        .str.strip()
        .str.replace(r"^\.", "", regex=True)
    )

    year_cols = [col for col in df.columns if col in ["2020","2021","2022","2023","2024"]]

    if not year_cols:
        raise ValueError(" No population columns detected — check CSV format.")

    latest_year = max(year_cols)

    print(f"➡ Latest year column detected: {latest_year}")

    # Convert population to numeric
    df[latest_year] = (
        df[latest_year]
        .astype(str)
        .str.replace(",", "")
        .replace("", "0")
        .astype(float)
    )

    # Extract California population
    california_population = df[df["Geographic Area"] == "California"][latest_year].values[0]

    counties = df[df["Geographic Area"] != "California"].copy()

    counties["county_share"] = counties[latest_year] / california_population
    counties["county_jobs_risk"] = counties["county_share"] * total_risky_jobs
    counties["cost_county"] = counties["county_jobs_risk"] * 350 * 26

    # 
    out_path = "data/california_county_risk.csv"
    counties.to_csv(out_path, index=False)

    return out_path
