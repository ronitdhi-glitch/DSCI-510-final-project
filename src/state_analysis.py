def filter_high_risk_jobs(df):
    # Returns only occupations where Probability is greater than the median 0.640.
    return df[df["probability"] > 0.640]


def get_top_states(df):
    
    # Returns the top 10 states with the highest estimated job loss risk.
    state_columns = df.columns[3:]   # skip soc, occupation, probability

    # Created a dictionary to store state level job loss estimates. 
    state_loss = {}
    for state in state_columns:
    # multiply probability * employment count per state and then sum across all occupations. 
        state_loss[state] = (df[state] * df["probability"]).sum()

    
    import pandas as pd
    result = pd.DataFrame({
        "state": state_loss.keys(),
        "jobs_lost_risk": state_loss.values()
    }).sort_values(by="jobs_lost_risk", ascending=False)

    return result.head(10)


def get_bottom_states(df):
    # Calculates and return the 10 states with the lowest estimated job loss.
    state_columns = df.columns[3:]

    state_loss = {}
    for state in state_columns:
        state_loss[state] = (df[state] * df["probability"]).sum()

    import pandas as pd
    result = pd.DataFrame({
        "state": state_loss.keys(),
        "jobs_lost_risk": state_loss.values()
    }).sort_values(by="jobs_lost_risk", ascending=True)

    return result.head(10)
