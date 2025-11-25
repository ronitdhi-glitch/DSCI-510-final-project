def filter_high_risk_jobs(df):
    """
    Returns only occupations where Probability > 50%
    """
    return df[df["probability"] > 0.640]


def get_top_states(df):
    """
    Sums job loss risk for each state column.
    """
    state_columns = df.columns[3:]   # skip soc, occupation, probability

    # multiply probability * employment count per state
    state_loss = {}
    for state in state_columns:
        state_loss[state] = (df[state] * df["probability"]).sum()

    # sort and convert to dataframe
    import pandas as pd
    result = pd.DataFrame({
        "state": state_loss.keys(),
        "jobs_lost_risk": state_loss.values()
    }).sort_values(by="jobs_lost_risk", ascending=False)

    return result.head(10)


def get_bottom_states(df):
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