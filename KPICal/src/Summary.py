import pandas as pd

summary = pd.DataFrame(columns=["Site Name", "Year-Month", "Availability"])


def update_summary(summary, site_name, year_month, availability):
    mask = (summary["Site Name"] == site_name) & (summary["Year-Month"] == year_month)
    if summary[mask].empty:
        new_row = pd.DataFrame(
            {
                "Site Name": site_name,
                "Year-Month": year_month,
                "Availability": availability,
            },
            index=[0],
        )
        summary = pd.concat([summary, new_row], ignore_index=True)
    else:
        summary.loc[mask, "Availability"] = availability

    return summary
