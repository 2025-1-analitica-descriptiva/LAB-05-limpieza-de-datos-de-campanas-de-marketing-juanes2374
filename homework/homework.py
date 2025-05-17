import os
import pandas as pd
import zipfile
from glob import glob

def clean_campaign_data():
    output_path = "files/output"
    os.makedirs(output_path, exist_ok=True)

    all_files = sorted(glob("files/input/bank-marketing-campaing-*.csv.zip"))
    dfs = []
    for zf_path in all_files:
        with zipfile.ZipFile(zf_path) as z:
            for file in z.namelist():
                if file.endswith(".csv"):
                    with z.open(file) as f:
                        df = pd.read_csv(f)
                        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    
    # Imprime las columnas para inspección (solo la primera vez)
    # print(df.columns)

    # Ajuste de nombres según lo que el test y enunciado piden:
    colmap = {
        "client_id": "client_id",
        "age": "age",
        "job": "job",
        "marital": "marital",
        "education": "education",
        "credit_default": "credit_default",
        "mortgage": "mortgage",
        # Estos nombres suelen variar:
        "number_contacts": None,  # campaign_contacts
        "contact_duration": "contact_duration",
        "previous_campaign_contacts": None,  # previous_campaing_contacts
        "previous_outcome": "previous_outcome",
        "campaign_outcome": "campaign_outcome",
        "day": "day",
        "month": "month",
        "cons_price_idx": "cons_price_idx",
        "euribor_three_months": "euribor_three_months",
    }
    # Busca nombres alternativos para las columnas ambiguas
    for c in df.columns:
        if c.lower() in ["campaign_contacts", "number_contacts", "nr_contacts"]:
            colmap["number_contacts"] = c
        if c.lower() in ["previous_campaign_contacts", "previous_campaing_contacts"]:
            colmap["previous_campaign_contacts"] = c

    # --- CLIENT ---
    client = pd.DataFrame()
    client["client_id"] = df[colmap["client_id"]]
    client["age"] = df[colmap["age"]]
    client["job"] = df[colmap["job"]].str.replace(r"\.", "", regex=True).str.replace("-", "_")
    client["marital"] = df[colmap["marital"]]
    client["education"] = (
        df[colmap["education"]].str.replace(".", "_").replace("unknown", pd.NA)
    )
    client["credit_default"] = df[colmap["credit_default"]].map(lambda x: 1 if x == "yes" else 0)
    client["mortgage"] = df[colmap["mortgage"]].map(lambda x: 1 if x == "yes" else 0)
    client.to_csv(f"{output_path}/client.csv", index=False)

    # --- CAMPAIGN ---
    campaign = pd.DataFrame()
    campaign["client_id"] = df[colmap["client_id"]]
    campaign["number_contacts"] = df[colmap["number_contacts"]]
    campaign["contact_duration"] = df[colmap["contact_duration"]]
    campaign["previous_campaign_contacts"] = df[colmap["previous_campaign_contacts"]]
    campaign["previous_outcome"] = df[colmap["previous_outcome"]].map(lambda x: 1 if x == "success" else 0)
    campaign["campaign_outcome"] = df[colmap["campaign_outcome"]].map(lambda x: 1 if x == "yes" else 0)
    # Mes/día
    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }
    days = df[colmap["day"]].astype(str).str.zfill(2)
    months = df[colmap["month"]].str.lower().map(month_map)
    campaign["last_contact_date"] = "2022-" + months + "-" + days
    campaign.to_csv(f"{output_path}/campaign.csv", index=False)

    # --- ECONOMICS ---
    economics = pd.DataFrame()
    economics["client_id"] = df[colmap["client_id"]]
    economics["cons_price_idx"] = df[colmap["cons_price_idx"]]
    economics["euribor_three_months"] = df[colmap["euribor_three_months"]]
    economics.to_csv(f"{output_path}/economics.csv", index=False)

if __name__ == "__main__":
    clean_campaign_data()

