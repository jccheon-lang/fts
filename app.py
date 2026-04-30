import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

SERVICE_KEY = os.getenv("FTC_SERVICE_KEY")

BASE_URL = "http://apis.data.go.kr/1130000/franchiseListService/getFranchiseList"

def fetch_all(years):
    results = []

    for year in years:
        page = 1
        while True:
            params = {
                "serviceKey": SERVICE_KEY,
                "pageNo": page,
                "numOfRows": 100,
                "resultType": "json",
                "jngBizCrtraYr": year
            }

            res = requests.get(BASE_URL, params=params)
            data = res.json()

            items = data.get("response", {}).get("body", {}).get("items", [])

            if not items:
                break

            results.extend(items)
            page += 1

    df = pd.DataFrame(results)
    df.to_csv("ftc_master.csv", index=False)
    return df


def normalize(x):
    if pd.isna(x):
        return ""
    return str(x).replace(" ", "").replace("(주)", "").lower()


def match(df_master, df_input):
    df_master["company_norm"] = df_master["frcsCnfmCmpnyNm"].apply(normalize)
    df_input["company_norm"] = df_input["사업자명"].apply(normalize)

    merged = df_input.merge(
        df_master,
        on="company_norm",
        how="left"
    )

    merged["match_result"] = merged["frcsCnfmCmpnyNm"].apply(
        lambda x: "MATCH" if pd.notna(x) else "NO_MATCH"
    )

    merged.to_excel("matched_results.xlsx", index=False)
    return merged


if __name__ == "__main__":
    years = [2023, 2024, 2025]

    print("Fetching FTC data...")
    master = fetch_all(years)

    print("Reading input...")
    df_input = pd.read_excel("input.xlsx")

    print("Matching...")
    match(master, df_input)

    print("Done!")
