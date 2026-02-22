import os
import pandas as pd
from datetime import datetime, timezone, timedelta

from tachi_turi_data_import import fetch_tatiuo_df
from kashimamaru import fetch_kashimamaru_df

JST = timezone(timedelta(hours=9))
OUT_DIR = "data"
OUT_XLSX = os.path.join(OUT_DIR, "funaduri_daily.xlsx")

def now_jst():
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    df_t = fetch_tatiuo_df()
    df_k = fetch_kashimamaru_df()

    stamp = now_jst()
    df_t["取得日時"] = stamp
    df_k["取得日時"] = stamp

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl", mode="w") as writer:
        df_t.to_excel(writer, sheet_name="tatiuo", index=False)
        df_k.to_excel(writer, sheet_name="kashimamaru", index=False)

    print("Saved:", OUT_XLSX)

if __name__ == "__main__":
    main()
