import os
from datetime import datetime, timezone, timedelta

import pandas as pd

from allfish_data_import import fetch_allfish_df
from kashimamaru import fetch_kashimamaru_df

JST = timezone(timedelta(hours=9))
OUT_DIR = "data"
OUT_XLSX = os.path.join(OUT_DIR, "funaduri_daily.xlsx")

SHEET_ALLFISH = "all_fish"
SHEET_KASHI = "kashimamaru"


def now_jst_str():
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def load_sheet(path: str, sheet: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_excel(path, sheet_name=sheet, dtype=str)
    except Exception:
        return pd.DataFrame()


def append_dedup(old_df: pd.DataFrame, new_df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    # new_df が空なら old を返す
    if new_df is None or new_df.empty:
        return old_df if not old_df.empty else pd.DataFrame()

    # 文字列化（重複判定のブレ防止）
    for c in new_df.columns:
        new_df[c] = new_df[c].astype(str)

    if old_df.empty:
        merged = new_df.copy()
    else:
        # 列合わせ（どちらかにしかない列があっても落ちないように）
        for c in new_df.columns:
            if c not in old_df.columns:
                old_df[c] = ""
        for c in old_df.columns:
            if c not in new_df.columns:
                new_df[c] = ""

        merged = pd.concat([old_df, new_df], ignore_index=True)

    # キー列が無い場合に備える
    for c in key_cols:
        if c not in merged.columns:
            merged[c] = ""

    return merged.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # 取得
    df_all = fetch_allfish_df()       # ← ここが変更点
    df_k = fetch_kashimamaru_df()

    stamp = now_jst_str()

    if df_all is not None and not df_all.empty:
        df_all["取得日時"] = stamp
    if df_k is not None and not df_k.empty:
        df_k["取得日時"] = stamp

    # 既存のExcelを読み込んで追記＋重複排除
    old_all = load_sheet(OUT_XLSX, SHEET_ALLFISH)
    old_k = load_sheet(OUT_XLSX, SHEET_KASHI)

    # 全魚種：混在するので fish_code をキーに含める
    key_all = ["date", "fish_code", "yado", "area_port", "choka", "size", "point", "source"]
    out_all = append_dedup(old_all, df_all, key_all)

    # かしま丸：ここは列名に合わせて必要なら調整
    key_k = ["日付", "釣り物", "数量", "型", "場所", "備考"]
    out_k = append_dedup(old_k, df_k, key_k)

    # 保存（2シート）
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl", mode="w") as writer:
        out_all.to_excel(writer, sheet_name=SHEET_ALLFISH, index=False)
        out_k.to_excel(writer, sheet_name=SHEET_KASHI, index=False)

    print("Saved:", OUT_XLSX)
    print(f"all_fish: {len(out_all)} rows / kashimamaru: {len(out_k)} rows")


if __name__ == "__main__":
    main()
