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
    """
    old_df + new_df を連結して、key_cols で重複排除（keep='last'）
    ※ merged の後ろにある行（=新しい取得）が残る
    """
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

    # 重要：古い→新しいの順で並んでいる前提（old_df の後に new_df を concat）
    # keep="last" で新しい方を残す
    return merged.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # 取得（かしま丸は hash を使って更新が無い日は空DFにする）
    df_all = fetch_allfish_df()
    df_k = fetch_kashimamaru_df(use_hash=True)

    stamp = now_jst_str()

    if df_all is not None and not df_all.empty:
        df_all["取得日時"] = stamp
    if df_k is not None and not df_k.empty:
        df_k["取得日時"] = stamp

    # 既存Excel読み込み
    old_all = load_sheet(OUT_XLSX, SHEET_ALLFISH)
    old_k = load_sheet(OUT_XLSX, SHEET_KASHI)

    # all_fish：従来通り
    key_all = ["date", "fish_code", "yado", "area_port", "choka", "size", "point", "source"]
    out_all = append_dedup(old_all, df_all, key_all)

    # ✅ かしま丸：日付が重複したら「最新（取得日時が新しい方）」を残す
    # ＝ dedup のキーを "日付" のみにする
    key_k = ["日付"]
    out_k = append_dedup(old_k, df_k, key_k)

    # もし見やすくしたいなら並び替え（不要なら消してOK）
    if not out_k.empty and "取得日時" in out_k.columns:
        out_k["取得日時"] = pd.to_datetime(out_k["取得日時"], errors="coerce")
        # 日付は「2/24(火)」みたいな形式なので、ここでは取得日時で並べます
        out_k = out_k.sort_values("取得日時", ascending=False).reset_index(drop=True)
        # datetime→文字列に戻す（Excelで見やすく）
        out_k["取得日時"] = out_k["取得日時"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 保存（2シート）
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl", mode="w") as writer:
        out_all.to_excel(writer, sheet_name=SHEET_ALLFISH, index=False)
        out_k.to_excel(writer, sheet_name=SHEET_KASHI, index=False)

    print("Saved:", OUT_XLSX)
    print(f"all_fish: {len(out_all)} rows / kashimamaru: {len(out_k)} rows")


if __name__ == "__main__":
    main()
