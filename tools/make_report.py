from __future__ import annotations

import re
from pathlib import Path
from datetime import timedelta
from collections import Counter

import numpy as np
import pandas as pd

# ===== 設定 =====
SRC = Path("data/funaduri_daily.xlsx")      # 元データExcel
SHEET_CANDIDATES = ["all_fish", "allfish"] # シート名ゆれ対策
OUT_MD = Path("reports/report.md")

# 任意：日次集計をためる（長期運用で便利）
HISTORY_CSV = Path("history/daily_metrics.csv")

WINDOW_MONTH = 30
WINDOW_YEAR = 365

# TOP5の対象にする最小隻数（魚種内比較が成立する範囲）
MIN_RECORDS_FOR_TOP5 = 3


# ===== 文字列レンジのパース =====
# Excel実データでは "X" が "〜" の代替として混ざっていました（例: "0.3X-1.5Xkg", "X-4 杯"）
# ここでは X をレンジ記号として扱い、数字レンジに直します。
def normalize_range_text(s: str) -> str:
    s = s.strip()
    # 例: "X-4" = "0-4" とみなす（下限が欠けるケース）
    s = re.sub(r"^X-", "0-", s)
    # 数値の後に出るXは「〜」相当として削る（"0.3X-1.5Xkg" -> "0.3-1.5kg"）
    s = s.replace("X", "")
    # "〜" "～" を "-" に統一
    s = s.replace("〜", "-").replace("～", "-")
    return s


def parse_range(val) -> tuple[float, float]:
    """ '18-40 尾' / '45cm' / 'X-4 杯' などを (min, max) に """
    if pd.isna(val):
        return (np.nan, np.nan)

    s = str(val).strip()
    if s in {"", "－", "-", "—", "―"}:
        return (np.nan, np.nan)

    s = normalize_range_text(s)

    # 数字・小数点・ハイフン以外を除去（単位などを落とす）
    s2 = re.sub(r"[^\d\.\-]", "", s)
    if not re.search(r"\d", s2):
        return (np.nan, np.nan)

    parts = [p for p in s2.split("-") if p]
    nums = []
    for p in parts[:2]:
        try:
            nums.append(float(p))
        except ValueError:
            pass

    if len(nums) == 0:
        return (np.nan, np.nan)
    if len(nums) == 1:
        return (nums[0], nums[0])
    return (min(nums), max(nums))


def mean_from_range_series(series: pd.Series) -> pd.Series:
    mm = series.apply(lambda x: pd.Series(parse_range(x), index=["min", "max"]))
    return mm[["min", "max"]].mean(axis=1)


def pct_vs(today: float, base: float) -> float:
    if pd.isna(today) or pd.isna(base) or base == 0:
        return np.nan
    return (today / base - 1.0) * 100.0


def fmt_pct(x: float) -> str:
    if pd.isna(x):
        return "NA"
    sign = "+" if x >= 0 else ""
    return f"{sign}{int(round(x))}%"


def extract_unit(text) -> str:
    """ '18-40 尾' -> '尾'  '0.3-1.5kg' -> 'kg' など。取れなければ空 """
    if pd.isna(text):
        return ""
    s = normalize_range_text(str(text))
    # 数字/./-/空白を落とした残りを単位とみなす
    s = re.sub(r"[\d\.\-\s]", "", s)
    return s.strip()


def most_common_unit(series: pd.Series) -> str:
    units = [extract_unit(x) for x in series if not pd.isna(x)]
    units = [u for u in units if u not in {"", "－", "-", "—", "―"}]
    if not units:
        return ""
    return Counter(units).most_common(1)[0][0]


def fmt_value_with_comp(value: float, unit: str, yoy: float, mom: float) -> str:
    # 例: 32尾（+76% / +27%）
    if pd.isna(value):
        return f"NA（{fmt_pct(yoy)} / {fmt_pct(mom)}）"
    v = int(round(value))
    return f"{v}{unit}（{fmt_pct(yoy)} / {fmt_pct(mom)}）"


def pick_sheet(xls: pd.ExcelFile) -> str:
    for s in SHEET_CANDIDATES:
        if s in xls.sheet_names:
            return s
    # 見つからなければ先頭
    return xls.sheet_names[0]


def main() -> None:
    if not SRC.exists():
        raise FileNotFoundError(f"Source file not found: {SRC}")

    xls = pd.ExcelFile(SRC)
    sheet = pick_sheet(xls)
    df = pd.read_excel(SRC, sheet_name=sheet)

    # 必須列（あなたの実データに合わせて確定）
    required = {"date", "fish_name", "area_port", "yado", "choka", "size"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in sheet '{sheet}': {missing}")

    # 日付整形
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date", "fish_name", "yado", "area_port"])

    # レンジ中央（代表値）
    df["choka_mean"] = mean_from_range_series(df["choka"])
    df["size_mean"] = mean_from_range_series(df["size"])

    # 「今日」= データ中の最新日
    today = df["date"].max()
    if pd.isna(today):
        raise ValueError("No valid dates found.")

    # 過去窓（今日を含めない）
    start_month = today - timedelta(days=WINDOW_MONTH)
    start_year = today - timedelta(days=WINDOW_YEAR)

    df_today = df[df["date"] == today].copy()
    df_month = df[(df["date"] < today) & (df["date"] >= start_month)].copy()
    df_year = df[(df["date"] < today) & (df["date"] >= start_year)].copy()

    # =========================
    # A) 主要指標テーブル（隻数順）
    # =========================
    # 今日の魚種別単位（表示用：魚種ごとに最頻単位）
    unit_today = (
        df_today.groupby("fish_name")
        .agg(
            catch_unit=("choka", most_common_unit),
            size_unit=("size", most_common_unit),
        )
        .reset_index()
    )

    # 今日の集計（隻数=records）
    g_today = (
        df_today.groupby("fish_name")
        .agg(
            records=("fish_name", "size"),
            choka_today=("choka_mean", "mean"),
            size_today=("size_mean", "mean"),
        )
        .reset_index()
    )

    # 過去平均
    g_month = (
        df_month.groupby("fish_name")
        .agg(choka_month=("choka_mean", "mean"), size_month=("size_mean", "mean"))
        .reset_index()
    )
    g_year = (
        df_year.groupby("fish_name")
        .agg(choka_year=("choka_mean", "mean"), size_year=("size_mean", "mean"))
        .reset_index()
    )

    out = (
        g_today.merge(g_month, on="fish_name", how="left")
        .merge(g_year, on="fish_name", how="left")
        .merge(unit_today, on="fish_name", how="left")
    )

    # 対比（％）
    out["choka_yoy"] = out.apply(lambda r: pct_vs(r["choka_today"], r["choka_year"]), axis=1)
    out["choka_mom"] = out.apply(lambda r: pct_vs(r["choka_today"], r["choka_month"]), axis=1)
    out["size_yoy"] = out.apply(lambda r: pct_vs(r["size_today"], r["size_year"]), axis=1)
    out["size_mom"] = out.apply(lambda r: pct_vs(r["size_today"], r["size_month"]), axis=1)

    # 表セル化（括弧で年/月）
    out["catch_cell"] = out.apply(
        lambda r: fmt_value_with_comp(r["choka_today"], r.get("catch_unit", "") or "", r["choka_yoy"], r["choka_mom"]),
        axis=1,
    )
    out["size_cell"] = out.apply(
        lambda r: fmt_value_with_comp(r["size_today"], r.get("size_unit", "") or "", r["size_yoy"], r["size_mom"]),
        axis=1,
    )

    table = out[["fish_name", "records", "catch_cell", "size_cell"]].copy()
    table = table.sort_values(["records", "fish_name"], ascending=[False, True])

    # =========================
    # B) サマリー：魚種内で突出した船 TOP5
    #    ルール：同魚種内で（平均との差%）が最大の船を魚種ごとに1件選び、
    #           それを突出率で並べて上位5件
    # =========================
    # 今日の魚種別平均（比較基準）
    fish_mean_today = df_today.groupby("fish_name")["choka_mean"].mean()

    df_top = df_today.copy()
    df_top["fish_mean"] = df_top["fish_name"].map(fish_mean_today)
    df_top["vs_others_pct"] = (df_top["choka_mean"] / df_top["fish_mean"] - 1.0) * 100.0

    # 魚種内比較が成立する魚種のみ
    fish_counts = df_today.groupby("fish_name")["fish_name"].size()
    valid_fish = fish_counts[fish_counts >= MIN_RECORDS_FOR_TOP5].index
    df_top = df_top[df_top["fish_name"].isin(valid_fish)]

    # 魚種ごとに “最も突出した1件” を抽出
    idx = df_top.groupby("fish_name")["vs_others_pct"].idxmax()
    df_top_best_each_fish = df_top.loc[idx].copy()

    # 全体で突出率順 TOP5
    df_top5 = df_top_best_each_fish.sort_values("vs_others_pct", ascending=False).head(5)

    # =========================
    # C) 履歴CSV（任意）
    # =========================
    # fish_name単位の日次集計を蓄積（将来の解析に便利）
    HISTORY_CSV.parent.mkdir(parents=True, exist_ok=True)
    daily_metrics = out[["fish_name", "records", "choka_today", "size_today"]].copy()
    daily_metrics.insert(0, "date", today)
    # 追記（同日があれば置換）
    if HISTORY_CSV.exists():
        hist = pd.read_csv(HISTORY_CSV)
        # date列を文字として扱い、同日削除→追記
        hist = hist[hist["date"] != str(today)]
        hist = pd.concat([hist, daily_metrics], ignore_index=True)
        hist.to_csv(HISTORY_CSV, index=False)
    else:
        daily_metrics.to_csv(HISTORY_CSV, index=False)

    # =========================
    # Markdown 出力
    # =========================
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)

    md = []
    md.append("# 📊 Daily Fish Report")
    md.append(f"**{today}**")
    md.append("")

    md.append("## 📝 サマリー")
    md.append("")
    md.append("### 🏆 今日の“魚種内突出船” TOP5")
    if len(df_top5) == 0:
        md.append("- （本日は比較可能な魚種（隻数>=3）がありませんでした）")
    else:
        for i, r in enumerate(df_top5.itertuples(index=False), start=1):
            # 表示：船（場所）— 魚種 釣果（他船対比 +○％）
            fish = r.fish_name
            boat = r.yado
            loc = r.area_port
            catch_unit = extract_unit(r.choka)
            catch_val = int(round(r.choka_mean)) if not pd.isna(r.choka_mean) else None
            pct = fmt_pct(r.vs_others_pct)
            if catch_val is None:
                md.append(f"{i}. **{boat}**（**{loc}**）— {fish} NA（他船対比 {pct}）")
            else:
                md.append(f"{i}. **{boat}**（**{loc}**）— {fish} **{catch_val}{catch_unit}（他船対比 {pct}）**")

    md.append("")
    md.append("## 📊 今日の主要指標（隻数順）")
    md.append("")
    md.append("| fish_name | 隻数 | 釣果（年 / 月） | サイズ（年 / 月） |")
    md.append("|---|---:|---|---|")
    for _, r in table.iterrows():
        md.append(f"| {r['fish_name']} | {int(r['records'])} | {r['catch_cell']} | {r['size_cell']} |")

    md.append("")
    md.append("<details>")
    md.append("<summary>📊 詳細（今日の全レコード）</summary>")
    md.append("")
    detail_cols = ["fish_name", "yado", "area_port", "choka", "size", "source", "url"]
    detail = df_today[detail_cols].copy()
    md.append(detail.to_markdown(index=False))
    md.append("")
    md.append("</details>")
    md.append("")
    md.append(f"Source: `{SRC}` / sheet: `{sheet}`")

    OUT_MD.write_text("\n".join(md), encoding="utf-8")
    print(f"Wrote: {OUT_MD}")


if __name__ == "__main__":
    main()
