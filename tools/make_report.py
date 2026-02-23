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
SHEET_KASHI = "kashimamaru"                # 鹿島丸シート名（run_daily.py で作成）
OUT_MD = Path("reports/report.md")

# 任意：日次集計をためる（長期運用で便利）
HISTORY_CSV = Path("history/daily_metrics.csv")

WINDOW_MONTH = 30
WINDOW_YEAR = 365

# TOP5の対象にする最小隻数（魚種内比較が成立する範囲）
MIN_RECORDS_FOR_TOP5 = 3

# 「タチウオ」とみなす魚種名（必要なら追加）
TACHIUO_NAMES = {"タチウオ", "太刀魚"}


# =========================================================
# X表記の扱い（仕様）
# - 1桁の "X" は 0 扱い（保守的）
# - 2桁の "1X" は 15（10〜19の中央値）, "2X" は 25 ...
# - 小数の "0.4X" は 0.45（0.40〜0.49の中央値）
# - レンジ "0-1X" は両端を上のルールで数値化して (min,max) を作る
# - 表示は「中央値（(min+max)/2）」の単値を基本にする
# =========================================================

def _x_token_to_value(token: str) -> float | None:
    """
    token: 'X', '1X', '2X', '0.4X', '12', '3.5' など
    返り値: 数値化できればfloat、できなければNone
    """
    t = token.strip()
    if not t:
        return None

    # 小数+X：例 0.4X -> 0.45
    m = re.fullmatch(r"(\d+)\.(\d)X", t)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}5")

    # 1桁 X：例 X -> 0
    if t == "X":
        return 0.0

    # 2桁 X：例 1X -> 15, 2X -> 25
    m = re.fullmatch(r"(\d)X", t)
    if m:
        return float(int(m.group(1)) * 10 + 5)

    # 通常の数値
    m = re.fullmatch(r"\d+(?:\.\d+)?", t)
    if m:
        return float(t)

    return None


def _pre_normalize(s: str) -> str:
    """
    文字のゆれを軽く統一（レンジ記号だけ）。
    Xは意味があるので消さない。
    """
    s = s.strip()
    s = s.replace("－", "-").replace("—", "-").replace("―", "-")
    s = s.replace("〜", "-").replace("～", "-")
    return s


def parse_range(val) -> tuple[float, float]:
    """ '18-40 尾' / '45cm' / 'X-4 杯' / '0-1X 本' / '0.4Xkg' などを (min, max) に """
    if pd.isna(val):
        return (np.nan, np.nan)

    s = str(val).strip()
    if s in {"", "－", "-", "—", "―"}:
        return (np.nan, np.nan)

    s = _pre_normalize(s)

    # レンジ（A-B）をまず探す（単位は後ろに付くので無視）
    # A/B は 'X', '1X', '0.4X', '12', '3.5' などを許容
    m = re.search(
        r"(?P<a>(?:\d+\.\dX)|(?:\dX)|X|(?:\d+(?:\.\d+)?))\s*-\s*(?P<b>(?:\d+\.\dX)|(?:\dX)|X|(?:\d+(?:\.\d+)?))",
        s,
    )
    if m:
        a = _x_token_to_value(m.group("a"))
        b = _x_token_to_value(m.group("b"))
        if a is None and b is None:
            return (np.nan, np.nan)
        if a is None:
            return (b, b)
        if b is None:
            return (a, a)
        return (min(a, b), max(a, b))

    # 単値（最初に見つかった数値トークンを使う）
    m = re.search(r"(\d+\.\dX|\dX|X|\d+(?:\.\d+)?)", s)
    if m:
        v = _x_token_to_value(m.group(1))
        if v is None:
            return (np.nan, np.nan)
        return (v, v)

    return (np.nan, np.nan)


def mean_from_range_series(series: pd.Series) -> pd.Series:
    mm = series.apply(lambda x: pd.Series(parse_range(x), index=["min", "max"]))
    # min,max がどっちか欠けても平均が取れるように（片方だけならその値）
    return mm[["min", "max"]].mean(axis=1, skipna=True)


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
    s = _pre_normalize(str(text))
    # 数字/./-/X/空白を落とした残りを単位とみなす（Xは数値側の記号なので落とす）
    s = re.sub(r"[\d\.\-\sX]", "", s)
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
    return xls.sheet_names[0]


def normalize_kashimamaru(df_k: pd.DataFrame) -> pd.DataFrame:
    """
    kashimamaru シートの列（例: 日付, 釣り物, 数量, 型, 場所, 備考）を
    all_fish 互換（date, fish_name, area_port, yado, choka, size, source, url）に寄せる
    """
    if df_k is None or df_k.empty:
        return pd.DataFrame(columns=["date", "fish_name", "area_port", "yado", "choka", "size", "source", "url"])

    # 列名ゆれ対策（最低限）
    colmap = {
        "日付": "date",
        "釣り物": "fish_name",
        "数量": "choka",
        "型": "size",
        "場所": "area_port",
        "備考": "note",
    }
    df = df_k.rename(columns={k: v for k, v in colmap.items() if k in df_k.columns}).copy()

    # 必須の欠けは空で作る
    for c in ["date", "fish_name", "choka", "size", "area_port"]:
        if c not in df.columns:
            df[c] = np.nan

    df["yado"] = "鹿島丸"
    df["source"] = "kashimamaru"

    # URLは固定で入れておく（リンク不要なら空でもOK）
    # run_daily 側で保存していない可能性があるのでここで付与
    df["url"] = "https://www.aqualine.jp/kashimamaru/"  # 変更したければここだけ

    # 形式を all_fish と揃える
    keep = ["date", "fish_name", "area_port", "yado", "choka", "size", "source", "url"]
    df = df[keep].copy()

    # area_port は funaduri が「地域 / 港」形式なので、鹿島丸は「鹿島丸 / 場所」に寄せる
    df["area_port"] = df["area_port"].astype(str).where(df["area_port"].notna(), "")
    df["area_port"] = df["area_port"].apply(lambda x: f"鹿島丸 / {x}".strip(" /") if x and x != "nan" else "鹿島丸")

    return df


def is_tachiuo_fishname(name: str) -> bool:
    if not isinstance(name, str):
        return False
    n = name.strip()
    return n in TACHIUO_NAMES or ("タチウオ" in n) or ("太刀魚" in n)


def main() -> None:
    if not SRC.exists():
        raise FileNotFoundError(f"Source file not found: {SRC}")

    xls = pd.ExcelFile(SRC)

    # ===== all_fish 読み込み =====
    sheet = pick_sheet(xls)
    df = pd.read_excel(SRC, sheet_name=sheet)

    # 必須列（あなたの実データに合わせて確定）
    required = {"date", "fish_name", "area_port", "yado", "choka", "size"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in sheet '{sheet}': {missing}")

    # ===== kashimamaru 読み込み（あれば混ぜる） =====
    if SHEET_KASHI in xls.sheet_names:
        df_k_raw = pd.read_excel(SRC, sheet_name=SHEET_KASHI)
        df_k = normalize_kashimamaru(df_k_raw)
        # all_fish と同じ列へ（存在しない列は追加）
        for c in ["source", "url"]:
            if c not in df.columns:
                df[c] = ""
        df = df[["date", "fish_name", "area_port", "yado", "choka", "size", "source", "url"]].copy()
        df = pd.concat([df, df_k], ignore_index=True)
    else:
        # ない場合でも detail 出力列が揃うように
        if "source" not in df.columns:
            df["source"] = ""
        if "url" not in df.columns:
            df["url"] = ""

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
    unit_today = (
        df_today.groupby("fish_name")
        .agg(
            catch_unit=("choka", most_common_unit),
            size_unit=("size", most_common_unit),
        )
        .reset_index()
    )

    g_today = (
        df_today.groupby("fish_name")
        .agg(
            records=("fish_name", "size"),
            choka_today=("choka_mean", "mean"),
            size_today=("size_mean", "mean"),
        )
        .reset_index()
    )

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

    out["choka_yoy"] = out.apply(lambda r: pct_vs(r["choka_today"], r["choka_year"]), axis=1)
    out["choka_mom"] = out.apply(lambda r: pct_vs(r["choka_today"], r["choka_month"]), axis=1)
    out["size_yoy"] = out.apply(lambda r: pct_vs(r["size_today"], r["size_year"]), axis=1)
    out["size_mom"] = out.apply(lambda r: pct_vs(r["size_today"], r["size_month"]), axis=1)

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
    # =========================
    fish_mean_today = df_today.groupby("fish_name")["choka_mean"].mean()

    df_top = df_today.copy()
    df_top["fish_mean"] = df_top["fish_name"].map(fish_mean_today)
    df_top["vs_others_pct"] = (df_top["choka_mean"] / df_top["fish_mean"] - 1.0) * 100.0

    fish_counts = df_today.groupby("fish_name")["fish_name"].size()
    valid_fish = fish_counts[fish_counts >= MIN_RECORDS_FOR_TOP5].index
    df_top = df_top[df_top["fish_name"].isin(valid_fish)]

    idx = df_top.groupby("fish_name")["vs_others_pct"].idxmax()
    df_top_best_each_fish = df_top.loc[idx].copy()

    df_top5 = df_top_best_each_fish.sort_values("vs_others_pct", ascending=False).head(5)

    # =========================
    # B-2) タチウオ限定ランキング TOP5（鹿島丸含む）
    # =========================
    df_tachiuo = df_today[df_today["fish_name"].apply(is_tachiuo_fishname)].copy()
    if not df_tachiuo.empty:
        t_mean = df_tachiuo["choka_mean"].mean()
        df_tachiuo["fish_mean"] = t_mean
        df_tachiuo["vs_others_pct"] = (df_tachiuo["choka_mean"] / df_tachiuo["fish_mean"] - 1.0) * 100.0
        df_tachiuo_top5 = df_tachiuo.sort_values("choka_mean", ascending=False).head(5)
    else:
        df_tachiuo_top5 = df_tachiuo

    # =========================
    # C) 履歴CSV（任意）
    # =========================
    HISTORY_CSV.parent.mkdir(parents=True, exist_ok=True)
    daily_metrics = out[["fish_name", "records", "choka_today", "size_today"]].copy()
    daily_metrics.insert(0, "date", today)
    if HISTORY_CSV.exists():
        hist = pd.read_csv(HISTORY_CSV)
        hist = hist[hist["date"] != str(today)]
        hist = pd.concat([hist, daily_metrics], ignore_index=True)
        hist.to_csv(HISTORY_CSV, index=False)
    else:
        daily_metrics.to_csv(HISTORY_CSV, index=False)

    # =========================
    # Markdown 出力
    # =========================
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)

    md: list[str] = []
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
    md.append("## ⚔ タチウオ限定ランキング")
    md.append("")
    md.append("### 🥇 今日のタチウオ船 TOP5（中央値）")
    if df_tachiuo_top5 is None or len(df_tachiuo_top5) == 0:
        md.append("- （本日はタチウオのレコードがありませんでした）")
    else:
        # 参考：他船対比は「タチウオの平均」に対する比
        if df_tachiuo["fish_mean"].notna().any() if not df_tachiuo.empty else False:
            t_mean_val = df_tachiuo["fish_mean"].iloc[0]
        else:
            t_mean_val = np.nan

        for i, r in enumerate(df_tachiuo_top5.itertuples(index=False), start=1):
            boat = r.yado
            loc = r.area_port
            unit = extract_unit(r.choka)
            val = int(round(r.choka_mean)) if not pd.isna(r.choka_mean) else None
            pct = fmt_pct((r.choka_mean / t_mean_val - 1.0) * 100.0) if (val is not None and not pd.isna(t_mean_val) and t_mean_val != 0) else "NA"
            if val is None:
                md.append(f"{i}. **{boat}**（**{loc}**）— NA（他船対比 {pct}）")
            else:
                md.append(f"{i}. **{boat}**（**{loc}**）— **{val}{unit}（他船対比 {pct}）**")

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
    for c in detail_cols:
        if c not in df_today.columns:
            df_today[c] = ""
    detail = df_today[detail_cols].copy()
    md.append(detail.to_markdown(index=False))
    md.append("")
    md.append("</details>")
    md.append("")
    md.append(f"Source: `{SRC}` / sheet: `{sheet}`" + (f" + `{SHEET_KASHI}`" if SHEET_KASHI in xls.sheet_names else ""))

    OUT_MD.write_text("\n".join(md), encoding="utf-8")
    print(f"Wrote: {OUT_MD}")


if __name__ == "__main__":
    main()
