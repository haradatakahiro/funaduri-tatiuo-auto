# -*- coding: utf-8 -*-
"""
funaduri.jp タチウオ釣果（fish.cgi?fish=tatiuo）の「今日の釣果」部分を抽出してExcelへ追記するスクリプト

前提：
- このページは <table> ではなく <div class="today"> 配下の <dl class="d0">...</dl> の繰り返し
- そのため pandas.read_html() は使わず BeautifulSoup で dl を解析する

出力：
- funaduri_tatiuo.xlsx（同フォルダ）/ シート名: tatiuo
- 日付 + 船宿 + 港 + 釣果 + サイズ + ポイント + 釣果元 をキーに重複排除して追記
"""

from __future__ import annotations

import re
import sys
import time
import datetime as dt
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

import pandas as pd


URL = "https://funaduri.jp/fish.cgi?fish=tatiuo"
OUT_XLSX = Path(__file__).with_name("funaduri_tatiuo.xlsx")
SHEET_NAME = "tatiuo"

# 取得が不安定なときのためのリトライ回数
RETRY = 3
TIMEOUT = 20


def jst_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9)))


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def get_text(el, sep=" "):
    if el is None:
        return ""
    return normalize_space(el.get_text(sep, strip=True))


def pick_first_a_text(el) -> str:
    """dd内の最初のaのテキスト（例：船宿名）"""
    if el is None:
        return ""
    a = el.find("a")
    return normalize_space(a.get_text(strip=True)) if a else normalize_space(el.get_text(" ", strip=True))


def fetch_html() -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    }
    last_err = None
    for i in range(1, RETRY + 1):
        try:
            r = requests.get(URL, headers=headers, timeout=TIMEOUT)
            r.raise_for_status()
            # サイト側は utf-8 指定（ソースのmeta）なので、requestsが誤判定しても強制utf-8に寄せる
            r.encoding = "utf-8"
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(1.5 * i)
    raise RuntimeError(f"取得に失敗: {last_err}")


def parse_today_records(html: str) -> Dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")

    # 今日の釣果の見出し（例： "2月21日、タチウオの釣果"）
    h2 = soup.find("h2", id="todaychoka")
    title = get_text(h2, sep=" ")
    if not title:
        raise RuntimeError("h2#todaychoka が見つかりませんでした（ページ構造が変わった可能性）")

    # "2月21日、タチウオの釣果" から日付部分（2月21日）を抜く
    m = re.search(r"(\d{1,2})月(\d{1,2})日", title)
    if not m:
        raise RuntimeError(f"日付が見出しから抽出できませんでした: {title}")

    month = int(m.group(1))
    day = int(m.group(2))

    # 年は「今の年」を基本に、もし未来日になったら前年扱い（年末年始対策）
    now = jst_now()
    year = now.year
    candidate = dt.date(year, month, day)
    if candidate > now.date() and (candidate - now.date()).days > 3:
        year -= 1

    record_date = dt.date(year, month, day).isoformat()

    today_div = soup.find("div", class_="today")
    if today_div is None:
        raise RuntimeError("div.today が見つかりませんでした（ページ構造が変わった可能性）")

    # データ行は dl.d0 / dl.d1 ... の繰り返し（ソース.txtでも dl class="d0" を確認）:
    # 例： <dl class="d0" ...> ... <dd class="t1">...</dd> ... <dd class="t2">...</dd> ...
    dls = today_div.find_all("dl")
    if not dls:
        raise RuntimeError("div.today 配下に dl が見つかりませんでした")

    rows: List[Dict[str, str]] = []
    for dl in dls:
        cls = dl.get("class", [])
        # tbtop は div なので dl には来ないはずだが、念のため
        if not cls:
            continue

        # 主要セル（classで拾う）
        dd_t1 = dl.find("dd", class_="t1")  # 地域/港
        dd_t2 = dl.find("dd", class_="t2")  # 船宿
        dd_t3 = dl.find("dd", class_="t3")  # 釣果
        dd_t4 = dl.find("dd", class_="t4")  # サイズ
        dd_t5 = dl.find("dd", class_="t5")  # ポイント
        dd_t6 = dl.find("dd", class_="t6")  # 釣果元
        dd_eval = dl.find("dd", class_="yeval")  # 評価

        # 地域/港は <a>が2つ + <br> なので sep=" / " にすると見やすい
        area_port = get_text(dd_t1, sep=" / ")

        # 船宿名（最初のa）
        yado = pick_first_a_text(dd_t2)

        # 各項目（無い場合は空欄）
        choka = get_text(dd_t3)
        size = get_text(dd_t4)
        point = get_text(dd_t5)
        source = get_text(dd_t6)

        # 評価（"4.3/4.4/4.6" みたいな表現を作る：ソースでは span.yerslt 内に数値がある） :contentReference[oaicite:1]{index=1}
        eval_text = ""
        if dd_eval:
            # 数値っぽいものだけ拾う
            nums = re.findall(r"\d+(?:\.\d+)?", dd_eval.get_text(" ", strip=True))
            if nums:
                # だいたい3つ（ホス/設/コンプライアンス）
                eval_text = "/".join(nums[:3])

        # 主要値が全部空ならスキップ
        if not any([area_port, yado, choka, size, point, source, eval_text]):
            continue

        rows.append(
            {
                "date": record_date,
                "area_port": area_port,
                "yado": yado,
                "choka": choka,
                "size": size,
                "point": point,
                "source": source,
                "eval": eval_text,
                "fetched_at_jst": now.isoformat(timespec="seconds"),
                "url": URL,
            }
        )

    if not rows:
        raise RuntimeError("釣果レコードが抽出できませんでした（dlの中身が想定と違う可能性）")

    return {
        "title": title,
        "date": record_date,
        "rows": rows,
    }


def load_existing(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(path, sheet_name=SHEET_NAME, dtype=str)
    except Exception:
        # シートが無い/壊れてる等
        return pd.DataFrame()


def save_append_dedup(new_df: pd.DataFrame, path: Path) -> pd.DataFrame:
    old_df = load_existing(path)

    # 文字列化（比較のブレ防止）
    for c in new_df.columns:
        new_df[c] = new_df[c].astype(str)

    if old_df.empty:
        merged = new_df.copy()
    else:
        for c in new_df.columns:
            if c not in old_df.columns:
                old_df[c] = ""
        for c in old_df.columns:
            if c not in new_df.columns:
                new_df[c] = ""

        merged = pd.concat([old_df, new_df], ignore_index=True)

    # 重複判定キー：日付 + 船宿 + 地域港 + 釣果 + サイズ + ポイント + 釣果元
    key_cols = ["date", "yado", "area_port", "choka", "size", "point", "source"]
    for c in key_cols:
        if c not in merged.columns:
            merged[c] = ""

    merged = merged.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)

    # 保存（openpyxl を使って上書き）
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as w:
        merged.to_excel(w, sheet_name=SHEET_NAME, index=False)

    return merged


def main():
    html = fetch_html()
    parsed = parse_today_records(html)

    new_df = pd.DataFrame(parsed["rows"])
    merged = save_append_dedup(new_df, OUT_XLSX)

    print(f"OK: {parsed['title']}")
    print(f"  抽出: {len(new_df)} 行 / 既存込み合計: {len(merged)} 行")
    print(f"  保存先: {OUT_XLSX}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # 失敗時はHTMLを保存して原因調査できるようにする
        debug_dir = Path(__file__).with_name("debug_funaduri")
        debug_dir.mkdir(exist_ok=True)
        ts = jst_now().strftime("%Y%m%d_%H%M%S")
        try:
            html = fetch_html()
            (debug_dir / f"error_{ts}.html").write_text(html, encoding="utf-8")
            print(f"debug保存: {debug_dir / f'error_{ts}.html'}")
        except Exception:
            pass

        print(f"失敗: {e}", file=sys.stderr)
        sys.exit(1)