# tachi_turi_data_import.py
from __future__ import annotations

import re
import time
import hashlib
import datetime as dt
from pathlib import Path
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
import pandas as pd


URL = "https://funaduri.jp/fish.cgi?fish=tatiuo"

RETRY = 3
TIMEOUT = 20

DATA_DIR = Path("data")
HASH_FILE = DATA_DIR / "funaduri_tatiuo_last_hash.txt"


def jst_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9)))


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def get_text(el, sep: str = " ") -> str:
    if el is None:
        return ""
    return normalize_space(el.get_text(sep, strip=True))


def pick_first_a_text(el) -> str:
    """dd内の最初のaのテキスト（例：船宿名）"""
    if el is None:
        return ""
    a = el.find("a")
    return normalize_space(a.get_text(strip=True)) if a else normalize_space(el.get_text(" ", strip=True))


def fetch_html(url: str = URL) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    }

    last_err = None
    for i in range(1, RETRY + 1):
        try:
            r = requests.get(url, headers=headers, timeout=TIMEOUT)
            r.raise_for_status()
            r.encoding = "utf-8"  # metaに合わせて固定
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(1.5 * i)

    raise RuntimeError(f"取得に失敗: {last_err}")


def _guess_record_date_from_title(title: str, now: dt.datetime) -> str:
    """
    例: "2月21日、タチウオの釣果" から YYYY-MM-DD を作る
    年末年始で未来日になった場合は前年扱い
    """
    m = re.search(r"(\d{1,2})月(\d{1,2})日", title)
    if not m:
        raise RuntimeError(f"日付が見出しから抽出できませんでした: {title}")

    month = int(m.group(1))
    day = int(m.group(2))

    year = now.year
    candidate = dt.date(year, month, day)
    if candidate > now.date() and (candidate - now.date()).days > 3:
        year -= 1

    return dt.date(year, month, day).isoformat()


def parse_today_records(html: str) -> Dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")

    # 今日の釣果の見出し
    h2 = soup.find("h2", id="todaychoka")
    title = get_text(h2, sep=" ")
    if not title:
        raise RuntimeError("h2#todaychoka が見つかりませんでした（ページ構造が変わった可能性）")

    now = jst_now()
    record_date = _guess_record_date_from_title(title, now)

    today_div = soup.find("div", class_="today")
    if today_div is None:
        raise RuntimeError("div.today が見つかりませんでした（ページ構造が変わった可能性）")

    dls = today_div.find_all("dl")
    if not dls:
        raise RuntimeError("div.today 配下に dl が見つかりませんでした")

    rows: List[Dict[str, str]] = []

    for dl in dls:
        # 主要セル（classで拾う）
        dd_t1 = dl.find("dd", class_="t1")  # 地域/港
        dd_t2 = dl.find("dd", class_="t2")  # 船宿
        dd_t3 = dl.find("dd", class_="t3")  # 釣果
        dd_t4 = dl.find("dd", class_="t4")  # サイズ
        dd_t5 = dl.find("dd", class_="t5")  # ポイント
        dd_t6 = dl.find("dd", class_="t6")  # 釣果元
        dd_eval = dl.find("dd", class_="yeval")  # 評価

        area_port = get_text(dd_t1, sep=" / ")
        yado = pick_first_a_text(dd_t2)

        choka = get_text(dd_t3)
        size = get_text(dd_t4)
        point = get_text(dd_t5)
        source = get_text(dd_t6)

        eval_text = ""
        if dd_eval:
            nums = re.findall(r"\d+(?:\.\d+)?", dd_eval.get_text(" ", strip=True))
            if nums:
                eval_text = "/".join(nums[:3])

        # 全部空ならスキップ
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

    return {"title": title, "date": record_date, "rows": rows}


def _calc_hash_for_rows(df: pd.DataFrame) -> str:
    """
    「取得日時」等の揺れを除いて、内容ベースでhash化したいので、
    主要列だけでhashを作る（列が無いときは空扱い）
    """
    key_cols = ["date", "yado", "area_port", "choka", "size", "point", "source", "eval"]
    tmp = df.copy()
    for c in key_cols:
        if c not in tmp.columns:
            tmp[c] = ""
    tmp = tmp[key_cols].astype(str)
    b = tmp.to_csv(index=False).encode("utf-8")
    return hashlib.md5(b).hexdigest()


def _read_old_hash(path: Path = HASH_FILE) -> str | None:
    if path.exists():
        s = path.read_text(encoding="utf-8").strip()
        return s or None
    return None


def _write_hash(h: str, path: Path = HASH_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(h, encoding="utf-8")


def fetch_tatiuo_df(use_hash: bool = False) -> pd.DataFrame:
    """
    run_daily.py から呼ぶ用。
    今日の釣果を DataFrame で返す。

    use_hash=True:
      - 前回と同じ内容なら空DataFrameを返す（更新なし）
      - 更新ありなら hash を data/ に保存
    """
    html = fetch_html(URL)
    parsed = parse_today_records(html)
    df = pd.DataFrame(parsed["rows"])

    # 文字列化（比較/保存のブレ防止）
    for c in df.columns:
        df[c] = df[c].astype(str)

    if not use_hash:
        return df

    new_hash = _calc_hash_for_rows(df)
    old_hash = _read_old_hash(HASH_FILE)

    if new_hash == old_hash:
        return pd.DataFrame(columns=df.columns)

    _write_hash(new_hash, HASH_FILE)
    return df


def main():
    df = fetch_tatiuo_df(use_hash=False)
    print(df.head())
    print(f"rows={len(df)}")


if __name__ == "__main__":
    main()
