# allfish_data_import.py
from __future__ import annotations

import re
import time
import hashlib
import datetime as dt
from pathlib import Path
from typing import Dict, List
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
import pandas as pd


BASE_URL = "https://funaduri.jp/"
ALL_FISH_URL = urljoin(BASE_URL, "fish.cgi?fish=all")

RETRY = 3
TIMEOUT = 20

# 魚種ごとのアクセス間隔（サイト負荷軽減）
SLEEP_BETWEEN_FISH_SEC = 0.25

DATA_DIR = Path("data")
HASH_FILE = DATA_DIR / "funaduri_allfish_last_hash.txt"


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


def fetch_html(url: str) -> str:
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

    raise RuntimeError(f"取得に失敗: {url} / {last_err}")


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


def list_fishes_from_all(html: str) -> List[Dict[str, str]]:
    """
    fish.cgi?fish=all から fish_code / fish_name / fish_url を抽出する。
    - #alist（最近釣果あり）と #nonalist（最近釣果なし）の両方を見る
    """
    soup = BeautifulSoup(html, "html.parser")
    fishes: List[Dict[str, str]] = []

    for ul_id in ("alist", "nonalist"):
        ul = soup.find("ul", id=ul_id)
        if not ul:
            continue

        for a in ul.find_all("a", href=True):
            href = a["href"]
            abs_url = urljoin(BASE_URL, href)

            qs = parse_qs(urlparse(abs_url).query)
            code = (qs.get("fish") or [""])[0].strip()
            if not code or code == "all":
                continue

            # 表示名：<b>があれば優先
            b = a.find("b")
            name_raw = normalize_space(b.get_text(strip=True)) if b else normalize_space(a.get_text(" ", strip=True))

            # 補足が混じることがあるので先頭トークンに寄せる
            fish_name = name_raw.split()[0] if name_raw else ""

            fishes.append(
                {
                    "fish_code": code,
                    "fish_name": fish_name,
                    "fish_url": urljoin(BASE_URL, f"fish.cgi?fish={code}"),
                }
            )

    # fish_code 重複除去
    seen = set()
    uniq: List[Dict[str, str]] = []
    for f in fishes:
        if f["fish_code"] in seen:
            continue
        seen.add(f["fish_code"])
        uniq.append(f)

    if not uniq:
        raise RuntimeError("fish=all から魚種リストが取得できませんでした（ページ構造が想定と違う可能性）")
    return uniq


def parse_today_records(
    html: str,
    *,
    fish_code: str,
    fish_name: str,
    page_url: str,
) -> Dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")

    # 今日の釣果の見出し
    h2 = soup.find("h2", id="todaychoka")
    title = get_text(h2, sep=" ")
    if not title:
        raise RuntimeError("h2#todaychoka が見つかりませんでした（今日の釣果が無い or 構造変更）")

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
                "fish_code": fish_code,
                "fish_name": fish_name,
                "area_port": area_port,
                "yado": yado,
                "choka": choka,
                "size": size,
                "point": point,
                "source": source,
                "eval": eval_text,
                "fetched_at_jst": now.isoformat(timespec="seconds"),
                "url": page_url,
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
    key_cols = [
        "date",
        "fish_code",
        "fish_name",
        "yado",
        "area_port",
        "choka",
        "size",
        "point",
        "source",
        "eval",
    ]
    tmp = df.copy()
    for c in key_cols:
        if c not in tmp.columns:
            tmp[c] = ""
    tmp = tmp[key_cols].astype(str)

    # ソートして順序ゆらぎを抑える（魚種巡回順が変わっても同一判定できる）
    tmp = tmp.sort_values(by=key_cols, kind="mergesort").reset_index(drop=True)

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


def fetch_allfish_df(use_hash: bool = False) -> pd.DataFrame:
    """
    run_daily.py などから呼ぶ用。
    全魚種の「今日の釣果」を1つのDataFrame（縦持ち）で返す。

    use_hash=True:
      - 前回と同じ内容なら空DataFrameを返す（更新なし）
      - 更新ありなら hash を data/ に保存
    """
    all_html = fetch_html(ALL_FISH_URL)
    fishes = list_fishes_from_all(all_html)

    all_rows: List[Dict[str, str]] = []

    for f in fishes:
        code = f["fish_code"]
        name = f["fish_name"]
        url = f["fish_url"]

        try:
            html = fetch_html(url)
            parsed = parse_today_records(html, fish_code=code, fish_name=name, page_url=url)
            all_rows.extend(parsed["rows"])
        except Exception:
            # 「今日の釣果が無い」「構造違い」などはスキップして継続
            pass

        time.sleep(SLEEP_BETWEEN_FISH_SEC)

    df = pd.DataFrame(all_rows)

    # 列の順序をできるだけ安定化（空でも列を作る）
    cols = [
        "date",
        "fish_code",
        "fish_name",
        "area_port",
        "yado",
        "choka",
        "size",
        "point",
        "source",
        "eval",
        "fetched_at_jst",
        "url",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]

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
    df = fetch_allfish_df(use_hash=False)
    print(df.head())
    print(f"rows={len(df)}")


if __name__ == "__main__":
    main()
