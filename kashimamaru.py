# kashimamaru.py
from __future__ import annotations

import hashlib
from io import StringIO
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests
import pandas as pd


URL = "https://www.aqualine.ne.jp/~kashimamaru/choka.cgi"

# GitHub Actionsでもローカルでも同じ場所に保存できるように、相対パスにする
DATA_DIR = Path("data")
HASH_FILE = DATA_DIR / "kashimamaru_last_hash.txt"

JST = timezone(timedelta(hours=9))


def fetch_html(url: str = URL, timeout: int = 30) -> str:
    """
    HTMLを取得して文字コードcp932(Shift_JIS系)として返す
    """
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    r.encoding = "cp932"
    return r.text


def pick_table(html: str) -> pd.DataFrame:
    """
    HTML中の表から、必要列を持つ表を1つ選んで返す
    """
    tables = pd.read_html(StringIO(html))

    required = ["日付", "釣り物", "数量", "型", "場所", "備考"]

    for t in tables:
        # 列名の前後空白を念のため除去
        t.columns = [str(c).strip() for c in t.columns]
        if all(col in t.columns for col in required):
            # 必要列だけ・順序も揃える（余計な列があってもOK）
            return t[required].copy()

    raise ValueError("対象の列を持つ表が見つかりません。")


def calc_hash(df: pd.DataFrame) -> str:
    """
    DataFrame内容のハッシュ（同じ表なら同じ値）
    """
    b = df.to_csv(index=False).encode("utf-8")
    return hashlib.md5(b).hexdigest()


def read_old_hash(path: Path = HASH_FILE) -> str | None:
    if path.exists():
        s = path.read_text(encoding="utf-8").strip()
        return s or None
    return None


def write_hash(h: str, path: Path = HASH_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(h, encoding="utf-8")


def fetch_kashimamaru_df(use_hash: bool = False) -> pd.DataFrame:
    """
    run_daily.py から呼ぶ用。
    釣果表を DataFrame で返す。

    use_hash=True の場合：
      - 前回と同じなら空DataFrameを返す（更新なし扱いにできる）
      - 更新ありなら hash を保存する
    """
    html = fetch_html(URL)
    df = pick_table(html)

    # 取得日時（JST）
    fetched_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    df.insert(0, "取得日時", fetched_at)

    if not use_hash:
        return df

    new_hash = calc_hash(df.drop(columns=["取得日時"], errors="ignore"))
    old_hash = read_old_hash(HASH_FILE)

    if new_hash == old_hash:
        # 更新なし → 空で返す
        return pd.DataFrame(columns=df.columns)

    write_hash(new_hash, HASH_FILE)
    return df


def main():
    """
    単体実行したい時用（ローカル動作確認）
    """
    df = fetch_kashimamaru_df(use_hash=False)
    print(df.head())
    print(f"rows={len(df)}")


if __name__ == "__main__":
    main()
