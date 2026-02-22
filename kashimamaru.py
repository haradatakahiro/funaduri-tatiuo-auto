import os
import hashlib
from datetime import datetime
import requests
import pandas as pd
from io import StringIO

URL = "https://www.aqualine.ne.jp/~kashimamaru/choka.cgi"
SAVE_DIR = r"C:\Users\kenta\OneDrive\デスクトップ\タチウオ研究関連"
EXCEL_FILE = os.path.join(SAVE_DIR, "kashimamaru_choka_log.xlsx")
HASH_FILE = os.path.join(SAVE_DIR, "last_hash.txt")

def fetch_html(url: str) -> str:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    r.encoding = "cp932"  # Shift_JIS系
    return r.text

def pick_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html))
    for table in tables:
        if all(col in table.columns for col in ["日付", "釣り物", "数量", "型", "場所", "備考"]):
            return table
    raise ValueError("対象の列を持つ表が見つかりません。")

def calc_hash(df: pd.DataFrame) -> str:
    return hashlib.md5(df.to_csv(index=False).encode("utf-8")).hexdigest()

def read_old_hash(path: str) -> str | None:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip() or None
    return None

def write_hash(path: str, h: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(h)

def main():
    os.makedirs(SAVE_DIR, exist_ok=True)
    html = fetch_html(URL)
    df = pick_table(html)

    new_hash = calc_hash(df)
    old_hash = read_old_hash(HASH_FILE)

    if new_hash == old_hash:
        print("更新なし：保存しませんでした。")
        return

    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.insert(0, "取得日時", fetched_at)

    if os.path.exists(EXCEL_FILE):
        old_df = pd.read_excel(EXCEL_FILE)
        out_df = pd.concat([old_df, df], ignore_index=True)
    else:
        out_df = df

    out_df.to_excel(EXCEL_FILE, index=False)
    write_hash(HASH_FILE, new_hash)

    print("更新あり：Excelに追記しました。")
    print(f"保存先: {EXCEL_FILE}")

if __name__ == "__main__":
    main()