"""
arawkawa_hirano_data.py（対処版：UA強化 + リトライ）

出力:
- data/reservation_log.xlsx（sheet: log）

ポイント:
- GitHub Actions など “サーバーからのアクセス” を弾くサイトがある
  → まず User-Agent/Accept/Lang をブラウザ相当にする（403対策の第一手）
- 一時的な通信失敗がある
  → Retry（自動リトライ）を導入

依存:
- pandas, requests, beautifulsoup4, pillow, openpyxl
- （任意）numpy, opencv-python  : 座席図から人数推定したい場合
"""

from __future__ import annotations

import hashlib
import io
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup
from PIL import Image, ImageOps

# ---- requests retry ----
from requests.adapters import HTTPAdapter
try:
    # urllib3 v2
    from urllib3.util.retry import Retry
except Exception:
    # older
    from urllib3 import Retry  # type: ignore

# ===== OpenCV（人数カウント：任意）=====
try:
    import cv2  # type: ignore
    import numpy as np  # type: ignore

    OPENCV_OK = True
except Exception:
    OPENCV_OK = False


# =====================
# 共通設定（GitHub運用向け：相対パス）
# =====================
JST = timezone(timedelta(hours=9))

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(REPO_ROOT, "data")
OUT_XLSX = os.path.join(DATA_DIR, "reservation_log.xlsx")
SHEET_ALL = "log"

ASSETS_DIR = os.path.join(REPO_ROOT, "assets")
TEMPLATE_PATH = os.path.join(ASSETS_DIR, "seat_template.png")  # 任意（無くてもOK）

TIMEOUT = 25
SLEEP_BETWEEN_REQUEST_SEC = 0.6


# =====================
# ★ 403対策：ブラウザっぽいヘッダ
# =====================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


# =====================
# URL（対象サイト）
# ※ 必要に応じてあなたのサイト実URLに合わせる
# =====================
ARAKAWA_URL = "https://www.arakawaya.jp/"
HIRANO_BASE_URL = "https://hiranomaru.net/"

# “もっと見る” がある場合の保険（サイトが違えば効かないので失敗してOK）
HIRANO_MORE_API_PATH = "/news/ajax/"
HIRANO_MAX_PAGES = 30


# =====================
# session（リトライ設定）
# =====================
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


# =====================
# ユーティリティ
# =====================
def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(ASSETS_DIR, exist_ok=True)


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def normalize_digits(s: str) -> str:
    if not isinstance(s, str):
        return ""
    trans = str.maketrans("０１２３４５６７８９", "0123456789")
    return s.translate(trans)


def normalize_space(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return re.sub(r"\s+", " ", s).strip()


def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def url_normalize(u: str) -> str:
    if not u:
        return ""
    sp = urlsplit(u)
    q = parse_qsl(sp.query, keep_blank_values=True)
    q_sorted = sorted([(k, v) for k, v in q])
    query = urlencode(q_sorted)
    path = sp.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return urlunsplit((sp.scheme, sp.netloc, path, query, ""))


def abs_url(base: str, href: str) -> str:
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        sp = urlsplit(base)
        return f"{sp.scheme}:{href}"
    if href.startswith("/"):
        sp = urlsplit(base)
        return f"{sp.scheme}://{sp.netloc}{href}"
    if base.endswith("/"):
        return base + href
    return base + "/" + href


def fetch_text(session: requests.Session, url: str, params: Optional[dict] = None, referer: Optional[str] = None) -> str:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer
    r = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
    # Retryはraise_on_status=Falseなので、ここで明示的にエラーを分かるようにする
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.reason} for url: {url}", response=r)
    r.encoding = r.apparent_encoding
    return r.text


def fetch_bytes(session: requests.Session, url: str, referer: Optional[str] = None, max_bytes: int = 1_200_000) -> bytes:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer
    r = session.get(url, headers=headers, timeout=TIMEOUT, stream=True)
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.reason} for url: {url}", response=r)
    data = r.raw.read(max_bytes)
    return data


def load_log(path: str, sheet: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_excel(path, sheet_name=sheet, dtype=str)
    except Exception:
        return pd.DataFrame()


def save_log(path: str, sheet: str, df: pd.DataFrame) -> None:
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as w:
        df.to_excel(w, sheet_name=sheet, index=False)


def append_dedup_by_id(old_df: pd.DataFrame, new_df: pd.DataFrame, id_col: str = "record_id") -> pd.DataFrame:
    if new_df is None or new_df.empty:
        return old_df if old_df is not None else pd.DataFrame()
    if old_df is None or old_df.empty:
        return new_df.drop_duplicates(subset=[id_col], keep="last").reset_index(drop=True)

    merged = pd.concat([old_df, new_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=[id_col], keep="last").reset_index(drop=True)
    return merged


# =====================
# 日付推定
# =====================
def parse_jp_ymd(s: str) -> Optional[date]:
    if not s:
        return None
    s2 = normalize_digits(str(s))
    s2 = s2.replace("年", "/").replace("月", "/").replace("日", "")
    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", s2)
    if not m:
        return None
    y, mo, d = map(int, m.groups())
    try:
        return date(y, mo, d)
    except Exception:
        return None


def infer_nearest_date(month: int, day: int, crawl_dt: datetime, base_year: Optional[int] = None) -> date:
    cand_years: List[int] = []
    if base_year is not None:
        cand_years.append(base_year)
    cand_years.extend([crawl_dt.year - 1, crawl_dt.year, crawl_dt.year + 1])

    best: Optional[date] = None
    best_delta: Optional[int] = None
    for y in cand_years:
        try:
            d = date(y, month, day)
        except Exception:
            continue
        delta = abs((datetime(d.year, d.month, d.day, tzinfo=JST) - crawl_dt).days)
        if best is None or best_delta is None or delta < best_delta:
            best, best_delta = d, delta
    assert best is not None
    return best


def date_flag(res_d: Optional[date], crawl_dt: datetime) -> str:
    if res_d is None:
        return "CHECK"
    today = crawl_dt.date()
    if res_d < today:
        return "PAST"
    if res_d == today:
        return "TODAY"
    return "FUTURE"


# =====================
# 魚種推定（最低限）
# =====================
FISH_HINTS = [
    ("タチウオ", ["タチウオ", "太刀魚", "tachi"]),
    ("アジ", ["アジ", "鯵"]),
    ("イカ", ["イカ", "スルメ", "ヤリ", "アオリ"]),
    ("シーバス", ["シーバス", "スズキ"]),
]


def extract_fish_and_confidence(service: str, fallback: str = "") -> Tuple[str, str]:
    text = f"{service} {fallback}"
    t = normalize_space(text)
    for fish, keys in FISH_HINTS:
        for k in keys:
            if k in t:
                return fish, "HIGH"
    return "", ""


# =====================
# 画像処理（座席図から人数推定：任意）
# =====================
def dhash(img: Image.Image, hash_size: int = 8) -> int:
    im = img.convert("L").resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS)
    pix = list(im.getdata())
    rows = [pix[i * (hash_size + 1) : (i + 1) * (hash_size + 1)] for i in range(hash_size)]
    bits = []
    for r in rows:
        for i in range(hash_size):
            bits.append(1 if r[i] > r[i + 1] else 0)
    out = 0
    for b in bits:
        out = (out << 1) | b
    return out


def hamming(a: int, b: int) -> int:
    return (a ^ b).bit_count()


def is_seatlike_by_color(img: Image.Image) -> bool:
    if not OPENCV_OK:
        # numpy/cv2無しの時は足切りしない
        return True
    im = img.convert("RGB")
    w, h = im.size
    im = im.resize((max(80, w // 8), max(80, h // 8)))
    pix = np.array(im)

    r = pix[:, :, 0].astype("int16")
    g = pix[:, :, 1].astype("int16")
    b = pix[:, :, 2].astype("int16")

    mask = (b > 140) & (g > 120) & (r < 190)
    ratio = float(mask.mean())
    return ratio >= 0.12


TEMPLATE_HASH: Optional[int] = None
if os.path.exists(TEMPLATE_PATH):
    try:
        TEMPLATE_HASH = dhash(Image.open(TEMPLATE_PATH))
    except Exception:
        TEMPLATE_HASH = None


def count_people_from_seat_image(img: Image.Image) -> Tuple[str, str]:
    if not OPENCV_OK:
        return "", "opencv_not_available"

    try:
        im = ImageOps.exif_transpose(img).convert("RGB")
        if not is_seatlike_by_color(im):
            return "", "not_seatlike"

        np_img = np.array(im)
        gray = cv2.cvtColor(np_img, cv2.COLOR_RGB2GRAY)
        blur = cv2.GaussianBlur(gray, (5, 5), 0)
        thr = cv2.adaptiveThreshold(
            blur, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 51, 6
        )
        kernel = np.ones((3, 3), np.uint8)
        thr = cv2.morphologyEx(thr, cv2.MORPH_OPEN, kernel, iterations=1)

        contours, _ = cv2.findContours(thr, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        areas = [cv2.contourArea(c) for c in contours]
        if not areas:
            return "", "no_contours"

        areas_sorted = sorted(areas)
        med = areas_sorted[len(areas_sorted) // 2]
        min_a = max(10.0, med * 0.15)
        max_a = med * 6.0

        cnt = 0
        for c in contours:
            a = cv2.contourArea(c)
            if a < min_a or a > max_a:
                continue
            peri = cv2.arcLength(c, True)
            if peri <= 0:
                continue
            circ = 4.0 * 3.14159 * a / (peri * peri)
            if circ < 0.25:
                continue
            cnt += 1

        if cnt <= 0:
            return "", f"cnt=0 med={med:.1f}"
        return str(cnt), f"cnt={cnt} med={med:.1f}"

    except Exception as e:
        return "", f"opencv_failed:{repr(e)}"


def pick_seat_chart(session: requests.Session, candidate_urls: List[str], referer: str) -> Tuple[str, str, str]:
    MIN_BYTES = 4_000
    MAX_BYTES = 800_000
    STREAM_LIMIT = 1_200_000

    def url_pref(u: str) -> int:
        s = (u or "").lower()
        score = 0
        if any(k in s for k in ["zaseki", "seat", "chart", "zu", "yoyaku"]):
            score += 4
        if any(k in s for k in ["thumb", "small", "icon", "sp_"]):
            score -= 2
        if any(k in s for k in ["large", "full"]):
            score += 1
        return score

    ordered = sorted([u for u in candidate_urls if u], key=url_pref, reverse=True)[:80]

    best_url = ""
    best_people = ""
    best_note = "no_candidate"
    best_score = -10**9

    for u in ordered:
        try:
            data = fetch_bytes(session, u, referer=referer, max_bytes=STREAM_LIMIT)
            if len(data) < MIN_BYTES or len(data) > MAX_BYTES:
                continue

            img = Image.open(io.BytesIO(data))
            img = ImageOps.exif_transpose(img)

            dist = None
            if TEMPLATE_HASH is not None:
                try:
                    dist = hamming(TEMPLATE_HASH, dhash(img))
                except Exception:
                    dist = None

            people, note = count_people_from_seat_image(img)

            score = 0
            if people:
                score += 10_000 + int(people)
            if dist is not None:
                score += max(0, 500 - dist)
            score += url_pref(u) * 10

            if score > best_score:
                best_score = score
                best_url = u
                best_people = people
                dist_s = f"dist={dist}" if dist is not None else "dist=None"
                best_note = f"{note} {dist_s} bytes={len(data)}"

        except Exception as e:
            # どのURLで落ちたか分かるようにメモ
            best_note = f"last_error={repr(e)}"
            continue

        time.sleep(0.05)

    return best_url, best_people, best_note


# =====================
# ひらの丸：投稿抽出（簡易）
# =====================
@dataclass
class Post:
    title: str
    body_text: str
    detail_url: str
    post_date: str


def extract_posts_from_html_hirano(html: str, source_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    posts: List[Dict[str, str]] = []

    for art in soup.select("article, .post, .news, .entry"):
        title = ""
        t = art.select_one("h1, h2, h3, .title")
        if t:
            title = normalize_space(t.get_text(" ", strip=True))

        body = normalize_space(art.get_text(" ", strip=True))

        link = art.select_one("a[href]")
        detail = abs_url(source_url, link["href"]) if link else source_url

        dt = ""
        tm = art.select_one("time")
        if tm and tm.get("datetime"):
            dt = tm["datetime"]
        elif tm:
            dt = normalize_space(tm.get_text(" ", strip=True))

        if body:
            posts.append(
                {"title": title, "body_text": body, "detail_url": detail, "post_date": dt}
            )

    if not posts:
        text = normalize_space(soup.get_text(" ", strip=True))
        if text:
            posts.append({"title": "", "body_text": text, "detail_url": source_url, "post_date": ""})

    return posts


def extract_reservations_from_post_text(text: str) -> List[Dict[str, str]]:
    t = normalize_digits(text)
    t = normalize_space(t)

    rows: List[Dict[str, str]] = []
    parts = re.split(r"[。\n\r]| {2,}", t)
    for p in parts:
        p = normalize_space(p)
        if not p:
            continue
        m = re.search(r"(\d{1,2})/(\d{1,2})", p)
        if not m:
            continue

        md = f"{int(m.group(1))}/{int(m.group(2))}"
        after = p[m.end():].strip()
        boat = after.split(" ")[0].strip() if after else ""

        people = ""
        m2 = re.search(r"(\d{1,2})\s*名", p)
        if m2:
            people = m2.group(1)

        status = ""
        for kw in ["満席", "受付中", "空き", "空席", "キャンセル", "中止", "休船", "募集中"]:
            if kw in p:
                status = kw
                break

        rows.append(
            {"reservation_md": md, "boat": boat, "people_count": people, "status_text": status or p}
        )
    return rows


def collect_hiranomaru(session: requests.Session, crawl_time: str) -> pd.DataFrame:
    top_url = HIRANO_BASE_URL
    html = fetch_text(session, top_url, referer=top_url)
    all_posts = extract_posts_from_html_hirano(html, source_url=top_url)

    # 追加取得（失敗してOK）
    for p in range(2, HIRANO_MAX_PAGES + 1):
        api_url = abs_url(HIRANO_BASE_URL, HIRANO_MORE_API_PATH)
        try:
            frag = fetch_text(session, api_url, params={"p": p}, referer=top_url)
        except Exception:
            break
        if frag.strip().startswith("nodata"):
            break
        all_posts.extend(extract_posts_from_html_hirano(frag, source_url=f"{api_url}?p={p}"))
        time.sleep(SLEEP_BETWEEN_REQUEST_SEC)

    crawl_dt = pd.to_datetime(crawl_time, errors="coerce")
    if pd.isna(crawl_dt):
        crawl_dt = datetime.now(JST)

    rows: List[Dict[str, str]] = []
    for post in all_posts:
        reservations = extract_reservations_from_post_text(post.get("body_text", ""))
        base_y = None
        jp = parse_jp_ymd(str(post.get("post_date", "")))
        if jp:
            base_y = jp.year

        for r in reservations:
            md = str(r.get("reservation_md", "")).strip()
            mm = re.match(r"(\d{1,2})/(\d{1,2})", md)
            res_d = None
            if mm:
                mo, da = map(int, mm.groups())
                res_d = infer_nearest_date(mo, da, crawl_dt.to_pydatetime(), base_year=base_y)

            res_s = res_d.isoformat() if res_d else ""
            flag = date_flag(res_d, crawl_dt.to_pydatetime())

            boat = r.get("boat") or ""
            status = r.get("status_text") or ""
            people = r.get("people_count") or ""
            detail_url = post.get("detail_url") or top_url
            title = post.get("title") or ""

            service = boat if boat else title
            fish, conf = extract_fish_and_confidence(service=service, fallback=title)

            logical_key = f"hiranomaru|{res_s}|{normalize_space(service)}|{people}|{status}"
            rid = sha1_text(logical_key)

            rows.append(
                {
                    "record_id": rid,
                    "crawl_time": crawl_time,
                    "site_name": "ひらの丸",
                    "reservation_date": res_s,
                    "date_flag": flag,
                    "service": service,
                    "fish": fish,
                    "fish_confidence": conf,
                    "people_count": people,
                    "status_text": status,
                    "url": detail_url,
                    "url_normalized": url_normalize(detail_url),
                    "seat_image_url": "",
                    "note": "",
                }
            )

    if not rows:
        rid = sha1_text("hiranomaru|healthcheck|noreservations")
        rows.append(
            {
                "record_id": rid,
                "crawl_time": crawl_time,
                "site_name": "ひらの丸",
                "reservation_date": "",
                "date_flag": "CHECK",
                "service": "ひらの丸",
                "fish": "",
                "fish_confidence": "",
                "people_count": "",
                "status_text": "no reservations extracted",
                "url": top_url,
                "url_normalized": url_normalize(top_url),
                "seat_image_url": "",
                "note": "",
            }
        )

    return pd.DataFrame(rows, dtype=str)


def collect_arakawaya(session: requests.Session, crawl_time: str) -> pd.DataFrame:
    top_url = ARAKAWA_URL
    html = fetch_text(session, top_url, referer=top_url)
    soup = BeautifulSoup(html, "html.parser")

    cand: List[str] = []
    for img in soup.select("img[src]"):
        u = abs_url(top_url, img.get("src", ""))
        if u:
            cand.append(u)
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if isinstance(href, str) and re.search(r"\.(png|jpg|jpeg|webp)(\?|$)", href, re.I):
            cand.append(abs_url(top_url, href))
    cand = list(dict.fromkeys(cand))

    seat_url, people, note = pick_seat_chart(session, cand, referer=top_url)

    title = ""
    t = soup.select_one("title")
    if t:
        title = normalize_space(t.get_text(" ", strip=True))
    service = "荒川屋"
    fish, conf = extract_fish_and_confidence(service=service, fallback=title)

    rid = sha1_text("arakawaya|seat|" + url_normalize(seat_url or top_url) + "|" + (people or ""))

    rows = [
        {
            "record_id": rid,
            "crawl_time": crawl_time,
            "site_name": "荒川屋",
            "reservation_date": "",
            "date_flag": "CHECK",
            "service": service,
            "fish": fish,
            "fish_confidence": conf,
            "people_count": people,
            "status_text": "seat_chart" if seat_url else "no_seat_chart",
            "url": seat_url or top_url,
            "url_normalized": url_normalize(seat_url or top_url),
            "seat_image_url": seat_url,
            "note": note,
        }
    ]
    return pd.DataFrame(rows, dtype=str)


def main() -> None:
    ensure_dirs()
    crawl_time = now_jst_str()
    old_log = load_log(OUT_XLSX, SHEET_ALL)

    cols = [
        "record_id",
        "crawl_time",
        "site_name",
        "reservation_date",
        "date_flag",
        "service",
        "fish",
        "fish_confidence",
        "people_count",
        "status_text",
        "url",
        "url_normalized",
        "seat_image_url",
        "note",
    ]

    with make_session() as session:
        parts: List[pd.DataFrame] = []

        # 荒川屋
        try:
            parts.append(collect_arakawaya(session, crawl_time))
        except Exception as e:
            rid = sha1_text("arakawaya|error|" + repr(e))
            parts.append(
                pd.DataFrame(
                    [
                        {
                            "record_id": rid,
                            "crawl_time": crawl_time,
                            "site_name": "荒川屋",
                            "reservation_date": "",
                            "date_flag": "CHECK",
                            "service": "荒川屋",
                            "fish": "",
                            "fish_confidence": "",
                            "people_count": "",
                            "status_text": f"failed: {repr(e)}",
                            "url": ARAKAWA_URL,
                            "url_normalized": url_normalize(ARAKAWA_URL),
                            "seat_image_url": "",
                            "note": "arakawaya_fetch_failed",
                        }
                    ],
                    dtype=str,
                )
            )

        # ひらの丸
        try:
            parts.append(collect_hiranomaru(session, crawl_time))
        except Exception as e:
            rid = sha1_text("hiranomaru|error|" + repr(e))
            parts.append(
                pd.DataFrame(
                    [
                        {
                            "record_id": rid,
                            "crawl_time": crawl_time,
                            "site_name": "ひらの丸",
                            "reservation_date": "",
                            "date_flag": "CHECK",
                            "service": "ひらの丸",
                            "fish": "",
                            "fish_confidence": "",
                            "people_count": "",
                            "status_text": f"failed: {repr(e)}",
                            "url": HIRANO_BASE_URL,
                            "url_normalized": url_normalize(HIRANO_BASE_URL),
                            "seat_image_url": "",
                            "note": "hiranomaru_fetch_failed",
                        }
                    ],
                    dtype=str,
                )
            )

    new_log = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    for c in cols:
        if c not in new_log.columns:
            new_log[c] = ""
        if not old_log.empty and c not in old_log.columns:
            old_log[c] = ""

    new_log = new_log[cols].astype(str)
    if not old_log.empty:
        old_log = old_log[cols].astype(str)

    merged = append_dedup_by_id(old_log, new_log, id_col="record_id")
    save_log(OUT_XLSX, SHEET_ALL, merged)

    print("Saved:", OUT_XLSX)
    print(f"rows: {len(merged)} (new={len(new_log)})")
    if not OPENCV_OK:
        print("NOTE: opencv is not available -> people_count may be blank.")


if __name__ == "__main__":
    main()
