from __future__ import annotations
import io
import re
import datetime as dt
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
import pdfplumber
import pandas as pd
from flask import Flask, jsonify, send_file, Response, render_template_string

# --------- Einstellungen ---------
TZ = ZoneInfo("Europe/Zurich")
BASE_URL = "https://www.restaurant-unisg.ch/"

# Wochentage (DE)
WEEKDAY_ORDER = ["montag", "dienstag", "mittwoch", "donnerstag", "freitag"]
WEEKDAY_PATTERNS = {
    # PDFs schreiben Wochentage oft als "D I E N S T A G" – Regex erlaubt Leerzeichen zwischen Buchstaben
    "montag": re.compile(r"\bm\s*o\s*n\s*t\s*a\s*g\b", re.IGNORECASE),
    "dienstag": re.compile(r"\bd\s*i\s*e\s*n\s*s\s*t\s*a\s*g\b", re.IGNORECASE),
    "mittwoch": re.compile(r"\bm\s*i\s*t\s*t\s*w\s*o\s*c\s*h\b", re.IGNORECASE),
    "donnerstag": re.compile(r"\bd\s*o\s*n\s*n\s*e\s*r\s*s\s*t\s*a\s*g\b", re.IGNORECASE),
    "freitag": re.compile(r"\bf\s*r\s*e\s*i\s*t\s*a\s*g\b", re.IGNORECASE),
}

app = Flask(__name__)

# --------- Helpers Zeit/Daten ---------
def today_local_date() -> dt.date:
    return dt.datetime.now(TZ).date()

def week_dates_for_today(today: dt.date) -> dict[str, dt.date]:
    # Wochenstart = Montag
    monday = today - dt.timedelta(days=today.weekday())
    return {
        "montag": monday,
        "dienstag": monday + dt.timedelta(days=1),
        "mittwoch": monday + dt.timedelta(days=2),
        "donnerstag": monday + dt.timedelta(days=3),
        "freitag": monday + dt.timedelta(days=4),
    }

def weekday_de_name(iso_day: int) -> str | None:
    # 0=Montag ... 6=Sonntag
    return WEEKDAY_ORDER[iso_day] if 0 <= iso_day <= 4 else None

# --------- Helpers Text/PDF ---------
def _squash_spaced_letters(s: str) -> str:
    """
    PDFs haben oft Buchstaben mit Zwischenräumen (z. B. 'M O N T A G').
    Diese Funktion fügt solche Sequenzen korrekt zusammen.
    """
    def fix_word(word: str) -> str:
        if re.fullmatch(r"(?:[A-Za-zÄÖÜäöü]\s*){3,}", word):
            return re.sub(r"\s+", "", word)
        return word
    return " ".join(fix_word(w) for w in s.split())

def _extract_items_from_lines(lines: list[str]) -> list[dict]:
    """
    Teilt einen Tagesblock in einzelne Gerichte und extrahiert den Preis.
    Erkennt mehrere CHF-Beträge pro Zeile/Textblock.
    """
    text = " ".join(lines)
    text = re.sub(r"\s+", " ", text).strip()

    # Allergencodes & Deko entfernen
    text = re.sub(r"\b([A-ZÄÖÜ](?:\s*,\s*[A-ZÄÖÜ])+)\b", "", text)
    text = re.sub(r"\b(Allerg(?:ene|ien)|Icon|Info|Bio)\b.*?$", "", text, flags=re.IGNORECASE)

    items = []

    # Diese Regex trennt jedes Menü sauber:
    #   <Titel> CHF <Preis>  -> nicht-gierig bis zum nächsten CHF oder Satzende
    pattern = re.compile(
        r"(?P<title>.+?)\s*CHF\s*(?P<price>\d{1,2}[.,]\d{2})(?=\s*(?:[A-ZÄÖÜ]|$)|\s*CHF)",
        re.IGNORECASE,
    )

    for m in pattern.finditer(text):
        title = m.group("title").strip(" ,;:-")
        price = m.group("price").replace(",", ".")
        # kosmetische Fixes
        title = title.replace("Tagessuppeklein", "Tagessuppe klein")
        title = title.replace("Tagessuppegross", "Tagessuppe gross")
        title = re.sub(r"([a-zäöü])([A-ZÄÖÜ])", r"\1 \2", title)
        title = re.sub(r"\s{2,}", " ", title)

        if title:
            items.append({"title": title, "price_chf": price})

    # Falls trotzdem keine Preise gefunden: jede Zeile als Item
    if not items:
        for ln in lines:
            t = re.sub(r"\s+", " ", ln).strip(" ,;")
            if t:
                items.append({"title": t, "price_chf": None})

    return items


# --------- PDF finden/laden ---------
_cached_pdf_url: str | None = None

def get_cached_pdf_url() -> str | None:
    return _cached_pdf_url

def set_cached_pdf_url(url: str) -> None:
    global _cached_pdf_url
    _cached_pdf_url = url

def fetch_current_week_pdf_url() -> str:
    """
    Sucht auf der Startseite einen Link 'Aktuelle Woche ... (pdf)'.
    Fällt zurück auf irgendeinen 'menueplan*.pdf', falls nötig.
    """
    headers = {"User-Agent": "Mozilla/5.0 (MenuBot/1.0)"}
    r = requests.get(BASE_URL, headers=headers, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # bevorzugt: „Aktuelle Woche“ + .pdf
    for a in soup.find_all("a", href=True):
        text = (a.get_text(" ") or "").strip().lower()
        href = a["href"]
        if "aktuelle woche" in text and href.lower().endswith(".pdf"):
            return href if href.startswith("http") else requests.compat.urljoin(BASE_URL, href)

    # fallback: menueplan*.pdf
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf") and ("menueplan" in href.lower() or "menueplaene" in href.lower()):
            return href if href.startswith("http") else requests.compat.urljoin(BASE_URL, href)

    raise RuntimeError("Kein Wochen-Menü-PDF gefunden.")

def load_week_pdf_bytes() -> bytes:
    url = fetch_current_week_pdf_url()
    set_cached_pdf_url(url)
    headers = {"User-Agent": "Mozilla/5.0 (MenuBot/1.0)"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.content

# --------- Parsing der Woche ---------
def parse_week_pdf(pdf_bytes: bytes) -> dict[str, list[dict]]:
    """
    Gibt je Wochentag eine Liste von Gerichten zurück:
      {'montag': [{'title': '...', 'price_chf': '...'}, ...], ...}
    """
    results = {wd: [] for wd in WEEKDAY_ORDER}

    # 1) Alles an Textzeilen einsammeln
    text_lines: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # Tabellen (falls vorhanden)
            try:
                tables = page.extract_tables() or []
            except Exception:
                tables = []
            for tbl in tables:
                for row in tbl:
                    if not row:
                        continue
                    line = " ".join([c for c in row if c]).strip()
                    if line:
                        text_lines.append(line)
            # Fließtext
            raw = page.extract_text() or ""
            for ln in raw.splitlines():
                ln = ln.strip()
                if ln:
                    text_lines.append(ln)

    # 2) Normalisieren & Deko filtern
    norm_lines: list[str] = []
    for ln in text_lines:
        ln = _squash_spaced_letters(ln)          # "M O N T A G" -> "MONTAG"
        ln = re.sub(r"\s+", " ", ln).strip()
        if not ln:
            continue
        low = ln.lower()
        if low.startswith("menüplan") or "kw" in low:
            continue
        norm_lines.append(ln)

    # 3) Versuch A: Wochentage im Gesamttest finden und stream segmentieren
    blob = "\n".join(norm_lines).lower()
    day_regexes = {wd: re.compile(p.pattern.replace("\\b", ""), re.IGNORECASE)
                   for wd, p in WEEKDAY_PATTERNS.items()}  # großzügig: Wortgrenzen raus
    hits = []
    for wd in WEEKDAY_ORDER:
        m = day_regexes[wd].search(blob)
        if m:
            hits.append((wd, m.start()))

    segments_by_pos = None
    if len(hits) >= 2:
        hits.sort(key=lambda x: x[1])
        segments_by_pos = {}
        for i, (wd, pos) in enumerate(hits):
            end = hits[i + 1][1] if i + 1 < len(hits) else len(blob)
            segments_by_pos[wd] = blob[pos:end]

    if segments_by_pos:
        # Blob-Segment zurück in Zeilen und extrahieren
        for wd in WEEKDAY_ORDER:
            if wd in segments_by_pos:
                seg_lines = [re.sub(r"\s+", " ", ln).strip()
                             for ln in segments_by_pos[wd].splitlines() if ln.strip()]
                results[wd] = _extract_items_from_lines(seg_lines)

    # 4) Versuch B (Fallback): Segmentierung über Marker „Tagessuppe klein“
    if not any(results.values()):
        marker_idx = [i for i, ln in enumerate(norm_lines)
                      if "tagessuppe" in ln.lower() and "klein" in ln.lower()]
        if len(marker_idx) >= 5:
            blocks = []
            for i in range(5):
                start = marker_idx[i]
                end = marker_idx[i + 1] if i + 1 < len(marker_idx) else len(norm_lines)
                blocks.append(norm_lines[start:end])
            for i, wd in enumerate(WEEKDAY_ORDER):
                results[wd] = _extract_items_from_lines(blocks[i])

    # 5) Dedup/Feinschliff
    for wd in results:
        seen = set()
        dedup = []
        for it in results[wd]:
            key = (it["title"].lower(), it.get("price_chf"))
            if key in seen:
                continue
            seen.add(key)
            dedup.append(it)
        results[wd] = dedup

    return results

# --------- High-Level: Heute/Woche als DataFrame ---------
def build_dataframe_for_today(week_data: dict[str, list[dict]], today: dt.date) -> pd.DataFrame:
    dates = week_dates_for_today(today)
    wd_key = weekday_de_name(today.weekday())
    rows = []
    if wd_key and wd_key in week_data:
        for it in week_data[wd_key]:
            rows.append({
                "date": dates[wd_key].isoformat(),
                "weekday": wd_key.capitalize(),
                "title": it["title"],
                "price_chf": it.get("price_chf"),
                "source": get_cached_pdf_url() or ""
            })
    return pd.DataFrame(rows, columns=["date", "weekday", "title", "price_chf", "source"])

def scrape_week() -> dict[str, list[dict]]:
    pdf_bytes = load_week_pdf_bytes()
    return parse_week_pdf(pdf_bytes)

def scrape_today_df() -> pd.DataFrame:
    week = scrape_week()
    return build_dataframe_for_today(week, today_local_date())

# --------- Flask Routes ---------
@app.route("/")
def index():
    df = scrape_today_df()
    # Optional: Am Wochenende die Freitagsdaten zeigen
    if df.empty and today_local_date().weekday() >= 5:
        # Freitag der aktuellen Woche
        week = scrape_week()
        dates = week_dates_for_today(today_local_date())
        rows = []
        for it in week.get("freitag", []):
            rows.append({
                "date": dates["freitag"].isoformat(),
                "weekday": "Freitag",
                "title": it["title"],
                "price_chf": it.get("price_chf"),
                "source": get_cached_pdf_url() or ""
            })
        df = pd.DataFrame(rows, columns=["date", "weekday", "title", "price_chf", "source"])

    if df.empty:
        table_html = "<p>Für heute wurde nichts gefunden (evtl. Wochenende oder PDF-Layout geändert).</p>"
    else:
        table_html = df.to_html(index=False, justify="left")

    return render_template_string("""
    <!doctype html>
    <html lang="de">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Restaurant UniSG – Menü heute</title>
      <style>
        body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 2rem; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; }
        th { background: #f5f5f5; text-align: left; }
        a { color: #06c; }
      </style>
    </head>
    <body>
      <h1>Restaurant UniSG – Menü heute ({{ today }})</h1>
      <p>Quelle: <a href="{{ src }}">{{ src }}</a></p>
      {{ table|safe }}
      <p><a href="/menu.json">JSON</a> · <a href="/save.csv">CSV (heute)</a> · <a href="/week.json">Woche JSON</a> · <a href="/week.csv">Woche CSV</a></p>
    </body>
    </html>
    """, table=table_html, today=today_local_date().isoformat(), src=get_cached_pdf_url() or "–")

@app.route("/menu.json")
def menu_json():
    df = scrape_today_df()
    return jsonify([] if df.empty else df.to_dict(orient="records"))

@app.route("/save.csv")
def download_csv_today():
    df = scrape_today_df()
    if df.empty:
        return Response("Keine Daten gefunden.", status=404)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=f"unisg_menu_{today_local_date().isoformat()}.csv"
    )

@app.route("/week.json")
def week_json():
    week = scrape_week()
    dates = week_dates_for_today(today_local_date())
    records = []
    for wd in WEEKDAY_ORDER:
        for it in week.get(wd, []):
            records.append({
                "date": dates[wd].isoformat(),
                "weekday": wd.capitalize(),
                "title": it["title"],
                "price_chf": it.get("price_chf"),
                "source": get_cached_pdf_url() or ""
            })
    return jsonify(records)

@app.route("/week.csv")
def week_csv():
    week = scrape_week()
    dates = week_dates_for_today(today_local_date())
    rows = []
    for wd in WEEKDAY_ORDER:
        for it in week.get(wd, []):
            rows.append({
                "date": dates[wd].isoformat(),
                "weekday": wd.capitalize(),
                "title": it["title"],
                "price_chf": it.get("price_chf"),
                "source": get_cached_pdf_url() or ""
            })
    df = pd.DataFrame(rows, columns=["date", "weekday", "title", "price_chf", "source"])
    if df.empty:
        return Response("Keine Daten gefunden.", status=404)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=f"unisg_menu_week_{today_local_date().isoformat()}.csv"
    )

if __name__ == "__main__":
    # Lokal starten: python app.py
    app.run(host="0.0.0.0", port=8000, debug=True)
