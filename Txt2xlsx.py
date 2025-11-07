# -*- coding: utf-8 -*-
import os
import glob
import pandas as pd


def try_read_lines(path):
    """Versuche, die Datei mit mehreren Encodings zu lesen und gebe die Zeilen zurück."""
    encodings = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                return f.read().splitlines()
        except Exception:
            continue
    raise IOError("Datei kann nicht gelesen werden mit bekannten Encodings.")


def find_header_index(lines):
    """Finde die Index der Kopfzeile, deren erstes Feld genau 'Date' ist."""
    for i, line in enumerate(lines):
        parts = [p.strip() for p in line.split("\t")]
        if parts and parts[0] == "Date":
            return i, parts
    return None, None


def build_dataframe_from_lines(header, data_lines):
    """Erzeuge DataFrame aus Header (Liste) und data_lines (Liste von Zeilen)."""
    rows = []
    ncols = len(header)
    for ln in data_lines:
        parts = ln.split("\t")
        # pad or trim to header length
        if len(parts) < ncols:
            parts = [p.strip() for p in parts] + [""] * (ncols - len(parts))
        elif len(parts) > ncols:
            parts = parts[:ncols]
            parts = [p.strip() for p in parts]
        else:
            parts = [p.strip() for p in parts]
        rows.append(parts)
    df = pd.DataFrame(rows, columns=header)
    return df


def merge_date_time_if_present(df):
    """Wenn eine Time-Spalte existiert (z.B. 'Time(s)' oder 'Time (s)' o.ä.), entferne sie.

    Der Nutzer wünscht explizit, dass die Time-Spalte nicht in der Ausgabe erscheint.
    """
    def find_time_col(columns):
        for c in columns:
            if not isinstance(c, str):
                continue
            norm = ''.join(ch.lower() for ch in c if ch.isalnum())
            if norm.startswith('time'):
                return c
        return None

    if "Date" not in df.columns:
        return df

    time_col = find_time_col(df.columns)
    if time_col and time_col != "Date":
        # Entferne die Time-Spalte vollständig
        if time_col in df.columns:
            df = df.drop(columns=[time_col])
            print(f"Entferne erkannte Time-Spalte: '{time_col}'")
    return df


def find_start_index_by_penult_col(df):
    """Bestimme den ersten Index, bei dem die vorletzte Spalte != 0 (nach Numerisierung).
    Liefert None, wenn kein solcher Wert existiert.
    """
    if df.shape[1] < 2:
        return None, None
    penult_col = df.columns[-2]
    # Ersetze Kommata mit Punkten und numerisch umwandeln
    ser = df[penult_col].astype(str).str.replace(",", ".", regex=False)
    ser_num = pd.to_numeric(ser, errors="coerce")
    mask = ser_num.notna() & (ser_num != 0)
    if not mask.any():
        return None, penult_col
    # erster True index (DataFrame hat RangeIndex 0..)
    idx = int(mask.idxmax())
    return idx, penult_col


def get_txt_path_from_user():
    choice = input("Pfad zur .txt-Datei (Enter = erstes .txt im aktuellen Ordner): ").strip()
    if choice:
        return choice
    # nimm erstes txt im cwd
    txts = sorted(glob.glob("*.txt"))
    if not txts:
        print("Keine .txt-Datei im aktuellen Verzeichnis gefunden.")
        return None
    print(f"Verwende erste gefundene Datei: {txts[0]}")
    return txts[0]


def get_sampling_k():
    raw = input("Jeden wievielten Datenpunkt übernehmen? (Default 1): ").strip()
    if not raw:
        return 1
    try:
        k = int(raw)
        if k < 1:
            raise ValueError
        return k
    except ValueError:
        print("Ungültige Eingabe. Verwende 1.")
        return 1


def main():
    print("Starte Konvertierung txt -> xlsx")
    path = get_txt_path_from_user()
    if not path:
        return
    if not os.path.isfile(path):
        print(f"Datei nicht gefunden: {path}")
        return

    k = get_sampling_k()

    try:
        lines = try_read_lines(path)
    except Exception as e:
        print("Fehler beim Lesen der Datei:", e)
        return

    header_idx, header = find_header_index(lines)
    if header_idx is None:
        print("Kopfzeile mit 'Date' nicht gefunden. Abbruch.")
        return

    data_lines = lines[header_idx + 1 :]
    if not data_lines:
        print("Keine Datenzeilen nach der Kopfzeile gefunden. Abbruch.")
        return

    df = build_dataframe_from_lines(header, data_lines)

    # Time(s) behandeln: an Date anhängen und entfernen
    df = merge_date_time_if_present(df)

    if df.empty:
        print("Keine Daten vorhanden nach Verarbeitung.")
        return

    start_idx, penult_col = find_start_index_by_penult_col(df)
    if start_idx is None:
        print(f"Kein erster Wert != 0 in der vorletzten Spalte ('{penult_col}') gefunden. Abbruch.")
        return

    # Auswahl: ab start_idx einschließlich, dann jedes k-te
    out_df = df.iloc[start_idx :: k].reset_index(drop=True)

    # Ausgabe-Dateiname: gleicher Basisname mit .xlsx
    base = os.path.splitext(os.path.basename(path))[0]
    out_name = base + ".xlsx"
    if os.path.exists(out_name):
        print(f"Warnung: Zieldatei '{out_name}' existiert bereits. Datei wird nicht überschrieben.")
        return

    try:
        # Writer verwendet openpyxl automatisch wenn installiert
        out_df.to_excel(out_name, index=False)
        print(f"Erfolgreich gespeichert: {out_name}")
        print(f"Ab Zeile (0-basiert im DataFrame): {start_idx}, Sampling: jede {k}. Zeile")
    except Exception as e:
        print("Fehler beim Speichern der Excel-Datei:", e)


if __name__ == "__main__":
    main()
