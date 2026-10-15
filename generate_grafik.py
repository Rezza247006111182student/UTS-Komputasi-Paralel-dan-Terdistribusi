# Nama Lengkap : Muhammad Rezza
# NPM/NIM      : 247006111182
# Kelas        : G
#
# Membuat 3 grafik Bagian C dari file hasil.csv.
# Format tiap baris hasil.csv (tanpa header, sama dengan baris "CSV," dari
# parallel_dataset_cleaner.py tanpa awalan "CSV,"):
#   rank,thread,proses,data,waktu,speedup,efisiensi
#
# Cara pakai:
#   python generate_grafik.py
#   python generate_grafik.py --input hasil.csv --out grafik

import argparse
import os

# pyrefly: ignore [missing-import]
import matplotlib
matplotlib.use("Agg")
# pyrefly: ignore [missing-import]
import matplotlib.pyplot as plt

# parameter baseline pribadi (dari NIM)
BASE_THREAD, BASE_PROSES, BASE_DATA = 4, 3, 1820


def baca_hasil(path):
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("CSV,"):
                line = line[4:]
            p = line.split(",")
            if len(p) != 7:
                continue
            rank, th, pr, data = int(p[0]), int(p[1]), int(p[2]), int(p[3])
            waktu, sp, ef = float(p[4]), float(p[5]), float(p[6])
            rows.append((rank, th, pr, data, waktu, sp, ef))
    return rows


def rata2_per_kunci(pairs):
    """pairs: [(x, y)]; jika x sama muncul lebih dari sekali, y dirata-rata."""
    d = {}
    for x, y in pairs:
        d.setdefault(x, []).append(y)
    return sorted((x, sum(v) / len(v)) for x, v in d.items())


def grafik_garis(titik, judul, xlabel, ylabel, warna, nama_file):
    x, y = zip(*titik)
    plt.figure(figsize=(6, 4.2))
    plt.plot(x, y, marker="o", color=warna)
    plt.title(judul)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(x)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(nama_file, dpi=150)
    plt.close()
    print("tersimpan:", nama_file)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="hasil.csv")
    ap.add_argument("--out", default=".")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    rows = baca_hasil(args.input)
    if not rows:
        raise SystemExit(f"Tidak ada data valid di {args.input}")

    # 1. Waktu vs Thread (proses & data = baseline)
    g1 = rata2_per_kunci([(r[1], r[4]) for r in rows
                          if r[2] == BASE_PROSES and r[3] == BASE_DATA])
    grafik_garis(g1, f"Waktu vs Jumlah Thread\n(Proses={BASE_PROSES}, Data={BASE_DATA})",
                 "Jumlah Thread", "Waktu (s)", "#2a6fbb",
                 os.path.join(args.out, "grafik_waktu_vs_thread.png"))

    # 2. Waktu vs Proses (thread & data = baseline)
    g2 = rata2_per_kunci([(r[2], r[4]) for r in rows
                          if r[1] == BASE_THREAD and r[3] == BASE_DATA])
    grafik_garis(g2, f"Waktu vs Jumlah Process\n(Thread={BASE_THREAD}, Data={BASE_DATA})",
                 "Jumlah Process", "Waktu (s)", "#d9822b",
                 os.path.join(args.out, "grafik_waktu_vs_proses.png"))

    # 3. Speedup vs Konfigurasi (semua percobaan, urut sesuai file)
    labels = [f"T{r[1]}-P{r[2]}-D{r[3]}" for r in rows]
    speedups = [r[5] for r in rows]
    plt.figure(figsize=(max(8, len(rows) * 0.8), 4.8))
    plt.bar(range(len(rows)), speedups, color="#3fa34d")
    plt.axhline(1.0, color="red", linestyle="--", linewidth=1, label="Speedup = 1 (sama dengan serial)")
    plt.xticks(range(len(rows)), labels, rotation=45, ha="right")
    plt.title(f"Speedup vs Konfigurasi ({len(rows)} percobaan)")
    plt.ylabel("Speedup")
    plt.legend()
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    nama = os.path.join(args.out, "grafik_speedup_vs_konfigurasi.png")
    plt.savefig(nama, dpi=150)
    plt.close()
    print("tersimpan:", nama)


if __name__ == "__main__":
    main()
