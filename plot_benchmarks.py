# Nama Lengkap : Muhammad Rezza
# NPM/NIM      : 247006111182
# Kelas        : G
#
# plot_benchmarks.py — Visualisasi Hasil Benchmark
#
# Membaca benchmark_summary.csv (atau benchmark_results.csv) dan menghasilkan
# 6 grafik yang disimpan ke results/plots/.
#
# Cara menjalankan:
#   python plot_benchmarks.py
#   python plot_benchmarks.py --results-dir results
#   python plot_benchmarks.py --summary results/benchmark_summary.csv
#
# Dependensi:
#   pip install matplotlib

import argparse
import csv
import os
import sys

try:
    import matplotlib  # type: ignore[import]
    matplotlib.use("Agg")  # non-interactive backend (aman di semua OS)
    import matplotlib.pyplot as plt  # type: ignore[import]
    import matplotlib.patches as mpatches  # type: ignore[import]
    from matplotlib.ticker import MaxNLocator  # type: ignore[import]
except ImportError:
    print("[ERROR] matplotlib tidak ditemukan. Instal dengan: pip install matplotlib")
    sys.exit(1)

# ── Konstanta visual ──
WARNA_SERIAL = "#E63946"       # merah
WARNA_PARALEL = "#457B9D"      # biru
WARNA_SPEEDUP = "#2A9D8F"      # hijau teal
WARNA_EFF = "#F4A261"          # oranye
WARNA_MISSING_BEFORE = "#E76F51"
WARNA_MISSING_FILLED = "#264653"
FONT_JUDUL = {"fontsize": 13, "fontweight": "bold", "pad": 14}
FONT_LABEL = {"fontsize": 10}
DPI = 150
FIGSIZE_BAR = (11, 5)
FIGSIZE_LINE = (11, 5)
FIGSIZE_QUALITY = (10, 5)


def baca_summary(path):
    """Baca benchmark_summary.csv dan kembalikan list of dict."""
    if not os.path.exists(path):
        return None, f"File tidak ditemukan: {path}"
    rows = []
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        kolom = reader.fieldnames or []
        for row in reader:
            rows.append(row)
    return rows, None


def baca_results(path):
    """Baca benchmark_results.csv (per pengulangan) dan kembalikan list of dict."""
    if not os.path.exists(path):
        return None, f"File tidak ditemukan: {path}"
    rows = []
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("status") == "success":
                rows.append(row)
    return rows, None


def safe_float(val, default=None):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def safe_int(val, default=None):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def label_konfigurasi(row):
    """Buat label singkat dari konfigurasi eksperimen."""
    r = safe_int(row.get("mpi_ranks"), 1)
    t = safe_int(row.get("threads"), 1)
    p = safe_int(row.get("processes"), 1)
    d = safe_int(row.get("data_rows"), 0)
    return f"{row.get('experiment_id','?')}\n({r}R·{t}T·{p}P·{d//1000 if d>=1000 else d}{'k' if d>=1000 else ''}D)"


def tambah_nilai_bar(ax, rects, fmt="{:.3f}", fontsize=8):
    """Tambahkan label nilai di atas setiap bar."""
    for rect in rects:
        h = rect.get_height()
        if h is not None and h > 0:
            ax.text(
                rect.get_x() + rect.get_width() / 2.0,
                h * 1.01,
                fmt.format(h),
                ha="center", va="bottom", fontsize=fontsize
            )


def simpan_grafik(fig, path, nama):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"  [OK] Disimpan: {path}")


# ════════════════════════════════════════════════════════════════
# GRAFIK 1 — Perbandingan Waktu Eksekusi (Serial vs Paralel)
# ════════════════════════════════════════════════════════════════
def grafik_waktu(rows, output_dir):
    """Grafik batang perbandingan waktu serial vs paralel."""
    data = [(r, safe_float(r.get("serial_time_seconds") or r.get("mean_serial_time_s")),
             safe_float(r.get("parallel_time_seconds") or r.get("mean_parallel_time_s")))
            for r in rows]
    data = [(r, s, p) for r, s, p in data if p is not None]

    if not data:
        print("  [SKIP] Grafik 1: tidak ada data waktu yang valid.")
        return

    labels = [label_konfigurasi(r) for r, _, _ in data]
    serials = [s if s is not None else 0 for _, s, _ in data]
    parallels = [p for _, _, p in data]

    x = range(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=FIGSIZE_BAR)
    bars1 = ax.bar([i - width/2 for i in x], serials, width, label="Waktu Serial (s)",
                   color=WARNA_SERIAL, alpha=0.85, edgecolor="white")
    bars2 = ax.bar([i + width/2 for i in x], parallels, width, label="Waktu Paralel / Makespan (s)",
                   color=WARNA_PARALEL, alpha=0.85, edgecolor="white")

    # Tampilkan nilai jika tidak terlalu padat
    if len(data) <= 8:
        tambah_nilai_bar(ax, bars1, fmt="{:.3f}", fontsize=7)
        tambah_nilai_bar(ax, bars2, fmt="{:.3f}", fontsize=7)

    ax.set_title("Grafik 1 — Perbandingan Waktu Eksekusi: Serial vs Paralel", **FONT_JUDUL)
    ax.set_xlabel("Konfigurasi Eksperimen", **FONT_LABEL)
    ax.set_ylabel("Waktu (detik)", **FONT_LABEL)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, fontsize=7, rotation=15, ha="right")
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.set_ylim(bottom=0)

    catatan = ("Waktu Serial = SUM waktu serial semua rank (pekerjaan setara jika dikerjakan satu per satu).\n"
               "Waktu Paralel = Makespan = MAX waktu dinding antar rank.")
    fig.text(0.01, -0.08, catatan, fontsize=7, color="gray", transform=ax.transAxes)

    simpan_grafik(fig, os.path.join(output_dir, "grafik1_waktu_eksekusi.png"), "Grafik 1")


# ════════════════════════════════════════════════════════════════
# GRAFIK 2 — Speedup
# ════════════════════════════════════════════════════════════════
def grafik_speedup(rows, output_dir):
    """Grafik batang/garis speedup setiap konfigurasi."""
    data = [(r, safe_float(r.get("speedup") or r.get("mean_speedup")))
            for r in rows]
    data = [(r, s) for r, s in data if s is not None]

    if not data:
        print("  [SKIP] Grafik 2: tidak ada data speedup yang valid.")
        return

    labels = [label_konfigurasi(r) for r, _ in data]
    speedups = [s for _, s in data]

    fig, ax = plt.subplots(figsize=FIGSIZE_BAR)
    colors = [WARNA_SPEEDUP if s >= 1.0 else WARNA_SERIAL for s in speedups]
    bars = ax.bar(range(len(labels)), speedups, color=colors, alpha=0.85, edgecolor="white")

    # Garis referensi speedup = 1
    ax.axhline(y=1.0, color="black", linestyle="--", linewidth=1.2, label="Speedup = 1 (setara serial)")

    if len(data) <= 8:
        tambah_nilai_bar(ax, bars, fmt="{:.3f}", fontsize=8)

    ax.set_title("Grafik 2 — Speedup Setiap Konfigurasi", **FONT_JUDUL)
    ax.set_xlabel("Konfigurasi Eksperimen", **FONT_LABEL)
    ax.set_ylabel("Speedup (Waktu Serial Total / Makespan)", **FONT_LABEL)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, fontsize=7, rotation=15, ha="right")
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.set_ylim(bottom=0)

    patch_a = mpatches.Patch(color=WARNA_SPEEDUP, label="Speedup ≥ 1 (lebih cepat dari serial)")
    patch_b = mpatches.Patch(color=WARNA_SERIAL, label="Speedup < 1 (lebih lambat dari serial)")
    ax.legend(handles=[patch_a, patch_b,
                       plt.Line2D([0], [0], color="black", linestyle="--", label="Referensi = 1")],
              fontsize=8)

    catatan = ("Speedup < 1 berarti overhead pembuatan proses/thread lebih besar dari manfaat paralelisme.\n"
               "Ini wajar untuk dataset kecil dan merupakan fenomena nyata, bukan bug.")
    fig.text(0.01, -0.08, catatan, fontsize=7, color="gray", transform=ax.transAxes)

    simpan_grafik(fig, os.path.join(output_dir, "grafik2_speedup.png"), "Grafik 2")


# ════════════════════════════════════════════════════════════════
# GRAFIK 3 — Efisiensi Paralel
# ════════════════════════════════════════════════════════════════
def grafik_efisiensi(rows, output_dir):
    """Grafik efisiensi paralel (persen)."""
    data = [(r,
             safe_float(r.get("efficiency_percent") or r.get("mean_efficiency_percent")),
             safe_float(r.get("efficiency_total_percent") or r.get("mean_efficiency_total_percent")))
            for r in rows]
    data = [(r, e, et) for r, e, et in data if e is not None]

    if not data:
        print("  [SKIP] Grafik 3: tidak ada data efisiensi yang valid.")
        return

    labels = [label_konfigurasi(r) for r, _, _ in data]
    effs = [e for _, e, _ in data]
    effs_total = [et if et is not None else 0 for _, _, et in data]
    has_total = any(et is not None for _, _, et in data)

    x = range(len(labels))
    width = 0.35 if has_total else 0.5

    fig, ax = plt.subplots(figsize=FIGSIZE_BAR)
    bars1 = ax.bar([i - width/2 for i in x] if has_total else list(x),
                   effs, width, label="Efisiensi (relatif n_proses) %",
                   color=WARNA_EFF, alpha=0.85, edgecolor="white")
    if has_total:
        bars2 = ax.bar([i + width/2 for i in x], effs_total, width,
                       label="Efisiensi Total (relatif n_rank × n_proses) %",
                       color=WARNA_PARALEL, alpha=0.65, edgecolor="white")

    ax.axhline(y=100, color="black", linestyle="--", linewidth=1, label="Efisiensi ideal = 100%")

    ax.set_title("Grafik 3 — Efisiensi Paralel", **FONT_JUDUL)
    ax.set_xlabel("Konfigurasi Eksperimen", **FONT_LABEL)
    ax.set_ylabel("Efisiensi (%)", **FONT_LABEL)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, fontsize=7, rotation=15, ha="right")
    ax.legend(fontsize=8)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.set_ylim(bottom=0)

    catatan = ("Efisiensi = (Speedup / n_proses) × 100%.\n"
               "Efisiensi Total = (Speedup / (n_rank × n_proses)) × 100%.\n"
               "Efisiensi 100% berarti setiap proses berkontribusi penuh (ideal linear scaling).")
    fig.text(0.01, -0.12, catatan, fontsize=7, color="gray", transform=ax.transAxes)

    simpan_grafik(fig, os.path.join(output_dir, "grafik3_efisiensi.png"), "Grafik 3")


# ════════════════════════════════════════════════════════════════
# GRAFIK 4 — Pengaruh Jumlah Data
# ════════════════════════════════════════════════════════════════
def grafik_pengaruh_data(rows, output_dir):
    """Waktu paralel vs jumlah data (konfigurasi komputasi sebanding)."""
    # Filter eksperimen dengan konfigurasi yang sama tetapi jumlah data berbeda
    # Fokus pada n_rank=1, n_thread=4, n_proses=3
    target = [r for r in rows
              if safe_int(r.get("mpi_ranks"), 0) == 1
              and safe_int(r.get("threads"), 0) == 4
              and safe_int(r.get("processes"), 0) == 3]

    if len(target) < 2:
        print("  [SKIP] Grafik 4: butuh ≥2 titik data dengan konfigurasi 1R·4T·3P.")
        return

    target.sort(key=lambda r: safe_int(r.get("data_rows"), 0) or 0)
    xs = [safe_int(r.get("data_rows"), 0) for r in target]
    ys_par = [safe_float(r.get("parallel_time_seconds") or r.get("mean_parallel_time_s")) for r in target]
    ys_ser = [safe_float(r.get("serial_time_seconds") or r.get("mean_serial_time_s")) for r in target]

    fig, ax = plt.subplots(figsize=FIGSIZE_LINE)
    ax.plot(xs, ys_par, "o-", color=WARNA_PARALEL, linewidth=2, markersize=7, label="Waktu Paralel (makespan)")
    if any(y is not None and y > 0 for y in ys_ser):
        ax.plot(xs, [y or 0 for y in ys_ser], "s--", color=WARNA_SERIAL, linewidth=1.5, markersize=6, label="Waktu Serial (total)")

    for x, y in zip(xs, ys_par):
        if y:
            ax.annotate(f"{y:.3f}s", (x, y), textcoords="offset points", xytext=(0, 8), fontsize=8, ha="center")

    ax.set_title("Grafik 4 — Pengaruh Jumlah Data terhadap Waktu Eksekusi\n(Konfigurasi: 1 Rank · 4 Thread · 3 Proses)", **FONT_JUDUL)
    ax.set_xlabel("Jumlah Baris Dataset", **FONT_LABEL)
    ax.set_ylabel("Waktu (detik)", **FONT_LABEL)
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.legend(fontsize=9)
    ax.grid(linestyle="--", alpha=0.5)
    ax.set_ylim(bottom=0)

    catatan = "Konfigurasi komputasi dijaga konstan (1R·4T·3P) untuk mengisolasi pengaruh ukuran data."
    fig.text(0.01, -0.05, catatan, fontsize=7, color="gray", transform=ax.transAxes)

    simpan_grafik(fig, os.path.join(output_dir, "grafik4_pengaruh_data.png"), "Grafik 4")


# ════════════════════════════════════════════════════════════════
# GRAFIK 5 — Pengaruh Konfigurasi Paralel
# ════════════════════════════════════════════════════════════════
def grafik_pengaruh_konfigurasi(rows, output_dir):
    """Dua sub-grafik: variasi jumlah proses dan variasi MPI rank (data tetap 1820)."""

    # --- 5a: Variasi jumlah proses (n_rank=1, n_thread=4, n_data=1820) ---
    var_proses = [r for r in rows
                  if safe_int(r.get("mpi_ranks"), 0) == 1
                  and safe_int(r.get("threads"), 0) == 4
                  and safe_int(r.get("data_rows"), 0) == 1820]
    var_proses.sort(key=lambda r: safe_int(r.get("processes"), 0) or 0)

    # --- 5b: Variasi MPI rank (n_thread=4, n_proses=3, n_data=1820) ---
    var_rank = [r for r in rows
                if safe_int(r.get("threads"), 0) == 4
                and safe_int(r.get("processes"), 0) == 3
                and safe_int(r.get("data_rows"), 0) == 1820]
    var_rank.sort(key=lambda r: safe_int(r.get("mpi_ranks"), 0) or 0)

    if not var_proses and not var_rank:
        print("  [SKIP] Grafik 5: tidak ada variasi konfigurasi yang cukup.")
        return

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Grafik 5 — Pengaruh Konfigurasi Paralel\n(Dataset: 1820 baris)",
                 fontsize=13, fontweight="bold", y=1.02)

    # Panel kiri: variasi proses
    ax = axes[0]
    if var_proses:
        ps = [safe_int(r.get("processes"), 0) for r in var_proses]
        times = [safe_float(r.get("parallel_time_seconds") or r.get("mean_parallel_time_s")) for r in var_proses]
        speeds = [safe_float(r.get("speedup") or r.get("mean_speedup")) for r in var_proses]
        ax2 = ax.twinx()
        bars = ax.bar(ps, [t or 0 for t in times], color=WARNA_PARALEL, alpha=0.7, label="Waktu Paralel (s)")
        line, = ax2.plot(ps, [s or 0 for s in speeds], "D--", color=WARNA_SPEEDUP, linewidth=2, markersize=7, label="Speedup")
        ax2.axhline(y=1, color="gray", linestyle=":", linewidth=1)
        ax.set_xlabel("Jumlah Proses (n_proses)", **FONT_LABEL)
        ax.set_ylabel("Waktu Paralel (detik)", **FONT_LABEL)
        ax2.set_ylabel("Speedup", **FONT_LABEL)
        ax.set_title("Isolasi: Pengaruh Jumlah Proses\n(1R · 4T · 1820D)", fontsize=10)
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        lines = [bars, line]
        labels = ["Waktu Paralel (s)", "Speedup"]
        ax.legend(lines, labels, fontsize=8, loc="upper right")
        ax.grid(axis="y", linestyle="--", alpha=0.4)
        ax.set_ylim(bottom=0)
    else:
        ax.text(0.5, 0.5, "Data tidak tersedia\n(butuh eksperimen variasi n_proses)",
                ha="center", va="center", fontsize=10, color="gray")
        ax.set_title("Isolasi: Pengaruh Jumlah Proses")

    # Panel kanan: variasi rank
    ax = axes[1]
    if var_rank:
        rs = [safe_int(r.get("mpi_ranks"), 0) for r in var_rank]
        times = [safe_float(r.get("parallel_time_seconds") or r.get("mean_parallel_time_s")) for r in var_rank]
        speeds = [safe_float(r.get("speedup") or r.get("mean_speedup")) for r in var_rank]
        ax2 = ax.twinx()
        bars = ax.bar(rs, [t or 0 for t in times], color=WARNA_EFF, alpha=0.7, label="Waktu Paralel (s)")
        line, = ax2.plot(rs, [s or 0 for s in speeds], "D--", color=WARNA_SPEEDUP, linewidth=2, markersize=7, label="Speedup")
        ax2.axhline(y=1, color="gray", linestyle=":", linewidth=1)
        ax.set_xlabel("Jumlah MPI Rank", **FONT_LABEL)
        ax.set_ylabel("Waktu Paralel (detik)", **FONT_LABEL)
        ax2.set_ylabel("Speedup", **FONT_LABEL)
        ax.set_title("Isolasi: Pengaruh Jumlah MPI Rank\n(4T · 3P · 1820D)", fontsize=10)
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        lines = [bars, line]
        labels = ["Waktu Paralel (s)", "Speedup"]
        ax.legend(lines, labels, fontsize=8, loc="upper right")
        ax.grid(axis="y", linestyle="--", alpha=0.4)
        ax.set_ylim(bottom=0)
    else:
        ax.text(0.5, 0.5, "Data tidak tersedia\n(butuh eksperimen variasi n_rank)",
                ha="center", va="center", fontsize=10, color="gray")
        ax.set_title("Isolasi: Pengaruh Jumlah MPI Rank")

    catatan = ("Penting: setiap panel hanya mengubah SATU variabel. "
               "Kesimpulan pengaruh variabel hanya valid jika variabel lain dijaga konstan.")
    fig.text(0.01, -0.04, catatan, fontsize=7, color="gray")

    plt.tight_layout()
    simpan_grafik(fig, os.path.join(output_dir, "grafik5_konfigurasi_paralel.png"), "Grafik 5")


# ════════════════════════════════════════════════════════════════
# GRAFIK 6 — Kualitas Data (Missing Values Sebelum vs Sesudah)
# ════════════════════════════════════════════════════════════════
def grafik_kualitas_data(rows, output_dir):
    """Perbandingan nilai kosong sebelum dan sesudah pembersihan."""
    data = [(r,
             safe_int(r.get("missing_values_before")),
             safe_int(r.get("missing_values_filled") or r.get("mean_missing_filled")))
            for r in rows]
    data = [(r, b, f) for r, b, f in data if b is not None and f is not None]

    if not data:
        print("  [SKIP] Grafik 6: tidak ada data kualitas yang valid.")
        return

    # Deduplikasi berdasarkan n_data (karena kualitas data tidak bergantung pada konfigurasi komputasi)
    seen = {}
    for r, b, f in data:
        key = (safe_int(r.get("data_rows"), 0),)
        if key not in seen:
            seen[key] = (r, b, f)
    data_unik = list(seen.values())
    data_unik.sort(key=lambda x: safe_int(x[0].get("data_rows"), 0) or 0)

    labels = [f"{safe_int(r.get('data_rows'),0)} baris" for r, _, _ in data_unik]
    befores = [b for _, b, _ in data_unik]
    afters = [b - f for _, b, f in data_unik]   # nilai kosong tersisa setelah pembersihan
    filleds = [f for _, _, f in data_unik]

    x = range(len(labels))
    width = 0.3

    fig, ax = plt.subplots(figsize=FIGSIZE_QUALITY)
    bars1 = ax.bar([i - width for i in x], befores, width, label="Nilai Kosong Sebelum Pembersihan",
                   color=WARNA_MISSING_BEFORE, alpha=0.85, edgecolor="white")
    bars2 = ax.bar(list(x), filleds, width, label="Nilai Kosong yang Berhasil Diisi",
                   color=WARNA_MISSING_FILLED, alpha=0.85, edgecolor="white")
    bars3 = ax.bar([i + width for i in x], afters, width, label="Nilai Kosong Tersisa",
                   color="lightgray", alpha=0.9, edgecolor="gray")

    if len(data_unik) <= 6:
        tambah_nilai_bar(ax, bars1, fmt="{:.0f}", fontsize=8)
        tambah_nilai_bar(ax, bars2, fmt="{:.0f}", fontsize=8)
        tambah_nilai_bar(ax, bars3, fmt="{:.0f}", fontsize=8)

    ax.set_title("Grafik 6 — Kualitas Data: Nilai Kosong Sebelum dan Sesudah Pembersihan", **FONT_JUDUL)
    ax.set_xlabel("Ukuran Dataset", **FONT_LABEL)
    ax.set_ylabel("Jumlah Nilai Kosong", **FONT_LABEL)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, fontsize=9)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.set_ylim(bottom=0)

    catatan = ("Data berdasarkan hasil pembersihan aktual dari program, bukan asumsi proporsi.\n"
               "Nilai kosong tersisa = 0 berarti semua nilai kosong berhasil diisi.")
    fig.text(0.01, -0.08, catatan, fontsize=7, color="gray", transform=ax.transAxes)

    simpan_grafik(fig, os.path.join(output_dir, "grafik6_kualitas_data.png"), "Grafik 6")


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="Visualisasi Hasil Benchmark Parallel Dataset Cleaner"
    )
    parser.add_argument("--results-dir", default="results",
                        help="Folder yang berisi file CSV benchmark (default: results)")
    parser.add_argument("--summary", default=None,
                        help="Path ke benchmark_summary.csv (opsional, otomatis dicari di --results-dir)")
    parser.add_argument("--raw", default=None,
                        help="Path ke benchmark_results.csv (opsional)")
    parser.add_argument("--plots-dir", default=None,
                        help="Folder output grafik (default: <results-dir>/plots)")
    args = parser.parse_args()

    plots_dir = args.plots_dir or os.path.join(args.results_dir, "plots")
    os.makedirs(plots_dir, exist_ok=True)

    summary_path = args.summary or os.path.join(args.results_dir, "benchmark_summary.csv")
    results_path = args.raw or os.path.join(args.results_dir, "benchmark_results.csv")

    print("=" * 60)
    print("  Visualisasi Benchmark — Parallel Dataset Cleaner")
    print(f"  Nama: Muhammad Rezza | NIM: 247006111182 | Kelas: G")
    print("=" * 60)

    # Utamakan summary (rata-rata), jika tidak ada gunakan results (per ulangan)
    rows_summary, err_s = baca_summary(summary_path)
    rows_results, err_r = baca_results(results_path)

    if rows_summary:
        print(f"[OK] Menggunakan summary: {summary_path} ({len(rows_summary)} eksperimen)")
        rows = rows_summary
        # Untuk grafik yang perlu data per-ulangan, gunakan rows_results jika ada
        rows_raw = rows_results or []
    elif rows_results:
        print(f"[WARN] Summary tidak tersedia. Menggunakan raw results: {results_path}")
        rows = rows_results
        rows_raw = rows_results
    else:
        print(f"[ERROR] Tidak ada data benchmark yang tersedia.")
        print(f"  Summary: {err_s}")
        print(f"  Raw results: {err_r}")
        print(f"\nJalankan terlebih dahulu: python run_benchmarks.py")
        sys.exit(1)

    print(f"\nMembuat grafik ke: {plots_dir}/")
    print("-" * 60)

    grafik_waktu(rows, plots_dir)
    grafik_speedup(rows, plots_dir)
    grafik_efisiensi(rows, plots_dir)
    grafik_pengaruh_data(rows, plots_dir)
    grafik_pengaruh_konfigurasi(rows, plots_dir)
    grafik_kualitas_data(rows, plots_dir)

    print("\n" + "=" * 60)
    print(f"Selesai! Semua grafik tersimpan di: {plots_dir}/")
    print("  grafik1_waktu_eksekusi.png")
    print("  grafik2_speedup.png")
    print("  grafik3_efisiensi.png")
    print("  grafik4_pengaruh_data.png")
    print("  grafik5_konfigurasi_paralel.png")
    print("  grafik6_kualitas_data.png")


if __name__ == "__main__":
    main()
