# Nama Lengkap : Muhammad Rezza
# NPM/NIM      : 247006111182
# Kelas        : G
#
# run_benchmarks.py — Sistem Benchmark Otomatis
#
# Script ini menjalankan beberapa konfigurasi eksperimen secara terkontrol,
# mengumpulkan hasil pengukuran waktu/speedup/efisiensi, dan menyimpannya
# ke benchmark_results.csv dan benchmark_summary.csv.
#
# Cara menjalankan:
#   python run_benchmarks.py
#   python run_benchmarks.py --repeats 3
#   python run_benchmarks.py --results-dir results
#
# CATATAN:
#   - Script ini memanggil mpiexec untuk setiap konfigurasi.
#   - Pastikan MS-MPI sudah terinstal (mpiexec --help harus jalan).
#   - Konfigurasi dengan n_rank > 1 memerlukan MPI yang berjalan dengan benar.
#   - Jika suatu konfigurasi gagal, hasilnya dicatat sebagai "failed" dan
#     eksperimen lain dilanjutkan.
#   - Jangan mengarang hasil: semua data berasal dari output program nyata.

import argparse
import csv
import os
import platform
import statistics
import subprocess
import sys
import time

# ─────────────────────────────────────────────────────────────────
# Konfigurasi eksperimen
# Setiap entry adalah dict dengan kunci:
#   name       : label eksperimen (string unik)
#   n_rank     : jumlah MPI rank
#   n_thread   : jumlah thread I/O
#   n_proses   : jumlah proses CPU-bound
#   n_data     : jumlah baris dataset yang diproses
#   description: keterangan singkat
# ─────────────────────────────────────────────────────────────────
EKSPERIMEN = [
    # --- Eksperimen dasar (sesuai parameter default NIM) ---
    {
        "name": "E1_baseline_serial",
        "n_rank": 1, "n_thread": 1, "n_proses": 1, "n_data": 1820,
        "description": "Mendekati serial: 1 rank, 1 thread, 1 proses, 1820 baris"
    },
    {
        "name": "E2_rank1_t2_p2",
        "n_rank": 1, "n_thread": 2, "n_proses": 2, "n_data": 1820,
        "description": "1 rank, 2 thread, 2 proses, 1820 baris"
    },
    {
        "name": "E3_rank1_t4_p3",
        "n_rank": 1, "n_thread": 4, "n_proses": 3, "n_data": 1820,
        "description": "1 rank, 4 thread, 3 proses, 1820 baris (parameter default NIM)"
    },
    {
        "name": "E4_rank2_t4_p3",
        "n_rank": 2, "n_thread": 4, "n_proses": 3, "n_data": 1820,
        "description": "2 rank, 4 thread, 3 proses, 1820 baris"
    },
    # --- Variasi jumlah data ---
    {
        "name": "E5_rank1_t4_p3_5000",
        "n_rank": 1, "n_thread": 4, "n_proses": 3, "n_data": 5000,
        "description": "1 rank, 4 thread, 3 proses, 5000 baris"
    },
    {
        "name": "E6_rank1_t4_p3_10000",
        "n_rank": 1, "n_thread": 4, "n_proses": 3, "n_data": 10000,
        "description": "1 rank, 4 thread, 3 proses, 10000 baris"
    },
    # --- Isolasi pengaruh jumlah proses ---
    {
        "name": "E7_rank1_t4_p1_1820",
        "n_rank": 1, "n_thread": 4, "n_proses": 1, "n_data": 1820,
        "description": "1 rank, 4 thread, 1 proses (isolasi pengaruh proses), 1820 baris"
    },
    {
        "name": "E8_rank1_t4_p2_1820",
        "n_rank": 1, "n_thread": 4, "n_proses": 2, "n_data": 1820,
        "description": "1 rank, 4 thread, 2 proses (isolasi pengaruh proses), 1820 baris"
    },
    # --- Isolasi pengaruh jumlah thread ---
    {
        "name": "E9_rank1_t1_p3_1820",
        "n_rank": 1, "n_thread": 1, "n_proses": 3, "n_data": 1820,
        "description": "1 rank, 1 thread, 3 proses (isolasi pengaruh thread), 1820 baris"
    },
    {
        "name": "E10_rank1_t2_p3_1820",
        "n_rank": 1, "n_thread": 2, "n_proses": 3, "n_data": 1820,
        "description": "1 rank, 2 thread, 3 proses (isolasi pengaruh thread), 1820 baris"
    },
]

KOLOM_HASIL = [
    "experiment_id", "mpi_ranks", "threads", "processes",
    "data_rows", "repeat",
    "serial_time_seconds", "parallel_time_seconds",
    "speedup", "efficiency_percent", "efficiency_total_percent",
    "rows_processed", "missing_values_before", "missing_values_filled",
    "status"
]


def cari_mpiexec():
    """Cari path mpiexec yang tersedia di sistem."""
    # Lokasi umum MS-MPI di Windows
    kandidat = [
        "mpiexec",
        r"C:\Program Files\Microsoft MPI\Bin\mpiexec.exe",
        r"C:\Program Files (x86)\Microsoft MPI\Bin\mpiexec.exe",
    ]
    for k in kandidat:
        try:
            result = subprocess.run(
                [k, "--help"] if k == "mpiexec" else [k, "-help"],
                capture_output=True, timeout=5
            )
            return k
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    return None


def cek_dataset(n_data, data_path="dataset.csv"):
    """Pastikan dataset tersedia dengan jumlah baris yang cukup.
    Generate ulang jika perlu."""
    if not os.path.exists(data_path):
        print(f"[INFO] {data_path} tidak ditemukan. Membuat dataset baru ({n_data} baris)...")
        hasil = subprocess.run(
            [sys.executable, "generate_dataset.py", "--n-data", str(n_data)],
            capture_output=True, text=True
        )
        if hasil.returncode != 0:
            print(f"[ERROR] Gagal membuat dataset: {hasil.stderr}")
            return False
        print(hasil.stdout.strip())
        return True

    # Hitung baris aktual
    with open(data_path, "r", newline="") as f:
        actual = sum(1 for _ in f) - 1  # kurangi header
    if actual < n_data:
        print(f"[INFO] Dataset hanya {actual} baris, dibutuhkan {n_data}. Membuat ulang...")
        hasil = subprocess.run(
            [sys.executable, "generate_dataset.py", "--n-data", str(n_data)],
            capture_output=True, text=True
        )
        if hasil.returncode != 0:
            print(f"[ERROR] Gagal membuat dataset: {hasil.stderr}")
            return False
        print(hasil.stdout.strip())
    return True


def jalankan_eksperimen(exp, repeat_id, mpiexec_cmd, results_csv, timeout_s=120):
    """Jalankan satu konfigurasi eksperimen dan kembalikan dict hasil.

    Mengembalikan dict dengan semua kolom KOLOM_HASIL.
    Jika gagal, kolom numerik diisi None dan status berisi pesan error.
    """
    name = exp["name"]
    n_rank = exp["n_rank"]
    n_thread = exp["n_thread"]
    n_proses = exp["n_proses"]
    n_data = exp["n_data"]

    cmd = [
        mpiexec_cmd, "-n", str(n_rank),
        sys.executable, "parallel_dataset_cleaner.py",
        "--n-thread", str(n_thread),
        "--n-proses", str(n_proses),
        "--n-data", str(n_data),
        "--output-csv", f"results/cleaned_{name}_rep{repeat_id}.csv",
        "--benchmark-out", results_csv,
        "--experiment-id", name,
        "--repeat-id", str(repeat_id),
    ]

    print(f"  [RUN] {name} (rep {repeat_id}) | cmd: {' '.join(cmd)}")
    t_wall_start = time.time()

    try:
        hasil = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        t_wall = time.time() - t_wall_start

        if hasil.returncode != 0:
            err_msg = (hasil.stderr or "").strip()[:200]
            print(f"  [FAIL] Return code {hasil.returncode}: {err_msg}")
            return {
                "experiment_id": name, "mpi_ranks": n_rank, "threads": n_thread,
                "processes": n_proses, "data_rows": n_data, "repeat": repeat_id,
                "serial_time_seconds": None, "parallel_time_seconds": None,
                "speedup": None, "efficiency_percent": None, "efficiency_total_percent": None,
                "rows_processed": None, "missing_values_before": None,
                "missing_values_filled": None,
                "status": f"failed:returncode={hasil.returncode}"
            }

        # Parse baris CSV dari output program
        csv_line = None
        for line in hasil.stdout.splitlines():
            if line.startswith("CSV,"):
                csv_line = line
                break

        if csv_line is None:
            print(f"  [WARN] Output tidak mengandung baris CSV ringkas.")
            print(f"  STDOUT: {hasil.stdout[:300]}")
            return {
                "experiment_id": name, "mpi_ranks": n_rank, "threads": n_thread,
                "processes": n_proses, "data_rows": n_data, "repeat": repeat_id,
                "serial_time_seconds": None, "parallel_time_seconds": None,
                "speedup": None, "efficiency_percent": None, "efficiency_total_percent": None,
                "rows_processed": None, "missing_values_before": None,
                "missing_values_filled": None,
                "status": "failed:no_csv_line"
            }

        # Format: CSV,ranks,threads,proses,rows,makespan,speedup,eff,eff_total,missing_before,missing_filled
        parts = csv_line.split(",")
        try:
            result = {
                "experiment_id": name,
                "mpi_ranks": int(parts[1]),
                "threads": int(parts[2]),
                "processes": int(parts[3]),
                "data_rows": n_data,
                "repeat": repeat_id,
                "serial_time_seconds": None,  # diambil dari benchmark CSV
                "parallel_time_seconds": float(parts[5]),
                "speedup": float(parts[6]),
                "efficiency_percent": float(parts[7]),
                "efficiency_total_percent": float(parts[8]) if len(parts) > 8 else None,
                "rows_processed": int(parts[4]),
                "missing_values_before": int(parts[9]) if len(parts) > 9 else None,
                "missing_values_filled": int(parts[10]) if len(parts) > 10 else None,
                "status": "success"
            }
        except (IndexError, ValueError) as e:
            print(f"  [WARN] Gagal parse CSV line: {e} | line: {csv_line}")
            result = {
                "experiment_id": name, "mpi_ranks": n_rank, "threads": n_thread,
                "processes": n_proses, "data_rows": n_data, "repeat": repeat_id,
                "serial_time_seconds": None, "parallel_time_seconds": None,
                "speedup": None, "efficiency_percent": None, "efficiency_total_percent": None,
                "rows_processed": None, "missing_values_before": None,
                "missing_values_filled": None,
                "status": f"failed:parse_error:{e}"
            }

        print(f"  [OK]   wall={t_wall:.2f}s | "
              f"parallel={result.get('parallel_time_seconds', 'N/A')}s | "
              f"speedup={result.get('speedup', 'N/A')} | "
              f"eff={result.get('efficiency_percent', 'N/A')}%")
        return result

    except subprocess.TimeoutExpired:
        print(f"  [TIMEOUT] Melebihi {timeout_s}s")
        return {
            "experiment_id": name, "mpi_ranks": n_rank, "threads": n_thread,
            "processes": n_proses, "data_rows": n_data, "repeat": repeat_id,
            "serial_time_seconds": None, "parallel_time_seconds": None,
            "speedup": None, "efficiency_percent": None, "efficiency_total_percent": None,
            "rows_processed": None, "missing_values_before": None,
            "missing_values_filled": None,
            "status": f"failed:timeout>{timeout_s}s"
        }
    except Exception as e:
        print(f"  [ERROR] {e}")
        return {
            "experiment_id": name, "mpi_ranks": n_rank, "threads": n_thread,
            "processes": n_proses, "data_rows": n_data, "repeat": repeat_id,
            "serial_time_seconds": None, "parallel_time_seconds": None,
            "speedup": None, "efficiency_percent": None, "efficiency_total_percent": None,
            "rows_processed": None, "missing_values_before": None,
            "missing_values_filled": None,
            "status": f"failed:{type(e).__name__}:{e}"
        }


def buat_ringkasan(results_csv, summary_csv):
    """Baca benchmark_results.csv dan hitung rata-rata + std dev per eksperimen."""
    if not os.path.exists(results_csv):
        print(f"[WARN] {results_csv} tidak ditemukan, ringkasan tidak dibuat.")
        return

    data = {}
    with open(results_csv, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("status") != "success":
                continue
            exp_id = row["experiment_id"]
            if exp_id not in data:
                data[exp_id] = {
                    "mpi_ranks": row["mpi_ranks"],
                    "threads": row["threads"],
                    "processes": row["processes"],
                    "data_rows": row["data_rows"],
                    "serial_times": [],
                    "parallel_times": [],
                    "speedups": [],
                    "efficiencies": [],
                    "efficiencies_total": [],
                    "rows_processed": row.get("rows_processed"),
                    "missing_before": row.get("missing_values_before"),
                    "missing_filled": [],
                }
            try:
                if row.get("serial_time_seconds"):
                    data[exp_id]["serial_times"].append(float(row["serial_time_seconds"]))
                data[exp_id]["parallel_times"].append(float(row["parallel_time_seconds"]))
                data[exp_id]["speedups"].append(float(row["speedup"]))
                data[exp_id]["efficiencies"].append(float(row["efficiency_percent"]))
                if row.get("efficiency_total_percent"):
                    data[exp_id]["efficiencies_total"].append(float(row["efficiency_total_percent"]))
                if row.get("missing_values_filled"):
                    data[exp_id]["missing_filled"].append(int(row["missing_values_filled"]))
            except (ValueError, KeyError):
                pass

    if not data:
        print("[WARN] Tidak ada eksperimen berhasil untuk dirangkum.")
        return

    def safe_mean(lst):
        return statistics.mean(lst) if lst else None

    def safe_stdev(lst):
        return statistics.stdev(lst) if len(lst) >= 2 else None

    with open(summary_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "experiment_id", "mpi_ranks", "threads", "processes", "data_rows",
            "n_repeats",
            "mean_serial_time_s",
            "mean_parallel_time_s", "stdev_parallel_time_s",
            "mean_speedup", "stdev_speedup",
            "mean_efficiency_percent", "stdev_efficiency_percent",
            "mean_efficiency_total_percent",
            "rows_processed", "missing_values_before", "mean_missing_filled"
        ])
        for exp_id, d in data.items():
            writer.writerow([
                exp_id,
                d["mpi_ranks"], d["threads"], d["processes"], d["data_rows"],
                len(d["parallel_times"]),
                round(safe_mean(d["serial_times"]) or 0, 6) if d["serial_times"] else "",
                round(safe_mean(d["parallel_times"]) or 0, 6),
                round(safe_stdev(d["parallel_times"]) or 0, 6) if safe_stdev(d["parallel_times"]) else "",
                round(safe_mean(d["speedups"]) or 0, 4),
                round(safe_stdev(d["speedups"]) or 0, 4) if safe_stdev(d["speedups"]) else "",
                round(safe_mean(d["efficiencies"]) or 0, 2),
                round(safe_stdev(d["efficiencies"]) or 0, 2) if safe_stdev(d["efficiencies"]) else "",
                round(safe_mean(d["efficiencies_total"]) or 0, 2) if d["efficiencies_total"] else "",
                d["rows_processed"],
                d["missing_before"],
                round(safe_mean(d["missing_filled"]) or 0, 1) if d["missing_filled"] else "",
            ])

    print(f"\nRingkasan disimpan ke: {summary_csv} ({len(data)} eksperimen berhasil)")


def main():
    # Paksa encoding UTF-8 agar karakter unicode tampil di terminal Windows
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass  # Python < 3.7

    parser = argparse.ArgumentParser(
        description="Benchmark otomatis Parallel Dataset Cleaner"
    )
    parser.add_argument("--repeats", type=int, default=3,
                        help="Jumlah pengulangan tiap konfigurasi (default: 3)")
    parser.add_argument("--results-dir", default="results",
                        help="Folder output (default: results)")
    parser.add_argument("--timeout", type=int, default=120,
                        help="Timeout per eksperimen dalam detik (default: 120)")
    parser.add_argument("--skip-generate", action="store_true",
                        help="Jangan generate ulang dataset meski tidak cukup")
    args = parser.parse_args()

    os.makedirs(args.results_dir, exist_ok=True)

    results_csv = os.path.join(args.results_dir, "benchmark_results.csv")
    summary_csv = os.path.join(args.results_dir, "benchmark_summary.csv")

    print("=" * 65)
    print("  Benchmark Otomatis — Parallel Dataset Cleaner")
    print(f"  Nama: Muhammad Rezza | NIM: 247006111182 | Kelas: G")
    print(f"  Sistem: {platform.system()} {platform.version()}")
    print(f"  Python: {sys.version.split()[0]}")
    print(f"  Pengulangan per eksperimen: {args.repeats}")
    print(f"  Output: {args.results_dir}/")
    print("=" * 65)

    # Cek mpiexec
    mpiexec = cari_mpiexec()
    if mpiexec is None:
        print("[ERROR] mpiexec tidak ditemukan. Pastikan MS-MPI sudah terinstal.")
        print("  Download: https://www.microsoft.com/en-us/download/details.aspx?id=57467")
        sys.exit(1)
    print(f"[OK] mpiexec ditemukan: {mpiexec}\n")

    # Tentukan jumlah baris maksimum yang dibutuhkan
    max_n_data = max(e["n_data"] for e in EKSPERIMEN)

    # Pastikan dataset cukup
    if not args.skip_generate:
        ok = cek_dataset(max_n_data)
        if not ok:
            print("[ERROR] Dataset tidak tersedia. Hentikan benchmark.")
            sys.exit(1)

    # Inisialisasi file hasil (hapus jika ada agar tidak menumpuk)
    if os.path.exists(results_csv):
        ts = time.strftime("%Y%m%d_%H%M%S")
        backup = results_csv.replace(".csv", f"_backup_{ts}.csv")
        os.rename(results_csv, backup)
        print(f"[INFO] Hasil lama dibackup ke: {backup}")

    # Tulis header CSV hasil
    with open(results_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(KOLOM_HASIL)

    # ── Jalankan semua eksperimen ──
    semua_hasil = []
    total_exp = len(EKSPERIMEN) * args.repeats

    print(f"\nTotal eksperimen yang akan dijalankan: {total_exp}")
    print("-" * 65)

    for i, exp in enumerate(EKSPERIMEN, 1):
        print(f"\n[{i}/{len(EKSPERIMEN)}] {exp['name']}")
        print(f"  Deskripsi: {exp['description']}")
        hasil_exp = []
        for rep in range(1, args.repeats + 1):
            result = jalankan_eksperimen(
                exp, rep, mpiexec, results_csv, timeout_s=args.timeout
            )
            semua_hasil.append(result)
            hasil_exp.append(result)
            time.sleep(0.5)  # jeda singkat antar pengulangan

        # Ringkasan cepat per eksperimen
        success_times = [r["parallel_time_seconds"] for r in hasil_exp
                         if r["status"] == "success" and r["parallel_time_seconds"] is not None]
        if success_times:
            mean_t = statistics.mean(success_times)
            print(f"  -> Rata-rata waktu paralel: {mean_t:.4f}s ({len(success_times)}/{args.repeats} berhasil)")
        else:
            print(f"  -> Semua pengulangan GAGAL")

    print("\n" + "=" * 65)
    n_success = sum(1 for r in semua_hasil if r["status"] == "success")
    n_failed = len(semua_hasil) - n_success
    print(f"Selesai: {n_success} berhasil, {n_failed} gagal dari {len(semua_hasil)} total")

    # Buat ringkasan
    buat_ringkasan(results_csv, summary_csv)

    print(f"\nFile output:")
    print(f"  - {results_csv}    (semua pengulangan)")
    print(f"  - {summary_csv}  (rata-rata per konfigurasi)")
    print(f"\nJalankan visualisasi dengan:")
    print(f"  python plot_benchmarks.py")


if __name__ == "__main__":
    main()
