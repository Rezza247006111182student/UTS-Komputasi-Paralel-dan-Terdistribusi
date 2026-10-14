# Nama Lengkap : Muhammad Rezza
# NPM/NIM      : 247006111182
# Kelas        : G
#
# UTS Komputasi Paralel dan Terdistribusi - Parallel Dataset Cleaner
#
# Arsitektur hybrid (3 level paralelisme, sesuai Bagian A):
#   Level 1 (I/O)        : ThreadPoolExecutor  -> baca sub-chunk CSV per rank
#   Level 2 (CPU-bound)  : ProcessPoolExecutor -> bersihkan nilai kosong + hitung statistik
#   Level 3 (distributed): mpi4py (MPI)        -> bagi data ke rank (scatter), gabung hasil (gather)
#
# Parameter pribadi dari NIM 247006111182 (dipakai sebagai default):
#   random.seed(NIM)
#   Jumlah thread  = 2 digit terakhir NIM (82) mod 4 + 2 = 4
#   Jumlah proses  = 2 digit tengah NIM   (61) mod 3 + 2 = 3  (lihat catatan di bawah)
#   Jumlah data    = 3 digit terakhir NIM (182) x 10     = 1820
#
# Catatan definisi "dua digit tengah": diambil dari posisi ke-6 dan ke-7
# (1-indexed) pada NIM 12 digit, karena soal tidak menjabarkan posisi
# persisnya dan contoh perhitungan pada soal mengandung kesalahan aritmetika
# (10 mod 3 + 2 ditulis = 4, padahal seharusnya = 3).
#
# ─────────────────────────────────────────────────────────────────
# DEFINISI METRIK (untuk konsistensi laporan):
#
#   Waktu serial   = waktu bersihkan_serial (single process, no thread pool)
#                    yang dijalankan oleh masing-masing rank, lalu dijumlahkan
#                    antar rank (karena tiap rank punya beban kerjanya masing-masing).
#                    total_serial = SUM(t_serial_i + t_baca_i) untuk semua rank i
#
#   Waktu paralel  = makespan = MAX(t_baca_i + t_paralel_i) untuk semua rank i
#                    (waktu dinding / wall-clock time dari rank yang paling lambat)
#
#   Speedup        = total_serial / makespan
#                    (mengukur efektivitas paralelisme relatif terhadap pekerjaan
#                     yang sama jika dikerjakan serial satu per satu)
#
#   Efisiensi (%)  = (Speedup / n_proses) * 100
#                    (relatif terhadap jumlah proses CPU-bound — karena process pool
#                     adalah bottleneck komputasi utama)
#
#   Catatan: efisiensi tidak memperhitungkan jumlah MPI rank atau thread secara
#   eksplisit. Jika ingin efisiensi total worker, gunakan:
#   Efisiensi_total = (Speedup / (n_mpi_rank * n_proses)) * 100
#   (kolom efficiency_total_percent di CSV benchmark)
# ─────────────────────────────────────────────────────────────────

import argparse
import csv
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# PENTING (Windows): import "from mpi4py import MPI" SENGAJA tidak diletakkan
# di level modul, melainkan di dalam main(). Alasannya:
# ProcessPoolExecutor di Windows memakai metode "spawn" yang RE-IMPORT modul
# utama (file ini) sebagai "__mp_main__" pada tiap proses worker, BEPAPA pun
# fungsi target-nya berada di cleaner_worker.py. Padahal mengimpor mpi4py.MPI
# otomatis memicu MPI_Init(). Kalau import ini ada di level modul, setiap
# worker (yang TIDAK dijalankan lewat mpiexec, sehingga tidak punya konteks
# bootstrap) ikut memanggil MPI_Init() dan mpiexec gagal dengan error
# "failed to attach to a bootstrap queue". Dengan memindah import ke dalam
# main(), proses worker (di mana __name__ != "__main__") tidak akan pernah
# mengimpor mpi4py sama sekali, sehingga MPI_Init() hanya dipanggil oleh
# proses yang benar-benar diluncurkan mpiexec.
from cleaner_worker import bersihkan_subchunk

NAMA = "Muhammad Rezza"
NIM = 247006111182

N_THREAD_DEFAULT = 4
N_PROSES_DEFAULT = 3
N_DATA_DEFAULT = 1820

KOLOM_NUMERIK = ["suhu_c", "kelembaban_persen", "tekanan_hpa", "kecepatan_angin_kmh"]


def baca_baris_csv(path, start, end):
    """Level 1 (I/O-bound): baca baris [start, end) dari file CSV.
    Dipanggil oleh tiap thread untuk sub-rentang baris yang berbeda.
    Indeks baris dihitung dari baris data (setelah header, 0-indexed)."""
    rows = []
    with open(path, "r", newline="") as f:
        header_line = f.readline().strip()
        header = header_line.split(",")
        for idx, line in enumerate(f):
            if start <= idx < end:
                rows.append(line.strip().split(","))
            elif idx >= end:
                break
    return header, rows


def baca_chunk_paralel(path, row_start, row_end, n_thread):
    """Membagi rentang baris [row_start, row_end) ke n_thread thread,
    masing-masing membaca bagiannya sendiri secara bersamaan (I/O-bound)."""
    total = row_end - row_start
    if total <= 0:
        # Kembalikan header saja jika tidak ada data
        with open(path, "r", newline="") as f:
            header = f.readline().strip().split(",")
        return header, []
    step = max(1, total // n_thread)
    ranges = []
    s = row_start
    while s < row_end:
        e = min(s + step, row_end)
        ranges.append((s, e))
        s = e

    header = None
    all_rows = []
    with ThreadPoolExecutor(max_workers=n_thread) as ex:
        futs = [ex.submit(baca_baris_csv, path, a, b) for a, b in ranges]
        for fut in futs:
            h, rows = fut.result()
            header = h
            all_rows.extend(rows)
    return header, all_rows


def bersihkan_paralel(header, rows, n_proses, kolom_numerik):
    """Membagi rows ke n_proses proses untuk dibersihkan bersamaan.

    Catatan rata-rata:
    Setiap proses menghitung rata-rata dari sub-chunk-nya sendiri
    dan menggunakan nilai tersebut untuk mengisi nilai kosong dalam
    sub-chunk tersebut. Ini menyebabkan imputasi sedikit berbeda
    per sub-chunk (bukan rata-rata global). Di level ini, kita
    menghitung weighted mean global hanya untuk pelaporan statistik.
    """
    if not rows:
        return [], {k: 0.0 for k in kolom_numerik}, 0

    chunk_size = max(1, len(rows) // n_proses)
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]
    tasks = [(header, chunk, kolom_numerik) for chunk in chunks]

    cleaned_all = []
    rata2_global = {k: [] for k in kolom_numerik}
    count_global = {k: 0 for k in kolom_numerik}
    missing_total = 0

    with ProcessPoolExecutor(max_workers=n_proses) as ex:
        for cleaned_rows, rata2, count, missing_terisi in ex.map(bersihkan_subchunk, tasks):
            cleaned_all.extend(cleaned_rows)
            missing_total += missing_terisi
            for k in kolom_numerik:
                if count[k] > 0:
                    rata2_global[k].append((rata2[k], count[k]))
                count_global[k] += count[k]

    # gabungkan rata-rata antar sub-chunk (weighted mean untuk pelaporan)
    rata2_final = {}
    for k in kolom_numerik:
        total_val = sum(r * c for r, c in rata2_global[k])
        total_cnt = sum(c for _, c in rata2_global[k])
        rata2_final[k] = total_val / total_cnt if total_cnt > 0 else 0.0

    return cleaned_all, rata2_final, missing_total


def bersihkan_serial(header, rows, kolom_numerik):
    """Baseline serial (tanpa thread/proses) untuk menghitung speedup.
    Menggunakan fungsi yang sama dengan ProcessPool (bersihkan_subchunk)
    agar perbandingan adil — hanya bedanya dijalankan single-process."""
    if not rows:
        return [], {k: 0.0 for k in kolom_numerik}, 0
    cleaned_rows, rata2, count, missing_terisi = bersihkan_subchunk((header, rows, kolom_numerik))
    return cleaned_rows, rata2, missing_terisi


def hitung_missing_awal(path, row_start, row_end, kolom_numerik):
    """Hitung jumlah nilai kosong di dataset sebelum pembersihan (untuk Grafik 6)."""
    missing_per_kolom = {k: 0 for k in kolom_numerik}
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        idx_kolom = {k: header.index(k) for k in kolom_numerik if k in header}
        for idx, row in enumerate(reader):
            if row_start <= idx < row_end:
                for k, i in idx_kolom.items():
                    if i < len(row) and row[i].strip() == "":
                        missing_per_kolom[k] += 1
            elif idx >= row_end:
                break
    return missing_per_kolom


def main():
    from mpi4py import MPI  # type: ignore[import]  # lihat catatan di atas: import ditunda agar worker "spawn" tidak memicu MPI_Init()

    parser = argparse.ArgumentParser(
        description="Parallel Dataset Cleaner — Hybrid MPI + Thread + ProcessPool"
    )
    parser.add_argument("--data-path", default="dataset.csv",
                        help="Path ke file dataset CSV (default: dataset.csv)")
    parser.add_argument("--n-thread", type=int, default=N_THREAD_DEFAULT,
                        help=f"Jumlah thread I/O (default: {N_THREAD_DEFAULT})")
    parser.add_argument("--n-proses", type=int, default=N_PROSES_DEFAULT,
                        help=f"Jumlah proses CPU-bound (default: {N_PROSES_DEFAULT})")
    parser.add_argument("--n-data", type=int, default=N_DATA_DEFAULT,
                        help=f"Jumlah baris yang diproses (default: {N_DATA_DEFAULT})")
    parser.add_argument("--output-csv", default="dataset_cleaned.csv",
                        help="Path output CSV hasil pembersihan (default: dataset_cleaned.csv)")
    parser.add_argument("--benchmark-out", default=None,
                        help="Jika diisi, append satu baris hasil ke file CSV ini (untuk run_benchmarks.py)")
    parser.add_argument("--experiment-id", default=None,
                        help="ID eksperimen untuk kolom benchmark CSV")
    parser.add_argument("--repeat-id", type=int, default=1,
                        help="Nomor pengulangan eksperimen (default: 1)")
    args = parser.parse_args()

    random.seed(NIM)

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # ── Validasi: pastikan file input ada ──
    if rank == 0 and not os.path.exists(args.data_path):
        print(f"[ERROR] File tidak ditemukan: {args.data_path}")
        print("Jalankan terlebih dahulu: python generate_dataset.py")
        comm.Abort(1)

    # ── Sinkronisasi setelah validasi ──
    comm.Barrier()

    # ── Hitung jumlah baris aktual di file ──
    if rank == 0:
        with open(args.data_path, "r", newline="") as f:
            actual_rows = sum(1 for _ in f) - 1  # kurangi header
    else:
        actual_rows = None
    actual_rows = comm.bcast(actual_rows, root=0)

    # Batasi n_data ke jumlah baris yang benar-benar tersedia
    n_data_efektif = min(args.n_data, actual_rows)
    if rank == 0 and n_data_efektif < args.n_data:
        print(f"[PERINGATAN] Dataset hanya memiliki {actual_rows} baris. "
              f"n_data dikurangi dari {args.n_data} menjadi {n_data_efektif}.")

    # ── Bagi baris data ke tiap rank (Level 3: distribusi antar rank) ──
    base = n_data_efektif // size
    rem = n_data_efektif % size
    row_start = rank * base + min(rank, rem)
    row_end = row_start + base + (1 if rank < rem else 0)

    # ── Hitung missing awal (untuk statistik kualitas data) ──
    missing_awal_lokal = hitung_missing_awal(args.data_path, row_start, row_end, KOLOM_NUMERIK)

    # ── Baca chunk rank ini secara paralel (Level 1: Threads) ──
    t0 = time.time()
    header, rows = baca_chunk_paralel(args.data_path, row_start, row_end, args.n_thread)
    t_baca = time.time() - t0

    if not header:
        # Fallback jika header tidak terbaca (rank tidak mendapat data)
        with open(args.data_path, "r", newline="") as f:
            header = f.readline().strip().split(",")

    # ── Baseline serial (untuk speedup) ──
    # Menggunakan data yang sudah dibaca oleh rank ini (setara dengan pekerjaan nyata)
    t0 = time.time()
    _, _, _ = bersihkan_serial(header, rows, KOLOM_NUMERIK)
    t_serial = time.time() - t0

    # ── Bersihkan secara paralel (Level 2: ProcessPool) ──
    t0 = time.time()
    cleaned_rows, rata2_lokal, missing_lokal = bersihkan_paralel(
        header, rows, args.n_proses, KOLOM_NUMERIK
    )
    t_paralel = time.time() - t0

    waktu_total_lokal = t_baca + t_paralel

    # ── Level 3: gabungkan hasil semua rank ke root ──
    hasil_lokal = {
        "rank": rank,
        "n_rows": len(rows),
        "n_rows_bersih": len(cleaned_rows),
        "missing_awal": missing_awal_lokal,
        "missing_terisi": missing_lokal,
        "rata2": rata2_lokal,
        "t_serial": t_serial,
        "t_paralel": t_paralel,
        "t_baca": t_baca,
        "cleaned_rows": cleaned_rows,
    }
    semua_hasil = comm.gather(hasil_lokal, root=0)
    makespan = comm.reduce(waktu_total_lokal, op=MPI.MAX, root=0)
    # total_serial = jumlah waktu serial SEMUA rank (termasuk waktu baca)
    # ini merepresentasikan total pekerjaan jika dilakukan serial satu per satu
    total_serial_semua_rank = comm.reduce(t_serial + t_baca, op=MPI.SUM, root=0)

    if rank == 0:
        total_missing_awal = {k: sum(h["missing_awal"].get(k, 0) for h in semua_hasil) for k in KOLOM_NUMERIK}
        total_missing_awal_semua = sum(total_missing_awal.values())

        total_missing = sum(h["missing_terisi"] for h in semua_hasil)
        total_rows = sum(h["n_rows"] for h in semua_hasil)
        total_rows_bersih = sum(h["n_rows_bersih"] for h in semua_hasil)

        # gabungkan rata-rata antar rank (weighted mean berdasarkan n_rows tiap rank)
        rata2_global = {}
        for k in KOLOM_NUMERIK:
            total_val = sum(h["rata2"].get(k, 0.0) * h["n_rows"] for h in semua_hasil)
            rata2_global[k] = total_val / total_rows if total_rows > 0 else 0.0

        # ── Metrik performa ──
        # Speedup = total pekerjaan serial / waktu paralel dinding (makespan)
        speedup = total_serial_semua_rank / makespan if makespan > 0 else 0.0
        # Efisiensi relatif terhadap jumlah proses (CPU-bound dominan)
        efisiensi = speedup / args.n_proses * 100
        # Efisiensi total (memperhitungkan semua worker: rank * proses)
        total_workers = size * args.n_proses
        efisiensi_total = speedup / total_workers * 100

        print("=" * 60)
        print(f"Hybrid Project by: {NAMA} ({NIM})")
        print(f"MPI Ranks: {size} | Threads: {args.n_thread} | Processes: {args.n_proses}")
        print(f"Data diminta: {args.n_data} | Data diproses: {total_rows}")
        print(f"Serial Time : {total_serial_semua_rank:.4f} s (SUM semua rank)")
        print(f"Parallel Time: {makespan:.4f} s (makespan/wall-clock)")
        print(f"Speedup     : {speedup:.3f}")
        print(f"Efficiency  : {efisiensi:.1f}% (relatif n_proses={args.n_proses})")
        print(f"Eff. Total  : {efisiensi_total:.1f}% (relatif n_rank*n_proses={total_workers})")
        print("=" * 60)
        print(f"Total baris dibersihkan    : {total_rows}")
        print(f"Total nilai kosong awal    : {total_missing_awal_semua}")
        print(f"Total nilai kosong terisi  : {total_missing}")
        missing_tersisa = total_missing_awal_semua - total_missing
        print(f"Nilai kosong tersisa       : {missing_tersisa}")
        print("Rata-rata tiap kolom (setelah dibersihkan):")
        for k, v in rata2_global.items():
            print(f"  - {k:25s}: {v:.4f}")

        # ── Simpan hasil gabungan ──
        semua_hasil_terurut = sorted(semua_hasil, key=lambda h: h["rank"])
        os.makedirs(os.path.dirname(args.output_csv) if os.path.dirname(args.output_csv) else ".", exist_ok=True)
        with open(args.output_csv, "w", newline="") as f:
            f.write(",".join(header) + "\n")
            for h in semua_hasil_terurut:
                for row in h["cleaned_rows"]:
                    f.write(",".join(row) + "\n")
        print(f"\n{args.output_csv} tersimpan ({total_rows_bersih} baris, nilai kosong sudah diisi).")

        # ── Baris ringkas CSV (backward compatible) ──
        csv_line = (f"CSV,{size},{args.n_thread},{args.n_proses},{total_rows},"
                    f"{makespan:.4f},{speedup:.3f},{efisiensi:.1f},{efisiensi_total:.1f},"
                    f"{total_missing_awal_semua},{total_missing}")
        print(csv_line)

        # ── Append ke benchmark CSV jika diminta ──
        if args.benchmark_out:
            exp_id = args.experiment_id or f"exp_{size}r_{args.n_thread}t_{args.n_proses}p_{total_rows}d"
            os.makedirs(os.path.dirname(args.benchmark_out) if os.path.dirname(args.benchmark_out) else ".", exist_ok=True)
            file_baru = not os.path.exists(args.benchmark_out)
            with open(args.benchmark_out, "a", newline="") as bf:
                writer = csv.writer(bf)
                if file_baru:
                    writer.writerow([
                        "experiment_id", "mpi_ranks", "threads", "processes",
                        "data_rows", "repeat",
                        "serial_time_seconds", "parallel_time_seconds",
                        "speedup", "efficiency_percent", "efficiency_total_percent",
                        "rows_processed", "missing_values_before", "missing_values_filled",
                        "status"
                    ])
                writer.writerow([
                    exp_id, size, args.n_thread, args.n_proses,
                    args.n_data, args.repeat_id,
                    round(total_serial_semua_rank, 6), round(makespan, 6),
                    round(speedup, 4), round(efisiensi, 2), round(efisiensi_total, 2),
                    total_rows, total_missing_awal_semua, total_missing,
                    "success"
                ])
            print(f"Hasil benchmark ditambahkan ke: {args.benchmark_out}")


if __name__ == "__main__":
    main()
