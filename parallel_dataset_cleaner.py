# Nama Lengkap : Muhammad Rezza
# NPM/NIM      : 247006111182
# Kelas        : G
#
# UTS Komputasi Paralel dan Terdistribusi - Parallel Dataset Cleaner
#
# Arsitektur hybrid (3 level paralelisme, sesuai Bagian A):
#   Level 1 (I/O)      : ThreadPoolExecutor  -> baca sub-chunk CSV per rank
#   Level 2 (CPU-bound): ProcessPoolExecutor -> bersihkan nilai kosong + hitung statistik
#   Level 3 (distributed): mpi4py (MPI)      -> bagi data ke rank (scatter), gabung hasil (gather)
#
# Parameter pribadi dari NIM 247006111182 (dipakai sebagai default):
#   random.seed(NIM)
#   Jumlah thread  = 2 digit terakhir NIM (82) mod 4 + 2 = 4
#   Jumlah proses  = 2 digit tengah NIM   (61) mod 3 + 2 = 3
#   Jumlah data    = 3 digit terakhir NIM (182) x 10     = 1820
#
# Catatan definisi "dua digit tengah": diambil dari posisi ke-6 dan ke-7
# (1-indexed) pada NIM 12 digit, karena soal tidak menjabarkan posisi
# persisnya dan contoh perhitungan pada soal mengandung kesalahan aritmetika
# (10 mod 3 + 2 ditulis = 4, padahal seharusnya = 3).

import argparse
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
    Dipanggil oleh tiap thread untuk sub-rentang baris yang berbeda."""
    rows = []
    with open(path, "r", newline="") as f:
        header = f.readline().strip().split(",")
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
        return [], []
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
    """Membagi rows ke n_proses proses untuk dibersihkan bersamaan."""
    if not rows:
        return [], {}, 0

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

    # gabungkan rata-rata antar sub-chunk (weighted mean)
    rata2_final = {}
    for k in kolom_numerik:
        total_val = sum(r * c for r, c in rata2_global[k])
        total_cnt = sum(c for _, c in rata2_global[k])
        rata2_final[k] = total_val / total_cnt if total_cnt > 0 else 0.0

    return cleaned_all, rata2_final, missing_total


def bersihkan_serial(header, rows, kolom_numerik):
    """Baseline serial (tanpa thread/proses) untuk menghitung speedup."""
    cleaned_rows, rata2, count, missing_terisi = bersihkan_subchunk((header, rows, kolom_numerik))
    return cleaned_rows, rata2, missing_terisi


def main():
    from mpi4py import MPI  # lihat catatan di atas: import ditunda agar worker "spawn" tidak memicu MPI_Init()

    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", default="dataset.csv")
    parser.add_argument("--n-thread", type=int, default=N_THREAD_DEFAULT)
    parser.add_argument("--n-proses", type=int, default=N_PROSES_DEFAULT)
    parser.add_argument("--n-data", type=int, default=N_DATA_DEFAULT)
    args = parser.parse_args()

    random.seed(NIM)

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # bagi baris data ke tiap rank (Level 3: distribusi antar rank)
    base = args.n_data // size
    rem = args.n_data % size
    row_start = rank * base + min(rank, rem)
    row_end = row_start + base + (1 if rank < rem else 0)

    # ---- baca chunk rank ini secara paralel (Level 1: Threads) ----
    t0 = time.time()
    header, rows = baca_chunk_paralel(args.data_path, row_start, row_end, args.n_thread)
    t_baca = time.time() - t0

    # ---- baseline serial (untuk speedup) ----
    t0 = time.time()
    _, _, _ = bersihkan_serial(header, rows, KOLOM_NUMERIK)
    t_serial = time.time() - t0

    # ---- bersihkan secara paralel (Level 2: ProcessPool) ----
    t0 = time.time()
    cleaned_rows, rata2_lokal, missing_lokal = bersihkan_paralel(
        header, rows, args.n_proses, KOLOM_NUMERIK
    )
    t_paralel = time.time() - t0

    waktu_total_lokal = t_baca + t_paralel

    # ---- Level 3: gabungkan hasil semua rank ke root ----
    hasil_lokal = {
        "rank": rank,
        "n_rows": len(rows),
        "missing_terisi": missing_lokal,
        "rata2": rata2_lokal,
        "t_serial": t_serial,
        "t_paralel": t_paralel,
        "t_baca": t_baca,
        "cleaned_rows": cleaned_rows,
    }
    semua_hasil = comm.gather(hasil_lokal, root=0)
    makespan = comm.reduce(waktu_total_lokal, op=MPI.MAX, root=0)
    total_serial_semua_rank = comm.reduce(t_serial + t_baca, op=MPI.SUM, root=0)

    if rank == 0:
        total_missing = sum(h["missing_terisi"] for h in semua_hasil)
        total_rows = sum(h["n_rows"] for h in semua_hasil)

        # gabungkan rata-rata antar rank (weighted mean)
        rata2_global = {}
        for k in KOLOM_NUMERIK:
            total_val = sum(h["rata2"].get(k, 0.0) * h["n_rows"] for h in semua_hasil)
            rata2_global[k] = total_val / total_rows if total_rows > 0 else 0.0

        speedup = total_serial_semua_rank / makespan if makespan > 0 else 0.0
        efisiensi = speedup / args.n_proses * 100  # relatif terhadap jumlah proses (CPU-bound dominan)

        print("=" * 60)
        print(f"Hybrid Project by: {NAMA} ({NIM})")
        print(f"Threads: {args.n_thread} | Processes: {args.n_proses} | Data: {total_rows}")
        print(f"Total Time: {makespan:.3f} s | Speedup: {speedup:.2f} | Efficiency: {efisiensi:.1f}%")
        print("=" * 60)
        print(f"MPI ranks digunakan : {size}")
        print(f"Total baris dibersihkan : {total_rows}")
        print(f"Total nilai kosong yang diisi : {total_missing}")
        print("Rata-rata tiap kolom (setelah dibersihkan):")
        for k, v in rata2_global.items():
            print(f"  - {k:22s}: {v:.2f}")

        # simpan hasil gabungan (data bersih dari seluruh rank, diurutkan per rank)
        semua_hasil_terurut = sorted(semua_hasil, key=lambda h: h["rank"])
        with open("dataset_cleaned.csv", "w", newline="") as f:
            f.write(",".join(header) + "\n")
            for h in semua_hasil_terurut:
                for row in h["cleaned_rows"]:
                    f.write(",".join(row) + "\n")
        print(f"\ndataset_cleaned.csv tersimpan ({total_rows} baris, nilai kosong sudah diisi).")

        print(f"CSV,{size},{args.n_thread},{args.n_proses},{total_rows},{makespan:.3f},{speedup:.2f},{efisiensi:.1f}")


if __name__ == "__main__":
    main()
