# Nama Lengkap : Muhammad Rezza
# NPM/NIM      : 247006111182
# Kelas        : G
#
# UTS Komputasi Paralel dan Terdistribusi - Parallel Dataset Cleaner
#
# Arsitektur hybrid (3 level paralelisme):
#   Level 1 (I/O)         : ThreadPoolExecutor  -> baca sub-chunk CSV per rank
#   Level 2 (CPU-bound)   : ProcessPoolExecutor -> hitung statistik & isi nilai kosong
#   Level 3 (distributed) : mpi4py (MPI)        -> bagi data antar rank, gabung hasil
#
# Parameter pribadi dari NIM 247006111182 (default):
#   random.seed(NIM)
#   Thread = 2 digit terakhir (82) mod 4 + 2 = 4
#   Proses = 2 digit tengah   (61) mod 3 + 2 = 3
#   Data   = 3 digit terakhir (182) x 10     = 1820
#   ("dua digit tengah" = posisi ke-6 & 7 dari 12 digit; contoh hitungan pada
#    soal 10 mod 3 + 2 = 4 keliru secara aritmetika, seharusnya 3.)
#
# DEFINISI METRIK (konsisten untuk semua percobaan):
#   T_serial   = waktu menjalankan SELURUH pekerjaan secara serial pada 1 proses
#                (baca CSV 1 thread + hitung statistik + isi nilai kosong),
#                diukur di rank 0 sebelum bagian paralel dimulai.
#   T_paralel  = waktu dinding (wall-clock) seluruh pipeline hybrid dari awal
#                sampai hasil terkumpul di root: baca (thread) + fase 1 & 2
#                (ProcessPool) + komunikasi MPI. Diukur antar dua Barrier agar
#                semua rank dihitung bersamaan.
#   Speedup    = T_serial / T_paralel
#   Efisiensi  = Speedup / (jumlah_rank x jumlah_proses) x 100%
#                (rank x proses = total unit pemrosesan CPU paralel; thread
#                hanya menumpuk waktu tunggu I/O sehingga tidak dihitung)
#
# VALIDASI: hasil pembersihan paralel dibandingkan sel demi sel dengan hasil
# serial; program mencetak "OK" jika identik.
#
# PENTING (Windows): "from mpi4py import MPI" ada di dalam main(), bukan di
# level modul, karena ProcessPoolExecutor ("spawn") mengimpor ulang file ini
# di tiap worker dan import mpi4py akan memicu MPI_Init() lagi.

import argparse
import random
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from cleaner_worker import (hitung_parsial, gabung_statistik,
                            hitung_rata2, isi_kosong)

NAMA = "Muhammad Rezza"
NIM = 247006111182

N_THREAD_DEFAULT = 4
N_PROSES_DEFAULT = 3
N_DATA_DEFAULT = 1820

KOLOM_NUMERIK = ["suhu_c", "kelembaban_persen", "tekanan_hpa", "kecepatan_angin_kmh"]


def bagi_rentang(awal, akhir, n):
    """Membagi [awal, akhir) menjadi tepat n bagian (selisih ukuran maks 1)."""
    total = akhir - awal
    dasar, sisa = divmod(total, n)
    hasil, s = [], awal
    for i in range(n):
        e = s + dasar + (1 if i < sisa else 0)
        if e > s:
            hasil.append((s, e))
        s = e
    return hasil


def hitung_baris_file(path):
    with open(path, "r", newline="") as f:
        return sum(1 for _ in f) - 1  # tanpa header


def baca_baris_csv(path, start, end):
    """Level 1 (I/O-bound): baca baris [start, end) dari CSV."""
    rows = []
    with open(path, "r", newline="") as f:
        header = f.readline().strip().split(",")
        for idx, line in enumerate(f):
            if start <= idx < end:
                rows.append(line.strip().split(","))
            elif idx >= end:
                break
    return header, rows


def baca_paralel(path, awal, akhir, n_thread):
    """Membaca [awal, akhir) dengan n_thread thread (tiap thread 1 sub-rentang)."""
    rentang = bagi_rentang(awal, akhir, n_thread)
    header, semua = None, []
    with ThreadPoolExecutor(max_workers=n_thread) as ex:
        futs = [ex.submit(baca_baris_csv, path, a, b) for a, b in rentang]
        for fut in futs:  # urutan submit dipertahankan
            h, rows = fut.result()
            header = h
            semua.extend(rows)
    return header, semua


def jalankan_serial(path, n_data, kolom):
    """Referensi serial: 1 proses, 1 thread, tanpa MPI."""
    header, rows = baca_baris_csv(path, 0, n_data)
    jumlah, banyak = hitung_parsial((header, rows, kolom))
    rata2 = hitung_rata2(jumlah, banyak, kolom)
    bersih, terisi = isi_kosong((header, rows, kolom, rata2))
    return header, bersih, terisi, rata2


def main():
    # pyrefly: ignore [missing-import]
    from mpi4py import MPI  # ditunda agar worker "spawn" tidak memicu MPI_Init()

    ap = argparse.ArgumentParser()
    ap.add_argument("--data-path", default="dataset.csv")
    ap.add_argument("--n-thread", type=int, default=N_THREAD_DEFAULT)
    ap.add_argument("--n-proses", type=int, default=N_PROSES_DEFAULT)
    ap.add_argument("--n-data", type=int, default=N_DATA_DEFAULT)
    ap.add_argument("--hasil", default="hasil.csv",
                    help="file tempat baris hasil ditambahkan (untuk grafik)")
    ap.add_argument("--no-simpan", action="store_true",
                    help="jangan tambahkan baris hasil ke file --hasil")
    args = ap.parse_args()

    random.seed(NIM)

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # jumlah baris sumber & jumlah baris yang diproses
    total_file = comm.bcast(hitung_baris_file(args.data_path) if rank == 0 else None, root=0)
    n_data = min(args.n_data, total_file)
    if rank == 0 and args.n_data > total_file:
        print(f"PERINGATAN: --n-data {args.n_data} > baris dataset ({total_file}); dipakai {n_data}.")

    # ---- referensi serial (rank 0 saja; rank lain menunggu) ----
    serial = None
    if rank == 0:
        t = time.perf_counter()
        serial = jalankan_serial(args.data_path, n_data, KOLOM_NUMERIK)
        T_serial = time.perf_counter() - t
    comm.Barrier()

    # ---- bagian paralel hybrid (diukur antar dua Barrier) ----
    rentang_rank = bagi_rentang(0, n_data, size)
    awal, akhir = rentang_rank[rank] if rank < len(rentang_rank) else (0, 0)

    t_mulai = time.perf_counter()

    # Level 1: baca dengan thread
    t = time.perf_counter()
    header, rows = baca_paralel(args.data_path, awal, akhir, args.n_thread) if akhir > awal else (None, [])
    t_baca = time.perf_counter() - t

    # Level 2 fase 1 + Level 3 (gabung statistik antar rank) + Level 2 fase 2
    t = time.perf_counter()
    sub = [rows[a - awal:b - awal] for a, b in bagi_rentang(awal, akhir, args.n_proses)]
    with ProcessPoolExecutor(max_workers=args.n_proses) as ex:
        parsial = list(ex.map(hitung_parsial, [(header, s, KOLOM_NUMERIK) for s in sub]))
        stat_rank = gabung_statistik(parsial, KOLOM_NUMERIK)
        t_pool1 = time.perf_counter() - t

        t = time.perf_counter()
        stat_semua = comm.allgather(stat_rank)           # Level 3: semua rank tahu statistik semua rank
        jumlah_g, banyak_g = gabung_statistik(stat_semua, KOLOM_NUMERIK)
        rata2_g = hitung_rata2(jumlah_g, banyak_g, KOLOM_NUMERIK)   # rata-rata GLOBAL
        t_komunikasi1 = time.perf_counter() - t

        t = time.perf_counter()
        hasil_isi = list(ex.map(isi_kosong, [(header, s, KOLOM_NUMERIK, rata2_g) for s in sub]))
        t_pool2 = time.perf_counter() - t

    bersih_lokal = [r for baris, _ in hasil_isi for r in baris]
    terisi_lokal = sum(n for _, n in hasil_isi)

    # Level 3: kumpulkan hasil ke root
    t = time.perf_counter()
    kumpul = comm.gather((bersih_lokal, terisi_lokal), root=0)
    t_komunikasi2 = time.perf_counter() - t

    comm.Barrier()
    T_paralel = time.perf_counter() - t_mulai

    # rincian waktu terlama antar rank (untuk analisis bottleneck)
    rincian = {
        "baca (thread)": t_baca,
        "ProcessPool fase 1": t_pool1,
        "ProcessPool fase 2": t_pool2,
        "komunikasi MPI": t_komunikasi1 + t_komunikasi2,
    }
    rincian_max = {k: comm.reduce(v, op=MPI.MAX, root=0) for k, v in rincian.items()}

    if rank == 0:
        bersih_paralel = [r for bl, _ in kumpul for r in bl]
        terisi_total = sum(n for _, n in kumpul)

        header_s, bersih_serial, terisi_serial, rata2_serial = serial
        valid = (bersih_paralel == bersih_serial and terisi_total == terisi_serial)

        speedup = T_serial / T_paralel if T_paralel > 0 else 0.0
        unit = size * args.n_proses
        efisiensi = speedup / unit * 100

        print("=" * 60)
        print(f"Hybrid Project by: {NAMA} ({NIM})")
        print(f"Threads: {args.n_thread} | Processes: {args.n_proses} | Data: {len(bersih_paralel)}")
        print(f"Total Time: {T_paralel:.3f} s | Speedup: {speedup:.2f} | Efficiency: {efisiensi:.1f}%")
        print("=" * 60)
        print(f"MPI ranks digunakan           : {size}")
        print(f"Data sumber / diproses        : {total_file} / {len(bersih_paralel)} baris")
        print(f"Total nilai kosong yang diisi : {terisi_total}")
        print(f"Validasi paralel vs serial    : {'OK (identik)' if valid else 'GAGAL (berbeda!)'}")
        print(f"T_serial = {T_serial:.3f} s | T_paralel = {T_paralel:.3f} s | "
              f"Efisiensi = Speedup / ({size} rank x {args.n_proses} proses)")
        print("Rincian waktu (terlama antar rank):")
        for k, v in rincian_max.items():
            print(f"  - {k:20s}: {v:.3f} s")
        print("Rata-rata tiap kolom (dari nilai valid, dipakai sebagai pengisi):")
        for k, v in rata2_g.items():
            print(f"  - {k:22s}: {v:.2f}")

        with open("dataset_cleaned.csv", "w", newline="") as f:
            f.write(",".join(header_s) + "\n")
            for row in bersih_paralel:
                f.write(",".join(row) + "\n")
        print(f"\ndataset_cleaned.csv tersimpan ({len(bersih_paralel)} baris).")

        baris = f"{size},{args.n_thread},{args.n_proses},{len(bersih_paralel)},{T_paralel:.3f},{speedup:.2f},{efisiensi:.1f}"
        print(f"CSV,{baris}")
        if not args.no_simpan:
            with open(args.hasil, "a") as f:
                f.write(baris + "\n")
            print(f"(baris ditambahkan ke {args.hasil})")


if __name__ == "__main__":
    main()
