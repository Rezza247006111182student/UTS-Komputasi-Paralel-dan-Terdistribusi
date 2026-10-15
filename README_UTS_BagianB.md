# UTS Komputasi Paralel dan Terdistribusi — Parallel Dataset Cleaner

**Nama:** Muhammad Rezza | **NIM:** 247006111182 | **Kelas:** G

## Arsitektur
| Level | Teknologi | Tugas |
|---|---|---|
| 1 (I/O-bound) | `ThreadPoolExecutor` | membaca sub-chunk CSV |
| 2 (CPU-bound) | `ProcessPoolExecutor` | fase 1: hitung jumlah & banyak nilai valid per kolom; fase 2: isi nilai kosong |
| 3 (distributed) | `mpi4py` | membagi baris antar rank, `allgather` statistik, `gather` hasil |

Nilai kosong diisi dengan **rata-rata global** seluruh nilai valid (bobot = jumlah
nilai valid per kolom), sehingga hasil pembersihan **sama persis** berapa pun
jumlah rank/proses/thread. Program memvalidasi ini dengan membandingkannya
sel demi sel dengan hasil serial (baris `Validasi paralel vs serial`).

## Parameter pribadi (NIM 247006111182)
Thread = 82 mod 4 + 2 = **4**, Proses = 61 mod 3 + 2 = **3**, Data = 182 x 10 = **1820**,
`random.seed(247006111182)`. ("Dua digit tengah" = posisi ke-6 & 7 dari 12 digit;
contoh hitungan pada soal `10 mod 3 + 2 = 4` keliru, seharusnya 3.)

## Definisi metrik
- **T_serial** — seluruh pekerjaan (baca 1 thread + statistik + isi nilai kosong) pada 1 proses, tanpa MPI
- **T_paralel** — waktu dinding pipeline hybrid (baca + ProcessPool + komunikasi MPI) antara dua `Barrier`
- **Speedup** = T_serial / T_paralel
- **Efisiensi** = Speedup / (jumlah rank x jumlah proses) x 100%

## Requirement
Python 3.x, MS-MPI (Windows) / MPICH atau OpenMPI, lalu `pip install mpi4py matplotlib`.
`cleaner_worker.py` **harus satu folder** dengan `parallel_dataset_cleaner.py` (sengaja
tanpa `mpi4py` agar worker `ProcessPoolExecutor` di Windows tidak memicu `MPI_Init()` ulang).

## Cara menjalankan
```powershell
python generate_dataset.py --n-data 5000        # dataset sumber 5000 baris (superset)
mpiexec -n 2 python parallel_dataset_cleaner.py # default NIM: 4 thread, 3 proses, 1820 baris pertama
mpiexec -n 2 python parallel_dataset_cleaner.py --n-thread 2 --n-proses 4 --n-data 1000
```
`--n-data` = jumlah baris dari awal `dataset.csv` yang diproses; `dataset_cleaned.csv`
selalu berisi baris yang diproses pada run terakhir. Setiap run menambahkan satu
baris ke `hasil.csv` (`rank,thread,proses,data,waktu,speedup,efisiensi`); gunakan
`--no-simpan` untuk mematikannya.

## Membuat grafik (Bagian C)
Hapus/ganti nama `hasil.csv` lama sebelum eksperimen baru, jalankan 13 konfigurasi, lalu:
```powershell
python generate_grafik.py --input hasil.csv --out grafik
```
Menghasilkan `grafik_waktu_vs_thread.png`, `grafik_waktu_vs_proses.png`,
`grafik_speedup_vs_konfigurasi.png`.

## Troubleshooting
- `No module named 'mpi4py'` → `pip install mpi4py`
- `cannot load MPI library` / `msmpi.dll` → pasang MS-MPI lalu restart terminal
- `python3` tidak dikenali di Windows → pakai `python`
- `failed to attach to a bootstrap queue` → pastikan `mpi4py` hanya di-import di dalam `main()` dan fungsi worker ada di `cleaner_worker.py`
