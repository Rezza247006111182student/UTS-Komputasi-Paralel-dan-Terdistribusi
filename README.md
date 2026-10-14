# Parallel Dataset Cleaner — UTS Komputasi Paralel dan Terdistribusi

**Nama:** Muhammad Rezza  
**NIM:** 247006111182  
**Kelas:** G  

---

## Daftar Isi
1. [Deskripsi Proyek](#deskripsi-proyek)
2. [Arsitektur Program](#arsitektur-program)
3. [Parameter Pribadi (NIM)](#parameter-pribadi-nim)
4. [Instalasi Dependensi](#instalasi-dependensi)
5. [Struktur File](#struktur-file)
6. [Cara Menjalankan](#cara-menjalankan)
7. [Cara Menjalankan Benchmark](#cara-menjalankan-benchmark)
8. [Cara Membuat Grafik](#cara-membuat-grafik)
9. [Definisi Metrik](#definisi-metrik)
10. [Format CSV Output](#format-csv-output)
11. [Batasan dan Catatan Penting](#batasan-dan-catatan-penting)

---

## Deskripsi Proyek

Program **Parallel Dataset Cleaner** membersihkan dataset sensor (CSV) yang berisi nilai kosong menggunakan pendekatan **hybrid parallel computing** dengan tiga level paralelisme:

| Level | Teknologi | Fungsi |
|---|---|---|
| Level 1 (I/O-bound) | `ThreadPoolExecutor` | Membaca sub-bagian CSV secara bersamaan per rank |
| Level 2 (CPU-bound) | `ProcessPoolExecutor` | Membersihkan nilai kosong + menghitung statistik |
| Level 3 (Distributed) | `mpi4py` (MPI) | Mendistribusikan data antar rank, menggabungkan hasil |

---

## Arsitektur Program

```
┌─────────────────────────────────────────────────────────┐
│                    mpiexec -n <N>                        │
│   ┌──────────────┐   ┌──────────────┐   ┌────────────┐ │
│   │   Rank 0     │   │   Rank 1     │   │  Rank N-1  │ │
│   │              │   │              │   │            │ │
│   │ ┌──────────┐ │   │ ┌──────────┐ │   │ ┌────────┐ │ │
│   │ │ Thread 1 │ │   │ │ Thread 1 │ │   │ │Thread 1│ │ │
│   │ │ Thread 2 │ │   │ │ Thread 2 │ │   │ │Thread 2│ │ │
│   │ │ Thread T │ │   │ │ Thread T │ │   │ │Thread T│ │ │
│   │ └──────────┘ │   │ └──────────┘ │   │ └────────┘ │ │
│   │ (baca CSV)   │   │ (baca CSV)   │   │ (baca CSV) │ │
│   │              │   │              │   │            │ │
│   │ ┌──────────┐ │   │ ┌──────────┐ │   │ ┌────────┐ │ │
│   │ │ Proses 1 │ │   │ │ Proses 1 │ │   │ │Proses 1│ │ │
│   │ │ Proses 2 │ │   │ │ Proses 2 │ │   │ │Proses 2│ │ │
│   │ │ Proses P │ │   │ │ Proses P │ │   │ │Proses P│ │ │
│   │ └──────────┘ │   │ └──────────┘ │   │ └────────┘ │ │
│   │ (bersihkan)  │   │ (bersihkan)  │   │(bersihkan) │ │
│   └──────┬───────┘   └──────┬───────┘   └─────┬──────┘ │
│          └─────────── comm.gather() ───────────┘        │
│                        Rank 0                           │
│                   (gabung + simpan CSV)                 │
└─────────────────────────────────────────────────────────┘
```

### Penjelasan Level Paralelisme

**Level 3 — MPI (Distribusi antar Node/Proses MPI):**
- Dataset dibagi secara merata ke setiap MPI rank menggunakan distribusi block
- Rank ke-`i` memproses baris `[row_start_i, row_end_i)`
- Setelah selesai, semua hasil di-`gather` ke rank 0
- Rank 0 menggabungkan data dan menyimpan ke `dataset_cleaned.csv`

**Level 2 — ProcessPoolExecutor (Komputasi Paralel):**
- Chunk baris milik tiap rank dibagi lagi ke `n_proses` proses
- Setiap proses membersihkan sub-chunk-nya (menghitung rata-rata dan mengisi NaN)
- **Catatan rata-rata:** rata-rata dihitung per sub-chunk, bukan global dataset. Ini menyebabkan nilai imputasi sedikit berbeda antar sub-chunk. Pendekatan ini memadai untuk eksperimen benchmark.

**Level 1 — ThreadPoolExecutor (I/O Paralel):**
- Pembacaan CSV dibagi ke `n_thread` thread
- Setiap thread membaca rentang baris berbeda dari file yang sama
- Efektif ketika I/O disk menjadi bottleneck

---

## Parameter Pribadi (NIM)

Berdasarkan NIM **247006111182**:

| Parameter | Rumus | Nilai |
|---|---|---|
| Jumlah thread | (82 mod 4) + 2 = 2 + 2 | **4** |
| Jumlah proses | (61 mod 3) + 2 = 1 + 2 | **3** |
| Jumlah data | 182 × 10 | **1820 baris** |
| Random seed | NIM | **247006111182** |
| Proporsi NaN | ~15% per kolom | **~15%** |

---

## Instalasi Dependensi

### 1. Microsoft MPI (Windows)
Download dan instal dari:  
https://www.microsoft.com/en-us/download/details.aspx?id=57467

Verifikasi:
```powershell
mpiexec --help
```

### 2. Library Python
```powershell
pip install mpi4py matplotlib
```

> **Catatan:** Program inti hanya membutuhkan `mpi4py`. `matplotlib` hanya diperlukan untuk `plot_benchmarks.py`.

---

## Struktur File

```
uts/
├── generate_dataset.py         # Generator dataset sensor sintetis
├── parallel_dataset_cleaner.py # Program utama (MPI + Thread + Process)
├── cleaner_worker.py           # Fungsi pembersih (dijalankan ProcessPoolExecutor)
├── run_benchmarks.py           # Benchmark otomatis berbagai konfigurasi
├── plot_benchmarks.py          # Visualisasi hasil benchmark (6 grafik)
├── README.md                   # Dokumentasi ini
├── dataset.csv                 # Dataset mentah (di-generate)
├── dataset_cleaned.csv         # Hasil pembersihan (di-generate)
└── results/
    ├── benchmark_results.csv   # Hasil semua pengulangan benchmark
    ├── benchmark_summary.csv   # Ringkasan (rata-rata + std dev) per konfigurasi
    ├── cleaned_*.csv           # CSV bersih per eksperimen benchmark
    └── plots/
        ├── grafik1_waktu_eksekusi.png
        ├── grafik2_speedup.png
        ├── grafik3_efisiensi.png
        ├── grafik4_pengaruh_data.png
        ├── grafik5_konfigurasi_paralel.png
        └── grafik6_kualitas_data.png
```

---

## Cara Menjalankan

### 1. Buat Dataset
```powershell
# Dataset default (1820 baris sesuai NIM)
python generate_dataset.py

# Dataset lebih besar untuk eksperimen
python generate_dataset.py --n-data 10000
```

### 2. Jalankan Pembersihan Data
```powershell
# Parameter default NIM (1 rank, 4 thread, 3 proses, 1820 baris)
mpiexec -n 1 python parallel_dataset_cleaner.py

# 2 MPI rank
mpiexec -n 2 python parallel_dataset_cleaner.py

# Parameter kustom
mpiexec -n 2 python parallel_dataset_cleaner.py --n-thread 4 --n-proses 3 --n-data 5000
```

### 3. Argumen yang Tersedia
| Argumen | Default | Keterangan |
|---|---|---|
| `--data-path` | `dataset.csv` | Path file input |
| `--n-thread` | `4` | Jumlah thread I/O |
| `--n-proses` | `3` | Jumlah proses CPU |
| `--n-data` | `1820` | Jumlah baris yang diproses |
| `--output-csv` | `dataset_cleaned.csv` | Path file output |

---

## Cara Menjalankan Benchmark

```powershell
# Benchmark dengan 3 pengulangan per konfigurasi (default)
python run_benchmarks.py

# Benchmark dengan 5 pengulangan
python run_benchmarks.py --repeats 5

# Folder output berbeda
python run_benchmarks.py --results-dir my_results
```

Script ini secara otomatis:
1. Mengecek apakah `mpiexec` tersedia
2. Membuat dataset jika belum ada atau kurang besar
3. Menjalankan 10 konfigurasi eksperimen berbeda
4. Menyimpan semua hasil ke `results/benchmark_results.csv`
5. Membuat ringkasan ke `results/benchmark_summary.csv`

**Konfigurasi Eksperimen:**
| ID | MPI Rank | Thread | Proses | Data | Tujuan |
|---|---|---|---|---|---|
| E1 | 1 | 1 | 1 | 1820 | Mendekati serial (baseline) |
| E2 | 1 | 2 | 2 | 1820 | Paralel ringan |
| E3 | 1 | 4 | 3 | 1820 | Default NIM |
| E4 | 2 | 4 | 3 | 1820 | +1 MPI rank |
| E5 | 1 | 4 | 3 | 5000 | Data lebih besar |
| E6 | 1 | 4 | 3 | 10000 | Data jauh lebih besar |
| E7 | 1 | 4 | 1 | 1820 | Isolasi: proses=1 |
| E8 | 1 | 4 | 2 | 1820 | Isolasi: proses=2 |
| E9 | 1 | 1 | 3 | 1820 | Isolasi: thread=1 |
| E10 | 1 | 2 | 3 | 1820 | Isolasi: thread=2 |

---

## Cara Membuat Grafik

```powershell
# Setelah run_benchmarks.py selesai
python plot_benchmarks.py

# Dengan folder hasil kustom
python plot_benchmarks.py --results-dir my_results
```

Grafik yang dihasilkan:

| File | Isi |
|---|---|
| `grafik1_waktu_eksekusi.png` | Perbandingan waktu serial vs paralel per konfigurasi |
| `grafik2_speedup.png` | Speedup setiap konfigurasi + garis referensi |
| `grafik3_efisiensi.png` | Efisiensi paralel (%) per konfigurasi |
| `grafik4_pengaruh_data.png` | Waktu eksekusi vs jumlah baris dataset |
| `grafik5_konfigurasi_paralel.png` | Isolasi pengaruh jumlah proses dan MPI rank |
| `grafik6_kualitas_data.png` | Nilai kosong sebelum dan sesudah pembersihan |

---

## Definisi Metrik

### Waktu Serial
> Total waktu yang dibutuhkan jika seluruh pekerjaan dikerjakan **secara serial, satu per satu**.

Dihitung sebagai penjumlahan (`SUM`) waktu serial dari **semua rank MPI**:
```
total_serial = Σ(t_serial_i + t_baca_i)  untuk semua rank i
```

### Waktu Paralel (Makespan)
> Waktu dinding (wall-clock time) dari awal hingga selesai, diukur dari **rank yang paling lambat**.

```
makespan = MAX(t_baca_i + t_paralel_i)  untuk semua rank i
```

### Speedup
> Seberapa banyak percepatan yang dicapai dibandingkan eksekusi serial.

```
Speedup = total_serial / makespan
```

- `Speedup > 1` → pendekatan paralel lebih cepat
- `Speedup = 1` → sama dengan serial
- `Speedup < 1` → paralel **lebih lambat** (overhead melebihi manfaat)

### Efisiensi (relatif n_proses)
> Berapa persen dari kapasitas ideal proses yang dimanfaatkan.

```
Efisiensi = (Speedup / n_proses) × 100%
```

Pembagi `n_proses` dipakai karena **ProcessPoolExecutor adalah bottleneck CPU-bound utama**.

### Efisiensi Total (relatif semua worker)
> Efisiensi yang memperhitungkan seluruh worker paralel (MPI rank × proses).

```
Efisiensi_total = (Speedup / (n_rank × n_proses)) × 100%
```

---

## Format CSV Output

### `benchmark_results.csv` (semua pengulangan)
```csv
experiment_id,mpi_ranks,threads,processes,data_rows,repeat,serial_time_seconds,parallel_time_seconds,speedup,efficiency_percent,efficiency_total_percent,rows_processed,missing_values_before,missing_values_filled,status
E3_rank1_t4_p3,1,4,3,1820,1,0.123456,0.234567,0.5264,17.55,17.55,1820,412,412,success
E3_rank1_t4_p3,1,4,3,1820,2,0.119234,0.228901,0.5211,17.37,17.37,1820,412,412,success
```

### `benchmark_summary.csv` (rata-rata per konfigurasi)
```csv
experiment_id,mpi_ranks,threads,processes,data_rows,n_repeats,mean_parallel_time_s,stdev_parallel_time_s,mean_speedup,...
E3_rank1_t4_p3,1,4,3,1820,3,0.2315,0.0032,0.521,...
```

**Catatan format:**
- Semua nilai numerik disimpan sebagai angka (bukan teks berformat)
- Waktu selalu dalam satuan **detik**
- Kolom `status`: `success` atau `failed:<pesan_error>`

---

## Batasan dan Catatan Penting

### Overhead Paralelisme
Untuk dataset kecil (≤ 1820 baris), **speedup sering di bawah 1** karena:
- Overhead spawn proses baru (ProcessPoolExecutor) di Windows lebih besar dari manfaatnya
- Komunikasi MPI menambah latensi
- Pembacaan file yang sudah di-cache OS membuat Thread kurang terasa manfaatnya

Ini adalah **fenomena nyata**, bukan bug. Untuk melihat speedup > 1, gunakan dataset lebih besar (`--n-data 10000`).

### Rata-rata Imputasi per Sub-chunk
Nilai kosong diisi dengan rata-rata dari **sub-chunk masing-masing** (bukan rata-rata global dataset). Ini menyebabkan nilai imputasi bisa sedikit berbeda antar sub-chunk. Untuk keperluan eksperimen benchmark, perbedaan ini dapat diabaikan.

### Kompatibilitas Windows
- `import mpi4py.MPI` **harus** berada di dalam `main()`, bukan di level modul
- Fungsi untuk ProcessPoolExecutor **harus** berada di `cleaner_worker.py` yang terpisah
- Kedua hal ini mencegah error `failed to attach to a bootstrap queue` yang disebabkan `MPI_Init()` dipanggil berulang oleh proses worker

### Reproduktibilitas
Semua angka acak menggunakan `random.seed(247006111182)` sehingga dataset yang sama selalu dihasilkan pada setiap run `generate_dataset.py`.

### Waktu Benchmark vs Waktu Nyata
Waktu eksekusi sangat dipengaruhi oleh:
- Beban CPU saat eksperimen (tutup aplikasi lain)
- Status cache disk OS
- Kondisi network (untuk MPI multi-node)
- Versi Python dan library

Gunakan **rata-rata dari beberapa pengulangan** (`--repeats 3` atau lebih) untuk hasil yang lebih stabil.
