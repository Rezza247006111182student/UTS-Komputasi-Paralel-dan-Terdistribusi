# UTS Komputasi Paralel dan Terdistribusi — Parallel Dataset Cleaner

**Nama:** Muhammad Rezza
**NIM:** 247006111182
**Kelas:** G

## Requirement

Sama seperti setup MPI yang sudah kamu pasang sebelumnya (MS-MPI + mpi4py):
- Python 3.x
- Microsoft MPI (MS-MPI) sudah terpasang (`mpiexec -help` harus jalan)
- Library tambahan:
```powershell
pip install mpi4py
```
(Script ini hanya memakai modul standar Python selain `mpi4py` — tidak perlu
`pandas`/`numpy`, supaya ringan dan mudah dijalankan di laptop mana pun.)

## File

| File | Fungsi |
|---|---|
| `generate_dataset.py` | Membuat `dataset.csv` (1820 baris, ada nilai kosong acak) |
| `parallel_dataset_cleaner.py` | Program utama hybrid: Thread (baca) + ProcessPool (bersihkan) + MPI (distribusi & gabung) |
| `cleaner_worker.py` | Fungsi pembersih data yang dijalankan ProcessPoolExecutor — **wajib ada di folder yang sama**, lihat catatan penting di bawah |

**PENTING:** `cleaner_worker.py` harus berada di folder yang sama dengan
`parallel_dataset_cleaner.py`. File ini sengaja dipisah dan tidak
meng-import `mpi4py` sama sekali — kalau digabung lagi ke file utama,
`mpiexec` akan gagal dengan error `failed to attach to a bootstrap queue`
di Windows (lihat bagian Troubleshooting).

## Cara Menjalankan

### 1. Generate dataset (sekali saja)
```powershell
python generate_dataset.py
```
Menghasilkan `dataset.csv` berisi 1820 baris data sensor (suhu, kelembaban,
tekanan, kecepatan angin) dengan sekitar 15% nilai kosong per kolom.

### 2. Jalankan program utama (parameter default sesuai NIM)
```powershell
mpiexec -n 2 python parallel_dataset_cleaner.py
```
`-n 2` berarti 2 rank MPI (bisa diganti, misal `-n 1` atau `-n 4` — lihat
Bagian C untuk variasi yang wajib diuji). Program otomatis memakai parameter
pribadi (4 thread, 3 proses, 1820 data) jika tidak diberi argumen tambahan.

Output yang akan muncul (dari rank 0):
```
Hybrid Project by: Muhammad Rezza (247006111182)
Threads: 4 | Processes: 3 | Data: 1820
Total Time: ... s | Speedup: ... | Efficiency: ...%
```
Serta rincian: jumlah rank MPI, total baris dibersihkan, total nilai kosong
yang diisi, dan rata-rata tiap kolom setelah dibersihkan. File
`dataset_cleaned.csv` otomatis tersimpan berisi data yang sudah bersih.

### 3. Menjalankan dengan parameter berbeda (untuk Bagian C)
Tiga argumen bisa diubah lewat command line tanpa mengedit kode:
```powershell
mpiexec -n <jumlah_rank> python parallel_dataset_cleaner.py --n-thread <T> --n-proses <P> --n-data <N>
```
Contoh:
```powershell
mpiexec -n 1 python parallel_dataset_cleaner.py --n-thread 2 --n-proses 2
mpiexec -n 1 python parallel_dataset_cleaner.py --n-thread 4 --n-proses 3
mpiexec -n 2 python parallel_dataset_cleaner.py --n-thread 4 --n-proses 3
mpiexec -n 1 python parallel_dataset_cleaner.py --n-thread 4 --n-proses 3 --n-data 5000
```
Catatan: `--n-data` mengubah jumlah baris yang DIPROSES dari `dataset.csv`,
jadi kalau mau menguji data lebih besar dari 1820, generate dataset baru
dulu dengan mengganti `JUMLAH_DATA` di `generate_dataset.py`.

Setiap run mencetak baris ringkas di akhir:
```
CSV,<rank>,<thread>,<proses>,<data>,<waktu>,<speedup>,<efisiensi>
```
Salin baris ini ke spreadsheet untuk membuat tabel hasil Bagian C.

## Catatan Penting Soal Hasil Speedup

Karena pembersihan 1820 baris itu sendiri sangat ringan (hitungan
milidetik), overhead membuat thread/proses baru bisa lebih besar daripada
manfaat paralelismenya, sehingga speedup bisa muncul **di bawah 1** (lebih
lambat dari serial) pada beberapa konfigurasi kecil. Ini bukan bug — ini
fenomena nyata yang sudah berulang kali terbukti di tugas-tugas
sebelumnya (overhead pembuatan proses/thread melebihi manfaatnya untuk
beban kerja kecil), dan justru jadi bahan analisis bagus untuk Bagian D
("faktor apa yang paling memengaruhi kecepatan program"). Kalau ingin
melihat speedup yang lebih jelas di atas 1, perbesar jumlah data (misalnya
`--n-data` dengan dataset yang lebih besar) supaya beban kerja CPU-nya
cukup berat untuk menutupi overhead paralelisme.

