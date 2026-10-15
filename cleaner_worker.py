# Nama Lengkap : Muhammad Rezza
# NPM/NIM      : 247006111182
# Kelas        : G
#
# Fungsi-fungsi yang dijalankan ProcessPoolExecutor (Level 2, CPU-bound).
#
# PENTING (Windows): file ini SENGAJA tidak meng-import mpi4py. Pada Windows,
# ProcessPoolExecutor memakai "spawn" sehingga worker mengimpor ulang modul;
# kalau mpi4py ikut ter-import, tiap worker memicu MPI_Init() dan mpiexec gagal.
#
# DESAIN BARU (perbaikan akurasi): pembersihan dibuat DUA FASE supaya hasilnya
# tidak bergantung pada jumlah proses/rank:
#   Fase 1 -> hitung_parsial : jumlah & banyak nilai valid per kolom (per sub-chunk)
#   (gabung seluruh sub-chunk & seluruh rank -> satu rata-rata GLOBAL per kolom)
#   Fase 2 -> isi_kosong     : isi nilai kosong dengan rata-rata GLOBAL itu
# Penjumlahan dilakukan dalam satuan "sen" (bilangan bulat, nilai x 100) agar
# hasilnya persis sama berapa pun urutan/jumlah pembagian data (tidak ada
# selisih pembulatan floating point antar konfigurasi).


def _indeks(header, kolom):
    return {nama: header.index(nama) for nama in kolom}


def hitung_parsial(args):
    """Fase 1: jumlah (dalam sen) dan banyaknya nilai valid tiap kolom."""
    header, rows, kolom = args
    idx = _indeks(header, kolom)
    jumlah = {k: 0 for k in kolom}
    banyak = {k: 0 for k in kolom}
    for row in rows:
        for k, i in idx.items():
            v = row[i]
            if v != "":
                jumlah[k] += round(float(v) * 100)
                banyak[k] += 1
    return jumlah, banyak


def gabung_statistik(daftar_stat, kolom):
    """Menjumlahkan banyak pasangan (jumlah, banyak) menjadi satu pasangan."""
    jumlah = {k: 0 for k in kolom}
    banyak = {k: 0 for k in kolom}
    for j, b in daftar_stat:
        for k in kolom:
            jumlah[k] += j[k]
            banyak[k] += b[k]
    return jumlah, banyak


def hitung_rata2(jumlah, banyak, kolom):
    """Rata-rata tiap kolom dari seluruh nilai valid (bobot = jumlah nilai valid)."""
    return {k: (jumlah[k] / banyak[k] / 100 if banyak[k] > 0 else 0.0) for k in kolom}


def isi_kosong(args):
    """Fase 2: isi nilai kosong dengan rata-rata global. Mengembalikan
    (baris_bersih, jumlah_sel_yang_diisi)."""
    header, rows, kolom, rata2 = args
    idx = _indeks(header, kolom)
    pengisi = {k: f"{rata2[k]:.2f}" for k in kolom}
    terisi = 0
    hasil = []
    for row in rows:
        baru = list(row)
        for k, i in idx.items():
            if baru[i] == "":
                baru[i] = pengisi[k]
                terisi += 1
        hasil.append(baru)
    return hasil, terisi
