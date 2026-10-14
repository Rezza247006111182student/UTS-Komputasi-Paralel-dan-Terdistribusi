# Nama Lengkap : Muhammad Rezza
# NPM/NIM      : 247006111182
# Kelas        : G
#
# File terpisah KHUSUS untuk fungsi yang dijalankan ProcessPoolExecutor.
#
# PENTING (Windows): file ini SENGAJA tidak meng-import mpi4py sama sekali.
# ProcessPoolExecutor di Windows membuat proses baru dengan cara "spawn",
# yang berarti proses worker akan mengimpor ulang modul tempat fungsi
# target-nya didefinisikan. Kalau fungsi ini didefinisikan di file utama
# yang meng-import mpi4py, setiap proses worker ProcessPoolExecutor ikut
# memicu MPI_Init() lagi dan menyebabkan error "failed to attach to a
# bootstrap queue" / "job aborted" pada mpiexec. Dengan memisahkan fungsi
# ini ke file sendiri yang bersih dari mpi4py, proses worker cukup
# mengimpor file kecil ini saja tanpa memicu MPI_Init() ulang.

def bersihkan_subchunk(args):
    """Level 2 (CPU-bound): bersihkan satu sub-chunk baris.
    Dijalankan di proses terpisah (ProcessPoolExecutor)."""
    header, rows, kolom_numerik = args

    idx_kolom = {nama: header.index(nama) for nama in kolom_numerik}

    jumlah = {k: 0.0 for k in kolom_numerik}
    count = {k: 0 for k in kolom_numerik}
    for row in rows:
        for k, i in idx_kolom.items():
            val = row[i]
            if val != "":
                jumlah[k] += float(val)
                count[k] += 1
    rata2 = {k: (jumlah[k] / count[k] if count[k] > 0 else 0.0) for k in kolom_numerik}

    missing_terisi = 0
    cleaned_rows = []
    for row in rows:
        new_row = list(row)
        for k, i in idx_kolom.items():
            if new_row[i] == "":
                new_row[i] = f"{rata2[k]:.2f}"
                missing_terisi += 1
        cleaned_rows.append(new_row)

    return cleaned_rows, rata2, count, missing_terisi
