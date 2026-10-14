# Nama Lengkap : Muhammad Rezza
# NPM/NIM      : 247006111182
# Kelas        : G
#
# Generate dataset "log sensor" (1820 baris, sesuai parameter pribadi:
# jumlah data = 3 digit terakhir NIM x 10 = 182 x 10 = 1820).
# Sebagian nilai pada kolom numerik sengaja dikosongkan (NaN) secara acak
# menggunakan random.seed(NIM) supaya hasilnya reproducible dan unik per NIM
# (sesuai ketentuan soal: "Nomor acak pribadi: gunakan random.seed(NIM)").

import csv
import random
import argparse

NIM = 247006111182
JUMLAH_DATA_DEFAULT = 1820  # 182 (3 digit terakhir NIM) x 10
PROPORSI_KOSONG = 0.15      # ~15% nilai dikosongkan untuk disimulasikan sbg data kotor

random.seed(NIM)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-data", type=int, default=JUMLAH_DATA_DEFAULT,
                         help="Jumlah baris dataset yang dibuat (default: 1820 sesuai parameter NIM)")
    args = parser.parse_args()
    jumlah_data = args.n_data

    with open("dataset.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "suhu_c", "kelembaban_persen", "tekanan_hpa", "kecepatan_angin_kmh"])
        for i in range(1, jumlah_data + 1):
            suhu = round(random.uniform(20.0, 35.0), 2)
            kelembaban = round(random.uniform(40.0, 95.0), 2)
            tekanan = round(random.uniform(995.0, 1025.0), 2)
            angin = round(random.uniform(0.0, 40.0), 2)

            row = [i, suhu, kelembaban, tekanan, angin]
            # kosongkan acak salah satu dari 4 kolom numerik dengan peluang PROPORSI_KOSONG
            for col_idx in range(1, 5):
                if random.random() < PROPORSI_KOSONG:
                    row[col_idx] = ""
            writer.writerow(row)

    print(f"dataset.csv dibuat: {jumlah_data} baris, ~{int(PROPORSI_KOSONG*100)}% nilai kosong per kolom")


if __name__ == "__main__":
    main()
