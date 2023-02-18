Project ini dijalankan dalam environment docker, pastikan docker desktop sudah terinstall (jika belum terinstall, silahkan lakukan instalasi terlebih dahulu)

1. Extract raw_data.zip pada folder JALA_Tech/assessment/data

2. Buka path menggunakan terminal, dan execute command: docker-compose -f docker-compose.yml up -d

3. Setelah proses selesai, cek docker image yang sedang running (bisa menggunakan command: docker compose -ps, atau buka aplikasi docker desktop, dan pastikan container sudah running:
	- mysqldb
	- postgres
	- scheduler
	- webserver

4. Execute command (pada terminal host machine): docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mysqldb  untuk melihat ip host machine untuk SQL Server.
   Simpan IP yang muncul

5. Execute (pada terminal host machine): docker exec -it mysqldb /bin/bash, maka akan masuk pada terminal mysql.

6. Execute (pada terminal mysql): mysql -h <IP> -P 3306 --protocol=TCP -u root -p   (Ganti <IP> dengan IP yang diperoleh pada langkah 3)
   Masukkan password = example

7. Execute (pada terminal mysql): SET GLOBAL local_infile = true;

8. Buka browser, dan ketikkan: localhost:8080. Akan muncul web GUI dari airflow.
   Login menggunakan username = admin, password = admin

9. Buat koneksi mysql di web GUI airflow pada menu admin -> connection
   conn_id = mysql_default
   conn_type = mysql
   host = IP dari step 3
   login = root
   password = example
   port = 3306
   extra = {"local_infile": true}

10. Masuk ke menu DAGs, dan unpause untuk DAG get_event_data



NOTE:
--------------------------------------------------------------------------------------------------------
credential mysql: username = root, password = example
credential airflow: username = admin, password = admin
script untuk generate MAU, DAU dan user stickiness ada pada folder data/mau_dau.sql

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
Program (airflow) ini akan running setiap 2 jam sekali, membaca file json berdasarkan nama filenya. 
Misalkan waktu saat ini adalah tanggal 7 Februari 2023 pukul 08.00 WIB.
Program disetting untuk mundur 4 bulan sehingga menjadi tanggal 7 November 2022 pukul 08.00 WIB (untuk menyesuaikan dengan folder dari raw data).
Ketika DAG running pada 7 Februari 2023 pukul 08.00 WIB, maka akan membaca file pada folder 2022-10-7 untuk json file dengan suffix 060000  sampai 075959
Ketika DAG running pada 7 Februari 2023 pukul 10.00 WIB, maka akan membaca file pada folder 2022-10-7 untuk json file dengan suffix 080000  sampai 095959