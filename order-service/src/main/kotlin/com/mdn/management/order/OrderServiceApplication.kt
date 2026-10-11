package com.mdn.management.order

import com.mdn.management.grpc.inventory.InventoryServiceGrpc
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.grpc.client.ImportGrpcClients

/**
 * @ImportGrpcClients adalah cara bawaan Spring Boot 4.1 untuk membuat gRPC client. Kamu tidak
 * perlu menulis kode pembuat koneksi sendiri:
 * - target = "inventory" menyebut nama channel yang kita daftarkan di application.properties
 *   (bagian channel.inventory). Spring akan mencari alamat aslinya (localhost:9090) dari sana.
 *   Dokumentasi Spring Boot memang menyarankan memakai nama channel seperti ini, bukan
 *   menulis alamat server langsung di kode.
 * - types = [InventoryServiceGrpc.InventoryServiceBlockingStub::class] menentukan jenis stub
 *   yang dibuatkan menjadi bean. Blocking stub berarti pemanggilnya menunggu sampai jawaban
 *   datang, seperti memanggil fungsi biasa. Cara ini paling mudah dipahami untuk pemula.
 *
 * - Hasilnya, bean InventoryServiceBlockingStub tersedia dan bisa disuntikkan ke class mana pun
 *   lewat constructor.
 */
@SpringBootApplication
@ImportGrpcClients(
	target = "inventory",
	types = [InventoryServiceGrpc.InventoryServiceBlockingStub::class],
)
class OrderServiceApplication

fun main(args: Array<String>) {
	runApplication<OrderServiceApplication>(*args)
}
