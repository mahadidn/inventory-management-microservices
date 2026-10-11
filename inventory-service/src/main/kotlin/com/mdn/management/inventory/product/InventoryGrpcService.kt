package com.mdn.management.inventory.product

import com.mdn.management.grpc.inventory.CheckStockRequest
import com.mdn.management.grpc.inventory.CheckStockResponse
import com.mdn.management.grpc.inventory.InventoryServiceGrpc
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.springframework.grpc.server.service.GrpcService

/**
 * Bentuk class ini mengikuti pola yang dicontohkan di dokumentasi resmi Spring Boot untuk gRPC
 * server di Kotlin. Penjelasannya bagian per bagian:
 *
 * 1. Bagian import
 * - CheckStockRequest, CheckStockResponse, dan InventoryServiceGrpc adalah class hasil generate dari inventory.proto di 4A. Package-nya com.mdn.management.grpc.inventory, sesuai java_package yang kita tulis di file .proto.
 * - Status adalah kumpulan kode status resmi gRPC.
 * - StreamObserver adalah “saluran” untuk mengirim jawaban ke pemanggil.
 * - GrpcService adalah anotasi dari Spring gRPC.
 *
 * 2. @GrpcService
 * - Anotasi ini membuat class ini menjadi bean Spring sekaligus mendaftarkannya sebagai layanan gRPC. Begitu bean ini ada, gRPC server punya sesuatu untuk dilayani dan akan menyala di port 9090. Inilah yang menyelesaikan masalah aplikasi berhenti tadi.
 * - class InventoryGrpcService(private val productRepository: ProductRepository)
 *   ProductRepository disuntikkan lewat constructor (constructor injection), sama seperti di @Service biasa.
 *   : InventoryServiceGrpc.InventoryServiceImplBase()
 *   Kita mewarisi class induk hasil generate. Di dalam class induk itu, method checkStock sudah ada, tapi isinya hanya mengembalikan error UNIMPLEMENTED. Tugas kita adalah menimpanya (override) dengan logika sungguhan.
 * a. override fun checkStock(request, responseObserver)
 *    - request berisi data dari pemanggil. Field product_code di .proto otomatis bisa dibaca di Kotlin sebagai request.productCode, dan quantity sebagai request.quantity.
 *    - responseObserver adalah tempat kita mengirim jawaban. gRPC sengaja memakai pola ini (bukan sekadar return), karena saluran yang sama juga dipakai untuk streaming, yaitu mengirim banyak jawaban berturut-turut.
 * - Validasi quantity <= 0
 * - Memesan 0 atau minus barang tidak masuk akal, jadi kita tolak dengan Status.INVALID_ARGUMENT. Ini padanan HTTP 400 di dunia gRPC.
 * - productRepository.findByCode(...) lalu cek null
 * - Kalau kode produk tidak ada di database, kita balas dengan Status.NOT_FOUND, padanan HTTP 404.
 * - Perhatikan pola responseObserver.onError(...) lalu langsung return. Setelah onError dipanggil, percakapan dengan pemanggil sudah ditutup, jadi kita tidak boleh lagi memanggil onNext atau onCompleted. return memastikan kode di bawahnya tidak ikut jalan.
 *
 * - Membangun jawaban
 *   Class hasil generate Protobuf tidak dibuat dengan constructor biasa, melainkan dengan pola builder: newBuilder(), isi field satu per satu dengan setXxx(...), lalu build(). Nilai available adalah hasil perbandingan stok >= jumlah yang diminta.
 *
 * - onNext(response) lalu onCompleted()
 *   onNext mengirim jawaban, dan onCompleted memberi tahu pemanggil bahwa tidak ada jawaban lagi. Untuk RPC biasa (satu pertanyaan, satu jawaban), keduanya selalu dipanggil berpasangan seperti ini.
 */
@GrpcService
class InventoryGrpcService(
    private val productRepository: ProductRepository
): InventoryServiceGrpc.InventoryServiceImplBase() {

    override fun checkStock(
        request: CheckStockRequest,
        responseObserver: StreamObserver<CheckStockResponse?>
    ) {
        if(request.quantity <= 0) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription("Quantity must be greater than 0")
                    .asRuntimeException()
            )
            return
        }

        val product = productRepository.findByCode(request.productCode)
        if(product == null) {
            responseObserver.onError(
                Status.NOT_FOUND
                    .withDescription("Product with code ${request.productCode} is not found")
                    .asRuntimeException()
            )
            return
        }

        val response = CheckStockResponse.newBuilder()
            .setAvailable(product.stock >= request.quantity)
            .setCurrentStock(product.stock)
            .setProductName(product.name)
            .build()

        responseObserver.onNext(response)
        responseObserver.onCompleted()

    }

}