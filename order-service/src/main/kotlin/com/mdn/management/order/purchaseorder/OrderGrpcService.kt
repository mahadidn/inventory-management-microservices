package com.mdn.management.order.purchaseorder

import com.mdn.management.grpc.inventory.CheckStockRequest
import com.mdn.management.grpc.inventory.InventoryServiceGrpc
import com.mdn.management.grpc.order.CreateOrderRequest
import com.mdn.management.grpc.order.CreateOrderResponse
import com.mdn.management.grpc.order.OrderServiceGrpc
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.grpc.server.service.GrpcService
import org.springframework.kafka.core.KafkaTemplate

@GrpcService
class OrderGrpcService(
    private val inventoryStub: InventoryServiceGrpc.InventoryServiceBlockingStub,
    private val purchaseOrderRepository: PurchaseOrderRepository,
    private val kafkaTemplate: KafkaTemplate<String, OrderCreatedEvent>
) : OrderServiceGrpc.OrderServiceImplBase() {

    private val logger = LoggerFactory.getLogger(OrderGrpcService::class.java)

    override fun createOrder(
        request: CreateOrderRequest,
        responseObserver: StreamObserver<CreateOrderResponse?>
    ) {
        val checkStockRequest = CheckStockRequest.newBuilder()
            .setProductCode(request.productCode)
            .setQuantity(request.quantity)
            .build()

        val checkStockResponse = try {
            inventoryStub.checkStock(checkStockRequest)
        } catch (exception: StatusRuntimeException) {
            responseObserver.onError(exception.status.asRuntimeException())
            return
        }

        if(!checkStockResponse.available) {
            responseObserver.onError(
                Status.FAILED_PRECONDITION
                    .withDescription(
                        "Stock is not enough for ${request.productCode}. " +
                        "Requested: ${request.quantity}, available: ${checkStockResponse.currentStock}"
                    )
                    .asRuntimeException()
            )
            return
        }

        val savedOrder = purchaseOrderRepository.save(
            PurchaseOrder(
                productCode = request.productCode,
                productName = checkStockResponse.productName,
                quantity = request.quantity,
                status = "CREATED",
            )
        )

        val savedOrderId = checkNotNull(savedOrder.id)

        val orderCreatedEvent = OrderCreatedEvent(
            orderId = savedOrderId,
            productCode = savedOrder.productCode,
            quantity = savedOrder.quantity
        )

        kafkaTemplate.send(OrderTopicConfig.ORDER_CREATED_TOPIC, savedOrder.productCode, orderCreatedEvent)
            .whenComplete { sendResult, exception ->
                if(exception != null){
                    logger.error("Failed to publish OrderCreatedEvent for order {}", savedOrderId, exception)
                } else {
                    val metadata = sendResult.recordMetadata
                    logger.info(
                        "Published OrderCreatedEvent for order {} to topic {} partition {} offset {}",
                        savedOrderId, metadata.topic(), metadata.partition(), metadata.offset()
                    )
                }
            }

        val response = CreateOrderResponse.newBuilder()
            .setOrderId(checkNotNull(savedOrder.id))
            .setProductCode(savedOrder.productCode)
            .setProductName(savedOrder.productName)
            .setQuantity(savedOrder.quantity)
            .setStatus(savedOrder.status)
            .build()

        responseObserver.onNext(response)
        responseObserver.onCompleted()

    }

}