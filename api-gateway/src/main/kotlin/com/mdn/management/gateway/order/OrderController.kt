package com.mdn.management.gateway.order

import com.mdn.management.grpc.order.CreateOrderRequest
import com.mdn.management.grpc.order.OrderServiceGrpc
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/orders")
class OrderController(
    private val orderStub: OrderServiceGrpc.OrderServiceBlockingStub
) {

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun createOrder(
        @Valid @RequestBody request: CreateOrderRestRequest
    ): OrderRestResponse {
        val grpcRequest = CreateOrderRequest.newBuilder()
            .setProductCode(request.productCode)
            .setQuantity(request.quantity)
            .build()

        val grpcResponse = orderStub.createOrder(grpcRequest)

        return OrderRestResponse(
            orderId = grpcResponse.orderId,
            productCode = grpcResponse.productCode,
            productName = grpcResponse.productName,
            quantity = grpcResponse.quantity,
            status = grpcResponse.status
        )
    }

}