package com.mdn.management.gateway

import com.mdn.management.grpc.order.OrderServiceGrpc
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.grpc.client.ImportGrpcClients

@SpringBootApplication
@ImportGrpcClients(
	target = "order",
	types = [OrderServiceGrpc.OrderServiceBlockingStub::class]
)
class ApiGatewayApplication

fun main(args: Array<String>) {
	runApplication<ApiGatewayApplication>(*args)
}
