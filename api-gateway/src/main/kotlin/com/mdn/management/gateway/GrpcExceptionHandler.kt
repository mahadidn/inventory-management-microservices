package com.mdn.management.gateway

import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class GrpcExceptionHandler {

    private val logger = LoggerFactory.getLogger(GrpcExceptionHandler::class.java)

    @ExceptionHandler(StatusRuntimeException::class)
    fun handleGrpcError(e: StatusRuntimeException): ProblemDetail {
        val grpcCode = e.status.code

        val httpStatus = when (grpcCode) {
            Status.Code.INVALID_ARGUMENT -> HttpStatus.BAD_REQUEST
            Status.Code.NOT_FOUND -> HttpStatus.NOT_FOUND
            Status.Code.FAILED_PRECONDITION -> HttpStatus.UNAUTHORIZED
            Status.Code.UNAVAILABLE -> HttpStatus.SERVICE_UNAVAILABLE
            Status.Code.DEADLINE_EXCEEDED -> HttpStatus.GATEWAY_TIMEOUT
            else -> HttpStatus.INTERNAL_SERVER_ERROR
        }

        logger.warn("gRPC call failed with {} -> HTTP {}", grpcCode, httpStatus.value())

        val detail = e.status.description ?: "Downstream service error ($grpcCode)"
        return ProblemDetail.forStatusAndDetail(httpStatus, detail)
    }

}