package com.shiccah.kafkatest

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.support.SendResult
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.util.concurrent.ListenableFuture
import java.time.LocalDate
import java.util.concurrent.ExecutionException


@Component
class SendMessageTask(private val producer: Producer) {
    private val logger: Logger = LoggerFactory.getLogger(SendMessageTask::class.java)

    // run every 3 sec
    @Scheduled(fixedRateString = "3000")
    @Throws(ExecutionException::class, InterruptedException::class)
    fun send() {
        val listenableFuture: ListenableFuture<SendResult<String, String>> = producer.sendMessage("INPUT_DATA", "IN_KEY", LocalDate.now().toString())
        val result: SendResult<String, String> = listenableFuture.get()
        logger.info(java.lang.String.format("Produced:\ntopic: %s\noffset: %d\npartition: %d\nvalue size: %d", result.recordMetadata.topic(),
                result.recordMetadata.offset(),
                result.recordMetadata.partition(), result.recordMetadata.serializedValueSize()))
    }
}