package com.shiccah.kafkatest

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service

@Service
@ConditionalOnProperty(value = ["example.kafka.consumer-enabled"], havingValue = "true")
class Consumer {

    val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    @KafkaListener(topics = ["INPUT_DATA"])
    fun consume(@Payload message: String,
                @Header(KafkaHeaders.OFFSET) offset: Int,
                @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) key: String,
                @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int,
                @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
                @Header(KafkaHeaders.RECEIVED_TIMESTAMP) ts: Long,
                acknowledgment: Acknowledgment
    ) {
        logger.info(String.format("#### -> Consumed message -> TIMESTAMP: %d\n%s\noffset: %d\nkey: %s\npartition: %d\ntopic: %s", ts, message, offset, key, partition, topic))
        acknowledgment.acknowledge()
    }
}