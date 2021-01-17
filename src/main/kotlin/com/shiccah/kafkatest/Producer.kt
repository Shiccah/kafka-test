package com.shiccah.kafkatest

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import org.springframework.util.concurrent.ListenableFuture

@Service
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
class Producer(private val kafkaTemplate: KafkaTemplate<String, String>) {

    fun sendMessage(topic: String, key: String, message: String): ListenableFuture<SendResult<String, String>> {
        return this.kafkaTemplate.send(topic, key, message);
    }
}