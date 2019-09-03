package com.kero99.wp

import java.util.Properties
import java.util.Arrays

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
/**
  *
  * @author wp
  * @date 2019-09-03 16:17
  *
  */
object kafka_consumer extends App{
  var groupid = "wp_gp"
  var consumerid = "wp"
  var topic = "d1903"
  val props:Properties = new Properties()
  props.put("bootstrap.servers", "apache01:9092,apache02:9092,apache03:9092")
  props.put("group.id", groupid)
  props.put("client.id", "test")
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)
  props.put("consumer.id", consumerid)
  props.put("auto.offset.reset", "latest")
  props.put("auto.commit.enable", "true")
  props.put("auto.commit.interval.ms", "100")
  val consumer =new KafkaConsumer[String,String](props)
  consumer.subscribe(Arrays.asList(topic))
  while(true){
    val tmp = consumer.poll(1000)
    val it = tmp.iterator()
    if(it.hasNext){
      val item = it.next();
      println(item.key()+":"+item.value())
    }
  }
}
