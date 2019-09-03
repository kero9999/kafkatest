import java.util
import java.util.{Properties, TimerTask}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
/**
  *
  * @author wp
  * @date 2019-09-03 15:14
  *
  */


object Test extends App{
  val random = new java.util.Random()
  val timer=new java.util.Timer()
  timer.schedule(new TimerTask {
    override def run(): Unit = {
      val msg = s"测试消息${random.nextInt(100)}"
      WpProducer.sendmessage("d1903","data",msg)
      println(s"生产者:${msg}")
    }
  },0,1000)
}

object WpProducer {
  def sendmessage(topic:String,key:String,msg:String): Unit = {
    val props = new Properties()
    val brokers = "apache01:9092,apache02:9092"
    props.put("delete.topic.enable", "true")
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
//    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", classOf[HashPartitioner].getName)
    props.put("queue.buffering.max.messages", "1000000")
    props.put("queue.enqueue.timeout.ms", "20000000")
    props.put("batch.num.messages", "1")
    props.put("producer.type", "sync")
    val producer = new KafkaProducer[String, String](props)
    val message = new ProducerRecord[String,String](topic,key,msg)
    producer.send(message)  //发送到指定的topic
  }
}

class HashPartitioner extends Partitioner{
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    val count = cluster.partitionCountForTopic(topic)
    key.hashCode()%count
  }

  override def close(): Unit = {

  }

  override def configure(configs: util.Map[String, _]): Unit = {

  }
}
