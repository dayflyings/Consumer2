import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

public class consumer {
    private final static String QUEUE_NAME = "Resort";
    private static ObjectPool<Connection> connPool;
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        int numThreads = 64;
        factory.setHost("172.31.87.118");
        factory.setUsername("test");
        factory.setPassword("test");
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
//        factory.setRequestedHeartbeat(20);

        ExecutorService es = Executors.newFixedThreadPool(numThreads);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        JedisPool pool = new JedisPool(poolConfig, "172.31.91.246", 6379, 1800);
        connPool = new GenericObjectPool<>(new RMQConnectionFactory(factory));
        for (int i = 0; i < numThreads; i++) {
            es.execute(new Runnable() {
                @Override
                public void run() {
                    Channel channel = null;
                    Connection finalConnection = null;
                    try {
                        finalConnection = connPool.borrowObject();
                        channel = finalConnection.createChannel();
                        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                        channel.basicQos(5);
                        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (finalConnection != null) {
                                connPool.returnObject(finalConnection);
                            }
                        } catch (Exception ignored) {
                        }
                    }
                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//                        System.out.println(" [x] Received '" + message + "'");
                        String[] messageArray = message.split("\n");
                        String resortId = messageArray[0];
                        String seasonId = messageArray[1];
                        String dayId = messageArray[2];
                        String skierId = messageArray[3];
                        String time = messageArray[4];
                        String liftId = messageArray[5];
                        String waitTime = messageArray[6];
                        String value = resortId + "," + seasonId + "," + dayId + ",";
                        String key = skierId;
                        int count = 0;
                        String collectionName = resortId + "_" + dayId;
                        try (Jedis jedis = pool.getResource()) {
                            jedis.auth("123456");
                            // Add skierID into a resort-day set
                            jedis.sadd(collectionName, skierId);
                            // Put all liftID into a redis hash and maintain everyday record into different hash
                            String hashName = "hash_" + collectionName;
                            long insertHash = jedis.hsetnx(hashName, liftId, "1");
//                            System.out.println("Insert hash return: " + insertHash);
                            if (insertHash == 0) {
                                String hashRideTotal = jedis.hget(hashName, liftId);
//                                System.out.println("current value is :" + hashRideTotal);
                                Integer overall = 1 + Integer.parseInt(hashRideTotal);
                                jedis.hset(hashName, liftId, overall.toString());
                            }
                            // For each day, create a hash to store hourly lift ride data
                            Integer hour = Integer.parseInt(time) / 60;
                            long insertHourHash = jedis.hsetnx(dayId, hour.toString(), "1");
                            if (insertHourHash == 0) {
                                String currentHourTotal = jedis.hget(dayId, hour.toString());
                                Integer newTotal = 1 + Integer.parseInt(currentHourTotal);
                                jedis.hset(dayId, hour.toString(), newTotal.toString());
                            }
                        }
                    };
                    try {
                        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        es.shutdown();
        while (!es.awaitTermination(10, TimeUnit.SECONDS)) {
            System.out.println("Waiting for all threads finished");
        }
    }
}