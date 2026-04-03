package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class TaskTwoTests {
    static final Logger logger = LoggerFactory.getLogger(TaskTwoTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void task_two_verifier() throws InterruptedException {
        String[] transactionLines = fileLoader.loadStrings("/test_data/poiuytrewq.uiop");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(2000);
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("use your debugger to watch for incoming transactions");

        logger.info("kill this test once you find the answer");
        float Array [] = new float[4];
        int count = 0;

for (String transactionLine : transactionLines) {
    kafkaProducer.send(transactionLine);

    // Extract amount
    String[] parts = transactionLine.split(",");
    String amount = parts[2];

    if (count < 4) {
        logger.info("Transaction Amount {}: {}", count + 1, amount);
        System.out.println("Transaction Amount " + (count + 1) + ": " + amount);
        Array[count] = Float.parseFloat(amount);
        count++;
    }
}
        while (true) {
            Thread.sleep(20000);
            System.out.println("The Second methods Answer is: [" + (Array[0] +" , " + Array[1] +" , "+ Array[2]+" , " + Array[3]+" ]56] "));
            logger.info("...");
        }
    }

}
