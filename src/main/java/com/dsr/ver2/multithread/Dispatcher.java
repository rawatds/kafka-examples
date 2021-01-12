package com.dsr.ver2.multithread;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;


@NoArgsConstructor
@AllArgsConstructor
@Data
public class Dispatcher implements Runnable {

    private KafkaProducer<Integer, String> producer;
    private String topic;
    private String fileLocation;

    private static Logger logger = LogManager.getLogger();

    @Override
    public void run() {
        logger.info("Processing " + fileLocation);

        File file = new File(fileLocation);
        int lines = 0;

        try (Scanner scanner = new Scanner(file)) {

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();

                producer.send(new ProducerRecord<Integer, String>(topic, null, line));
                lines++;
            }

            logger.info("Total lines sent " + lines + " from " + fileLocation);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
