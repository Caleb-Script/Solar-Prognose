package com.solarprediction;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class SolarPrognoseConsumer {
    public static void main(String[] args) throws IOException {

        // Definiere den Pfad zur Log-Datei
        String logDateiPfad = "consumer.log";

        // Erstelle einen Logger
        final Logger logger = LoggerFactory.getLogger(SolarPrognoseConsumer.class);

        // Erstelle einen FileWriter, um den Wert in die Datei zu schreiben
        FileWriter dateiSchreiber = new FileWriter(logDateiPfad);

        // Konfiguriere SLF4J, um Protokolle in eine Datei zu schreiben
        org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        org.slf4j.LoggerFactory.getLogger(org.slf4j.impl.StaticLoggerBinder.class);

        // Setze den Pfad zur Log-Datei für SLF4J
         //System.setProperty("org.slf4j.simpleLogger.logFile", logDateiPfad);

        String bootstrapServer = "localhost:9092";
        String verbraucherGruppenID = "a";

        Properties eigenschaften = new Properties();
        eigenschaften.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        eigenschaften.put(ConsumerConfig.GROUP_ID_CONFIG, verbraucherGruppenID);
        eigenschaften.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        eigenschaften.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> verbraucher = new KafkaConsumer<>(eigenschaften);

        String thema = "solar-prognose";
        verbraucher.subscribe(Arrays.asList(thema));

        try{
            while (true) {
                ConsumerRecords<String, String> aufzeichnungen = verbraucher.poll(100);
                for (ConsumerRecord<String, String> aufzeichnung : aufzeichnungen) {
                    logger.info("\nNeue Nachricht: \n" +
                            "Schlüssel: " + aufzeichnung.key() + ", " +
                            "Wert: " + aufzeichnung.value() + ", " +
                            "Thema: " + aufzeichnung.topic() + ", " +
                            //"Partition: " + aufzeichnung.partition() + ", " +
                            "Offset: " + aufzeichnung.offset() +
                            "\n");

                    // Schreibe den Wert in die Datei
                    String wert = aufzeichnung.value();
                    dateiSchreiber.write(wert + "\n");
                    dateiSchreiber.flush();
                }
            }
        } catch (Exception e){
            System.out.println(e);
        } finally {
            // Schließe den FileWriter am Ende
            if (dateiSchreiber != null) {
                dateiSchreiber.close();
            }
        }

    }
}
