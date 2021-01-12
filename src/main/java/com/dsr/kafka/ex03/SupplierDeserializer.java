package com.dsr.kafka.ex03;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class SupplierDeserializer implements Deserializer<Supplier> {

    private String encoding = "UTF8";

    @Override
    public Supplier deserialize(String topic, byte[] data) {

        try {

            if (data == null) {
                System.err.println("Invalid data to serialize");
                return null;
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);

            int supplierId = buffer.getInt();

            int sizeName = buffer.getInt();
            byte[] nameBytes = new byte[sizeName];
            buffer.get(nameBytes);
            String deserializedName = new String(nameBytes, encoding);

            int sizeDoj = buffer.getInt();
            byte[] dojBytes = new byte[sizeDoj];
            buffer.get(dojBytes);
            String deserializedDoj = new String(dojBytes, encoding);

            DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

            double creditLimit = buffer.getDouble();

            Supplier supplier = new Supplier(supplierId, deserializedName, df.parse(deserializedDoj), creditLimit);

            System.out.println("Deserialized: " + supplier);

            return supplier;

        } catch (Exception e) {
            System.err.println("Error while deserialzing : " + e.getMessage());
        }

        return null;

    }


}
