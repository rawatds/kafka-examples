package com.dsr.kafka.ex03;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;

public class SupplierSerializer implements Serializer<Supplier> {

    private String encoding = "UTF8";

    @Override
    public byte[] serialize(String topic, Supplier supplier) {

        try {
            if (supplier == null) {
                return null;
            }

            byte[] serializedName = supplier.getSupplierName().getBytes(encoding);
            byte[] serializedDoj = supplier.getDoj().toString().getBytes(encoding);

            int sizeName = serializedName.length;
            int sizeDoj = serializedDoj.length;

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + sizeName + 4 + sizeDoj + 8);

            buffer.putInt(supplier.getSupplierId());
            buffer.putInt(sizeName);
            buffer.put(serializedName);
            buffer.putInt(sizeDoj);
            buffer.put(serializedDoj);
            buffer.putDouble(supplier.getCreditLimit());

            System.out.println("Serialized data for supplierid :" + supplier.getSupplierId() + " : " + new String(buffer.array()));

            return buffer.array();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}

