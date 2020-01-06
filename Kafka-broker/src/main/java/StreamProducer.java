import kafka.utils.Json;
import kafka.utils.json.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class Ordre {
    public String company;
    public String typeOrdre;
    public int nbrAction;
    public Collection<Double> prices;

    public Ordre(String company, String typeOrdre, int nbrAction, Collection<Double> prices) {
        this.company = company;
        this.typeOrdre = typeOrdre;
        this.nbrAction = nbrAction;
        this.prices = prices;
        this.setPrices();
    }

    public void setPrices() {
        if(nbrAction > 0) {
            for (int i = 0; i < nbrAction; i++) {
                prices.add(Math.random()*10000);
            }
        }
    }

    @Override
    public String toString() {
        return "Ordre{" +
                "'company': '" + company + '\'' +
                ", 'typeOrdre': '" + typeOrdre + '\'' +
                ", 'nbrAction': " + nbrAction +
                ", 'prices': " + prices +
                '}';
    }
}

public class StreamProducer {
    private int counter;
    private String KAFKA_BROKER_URL = "localhost:9092";
    private String TOPIC_NAME = "testTopic";
    private String clientID = "client_prod_1";
    public static Map<Integer, String> companies = new HashMap<>();
    public static Map<Integer, String> typeOrdre = new HashMap<>();

    public static void loadCompanies() {
        companies.put(1, "RAM");
        companies.put(2, "OCP");
        companies.put(3, "AIRBUS");
        companies.put(4, "CCC");
        companies.put(5, "IBM");
        companies.put(6, "CASATRAMWAY");
    }

    public static void loadTypeOrdre() {
        typeOrdre.put(2, "VENTE");
        typeOrdre.put(1, "ACHAT");
    }

    public static void main(String[] args) {
        loadCompanies();
        loadTypeOrdre();
        new StreamProducer();
    }

    public StreamProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BROKER_URL);
        properties.put("client.id", clientID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(properties);
        Random random = new Random();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{
            ++counter;
            Ordre ordre = new Ordre(companies.get(random.nextInt(6)+1), typeOrdre.get(random.nextInt(2)+1), random.nextInt(10)+1,new ArrayList<>());
            String msg = ordre.toString();
            kafkaProducer.send(new ProducerRecord<Integer, String>(TOPIC_NAME, ++counter, msg), (metadata, ex) -> {
                System.out.println("Sending Message key: " + counter + ", Value: " + msg);
                System.out.println("Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
            });
        }, 10, 10, TimeUnit.MILLISECONDS);
    }
}
