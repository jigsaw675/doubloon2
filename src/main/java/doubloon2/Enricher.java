package doubloon2;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.maxmind.geoip.Location;
import doubloon2.extractors.GeoIP;
import java.io.IOException;

import doubloon2.extractors.OpenExchange;
import org.apache.kafka.clients.producer.*;
public class Enricher implements Producer{
    private KafkaProducer<String,String> producer;
    private String goodTopic,badTopic;
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    public Enricher(String servers, String goodTopic, String badTopic){
        this.producer = new KafkaProducer<String, String>(Producer.createConfig(servers));
        this.goodTopic = goodTopic;
        this.badTopic = badTopic;
    }

    @Override
    public void produce(String message){

        try{
            JsonNode root = MAPPER.readTree(message);
            JsonNode temp = root.path("customer").path("ipAddress");
            if(temp.isMissingNode()){
                String value = "{\"error\":\"customer.ipAddress is missing\"}";
                Producer.write(this.producer,badTopic,value);
            }else{
                String ipAddress = temp.textValue();
                Location location = new GeoIP().getLocation(ipAddress);
                ((ObjectNode) root).with("customer").put("country",location.countryName);
                ((ObjectNode) root).with("customer").put("city",location.city);
                Double rate = new OpenExchange().getPrice("BTC");
                ((ObjectNode) root).with("currency").put("rate",rate);
                String value = MAPPER.writeValueAsString(root);
                Producer.write(this.producer,goodTopic,value);
            }
        }catch(IOException e){
            String value = "{\"error\":\""+ e.getClass().getSimpleName()+": "+e.getMessage()+"\"}";
            Producer.write(this.producer,badTopic,value);
        }
    }
}
