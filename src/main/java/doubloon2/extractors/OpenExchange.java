package doubloon2.extractors;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
public class OpenExchange {
    private static final String API_KEY="d246445f86aa4464918c9f5460c7b72c";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public double getPrice(String currency) {
        try {
            URL url = new URL("https://openexchangerates.org/api/latest.json?app_id=" + API_KEY);
            JsonNode root = MAPPER.readTree(url);
            JsonNode node = root.path("rates").path(currency);
            return Double.parseDouble(node.toString());
        }catch(MalformedURLException ex){
            Logger.getLogger(OpenExchange.class.getName()).log(Level.SEVERE,null,ex);
        }catch(IOException ex){
            Logger.getLogger(OpenExchange.class.getName()).log(Level.SEVERE,null,ex);
        }
        return 0;
    }
}
