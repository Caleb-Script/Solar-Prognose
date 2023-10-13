package com.solarprediction;


        import com.fasterxml.jackson.databind.JsonNode;
        import com.fasterxml.jackson.databind.ObjectMapper;
        import com.google.gson.JsonArray;
        import com.google.gson.JsonObject;
        import com.google.gson.JsonParser;
        import org.apache.http.client.HttpClient;
        import org.apache.http.client.methods.HttpGet;
        import org.apache.http.impl.client.HttpClients;
        import org.apache.http.util.EntityUtils;
        import org.apache.kafka.clients.producer.KafkaProducer;
        import org.apache.kafka.clients.producer.Producer;
        import org.apache.kafka.clients.producer.ProducerRecord;
        import org.apache.kafka.common.serialization.StringSerializer;

        import java.io.BufferedReader;
        import java.io.InputStreamReader;
        import java.io.UnsupportedEncodingException;
        import java.net.HttpURLConnection;
        import java.net.URL;
        import java.net.URLEncoder;
        import java.time.LocalDate;
        import java.time.format.DateTimeFormatter;
        import java.util.Iterator;
        import java.util.Map;
        import java.util.Properties;
        import java.util.Scanner;

 class SolarPrognoseProducer {
    public static void main(String[] args) {

        // Erstellen Sie einen Scanner, um die Konsole zu lesen
        Scanner scanner = new Scanner(System.in);

        // Benachrichtigt den Benutzer und fordern eine Adresse an
        System.out.println("Gib eine Adresse ein (Straße Nr, PLZ Ort): ");

        // Lesen Sie die Eingabe des Benutzers
        String adresse = scanner.nextLine();

        // Benachrichtigt den Benutzer und fordert die Solarleistung an
        System.out.println("Geben Sie die Leistung der Solaranlage (in Watt) an: ");
        float solarLeistung = scanner.nextFloat(); // Liest eine Fließkommazahl von der Konsole

        adresse = Geocoding(adresse);

        SolarPrognoseServer(adresse, solarLeistung);

        // Schließe den Scanner, weil man ihn nicht mehr braucht
        scanner.close();
    }

    private static String Geocoding(String adresse) {
        String daten = null;

        try {
            // Nominatim-Anfrage
            String codierteAdresse = URLEncoder.encode(adresse, "UTF-8");
            String url = "https://nominatim.openstreetmap.org/search?q=" + codierteAdresse + "&format=json";
            HttpURLConnection verbindung = (HttpURLConnection) new URL(url).openConnection();
            verbindung.setRequestMethod("GET");

            int antwortCode = verbindung.getResponseCode();
            if (antwortCode == 200) {
                BufferedReader leser = new BufferedReader(new InputStreamReader(verbindung.getInputStream()));
                StringBuilder antwort = new StringBuilder();
                String zeile;

                while ((zeile = leser.readLine()) != null) {
                    antwort.append(zeile);
                }
                leser.close();

                // JSON-Antwort analysieren
                JsonParser parser = new JsonParser();
                JsonArray ergebnisArray = parser.parse(antwort.toString()).getAsJsonArray();

                if (ergebnisArray.size() > 0) {
                    JsonObject ort = ergebnisArray.get(0).getAsJsonObject();
                    double breitengrad = ort.get("lat").getAsDouble();
                    double laengengrad = ort.get("lon").getAsDouble();

                    String formatierterBreitengrad = String.format("%.2f", breitengrad);
                    String formatierterLaengengrad = String.format("%.2f", laengengrad);

                    daten = String.format("%s/%s", formatierterBreitengrad, formatierterLaengengrad);
                } else {
                    System.out.println("Adresse nicht gefunden.");
                }
            } else {
                System.out.println("Fehler bei der Anfrage. HTTP-Statuscode: " + antwortCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return daten;
    }

    private static void SolarPrognoseServer(String adresse, float solarLeistung) {
        // Konfiguration für den Kafka-Producer
        Properties kafkaEigenschaften = new Properties();
        kafkaEigenschaften.put("bootstrap.servers", "localhost:9092");
        kafkaEigenschaften.put("key.serializer", StringSerializer.class.getName());
        kafkaEigenschaften.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> produzent = new KafkaProducer<>(kafkaEigenschaften);

        try {
            // Erstellen Sie die API-Anfrage-URL mit Adresse und Leistung
            String url = "https://api.forecast.solar/estimate/watthours/day/%s/%.2f/1.00/1.00?time=utc";
            String apiUrl = String.format(url, adresse, solarLeistung);

            // HTTP-Client erstellen
            HttpClient httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(apiUrl);

            // API-Anfrage senden und Antwort abrufen
            String antwort = EntityUtils.toString(httpClient.execute(httpGet).getEntity());

            // Extrahieren Sie die Solarproduktionswerte aus der Antwort
            String solarPrognose = extrahiereSolarproduktionFuersAktuellesDatum(antwort);

            // Nachricht an Kafka senden
            ProducerRecord<String, String> aufzeichnung = new ProducerRecord<>("solar-prognose", solarPrognose);
            produzent.send(aufzeichnung);

            produzent.close();
        } catch (UnsupportedEncodingException e) {
            // Hier können Sie den Fehler behandeln oder die Ausnahme auslösen, falls erforderlich
            System.out.println("Fehler: " + e); // Hier wird die Ausnahme einfach gedruckt
        } catch (Exception e) {
            System.out.println("Fehler: " + e);
        }
    }

    private static String extrahiereSolarproduktionFuersAktuellesDatum(String antwort) {
        try {
            // Verwenden Sie eine JSON-Verarbeitungsbibliothek, um das JSON-Objekt aus der API-Antwort zu extrahieren.
            ObjectMapper objektmapper = new ObjectMapper();
            JsonNode wurzelknoten = objektmapper.readTree(antwort);

            // Das aktuelle Datum im gewünschten Format (z. B. "2023-10-13")
            LocalDate aktuellesDatum = LocalDate.now();
            DateTimeFormatter datumsformatierer = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String aktuellesDatumString = aktuellesDatum.format(datumsformatierer);

            System.out.println(aktuellesDatum);

            // Prüfen Sie, ob die API-Antwort ein Feld für das aktuelle Datum enthält, wobei Uhrzeit ignoriert wird
            boolean gefunden = false;
            JsonNode ergebnisKnoten = wurzelknoten.path("result");
            Iterator<Map.Entry<String, JsonNode>> feldIterator = ergebnisKnoten.fields();

            System.out.println(antwort);

            while (feldIterator.hasNext()) {
                Map.Entry<String, JsonNode> eintrag = feldIterator.next();
                String feldName = eintrag.getKey();

                if (feldName.startsWith(aktuellesDatumString)) {
                    JsonNode datumKnoten = eintrag.getValue();

                    if (datumKnoten.isInt()) {
                        // Wenn das Feld gefunden wird und ein Integer ist, extrahieren Sie den Wert.
                        int solarProduktion = datumKnoten.asInt();
                        return "Solarproduktion am " + feldName + ": " + solarProduktion + " Wattstunden (Wh)";
                    } else {
                        return "Fehler: Solarproduktion für das aktuelle Datum ist kein ganzzahliger Wert.";
                    }
                }
            }

            if (!gefunden) {
                return "Für das aktuelle Datum wurde keine Solarproduktion gefunden.";
            }
        } catch (Exception e) {
            // Hier können Sie Fehlerbehandlung hinzufügen, wenn beim Extrahieren der Prognose ein Fehler auftritt.
            System.out.println("Fehler beim Extrahieren der Solarproduktion: " + e.getMessage());
            return "Fehler: Fehler bei der Extraktion der Solarproduktion";
        }
        // Fügen Sie ein Standard-Rückgabewert hinzu, wenn keine der Bedingungen erfüllt ist
        return "Keine Solarproduktion gefunden."; // Oder einen geeigneten Text nach Ihren Anforderungen.
    }
}