package eu.ostrzyciel.experiments.jelly_cbor_ld;

import com.apicatalog.cborld.CborLd;
import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.document.JsonDocument;
import com.apicatalog.jsonld.document.RdfDocument;
import com.apicatalog.rdf.RdfDataset;
import com.apicatalog.rdf.RdfDatasetSupplier;
import eu.ostrzyciel.jelly.convert.titanium.TitaniumJellyReader;
import jakarta.json.Json;
import jakarta.json.JsonObject;

import java.util.zip.GZIPInputStream;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello, World!");

        JsonObject context = null;
        try (var is = Main.class.getResourceAsStream("/context.jsonld")) {
            // parse with jakarta
            var parser = Json.createParser(is);
            parser.next();
            context = parser.getObject();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        var inputFileName = "/assist-iot-weather-graphs_10k.jelly.gz";
        try (
            var is = Main.class.getResourceAsStream(inputFileName);
            var gzipIs = new GZIPInputStream(is)
        ) {
            TitaniumJellyReader jellyReader = TitaniumJellyReader.factory();
            while (gzipIs.available() > 0) {
                var rdfDatasetSupplier = new RdfDatasetSupplier();
                jellyReader.parseFrame(rdfDatasetSupplier, gzipIs);
                RdfDataset rdfDataset = rdfDatasetSupplier.get();
                JsonObject jsonLdDocument = JsonLd.fromRdf(RdfDocument.of(rdfDataset))
                    .get()
                    .getFirst().asJsonObject();
                JsonObject jsonLdCompacted = JsonLd
                    .compact(JsonDocument.of(jsonLdDocument), JsonDocument.of(context))
                    .compactArrays(true).get();

                // Doesn't work. Context with multiple entries is not supported.
                byte[] cborLdBytes = CborLd.createEncoder().build().encode(jsonLdCompacted);
            }
        } catch (Exception e) {
            e.printStackTrace(); // We are just experimenting here...
            System.exit(1);
        }
    }

    private static void createCborLdEncoder() {
        //
    }
}