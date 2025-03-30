package eu.ostrzyciel.experiments.jelly_cbor_ld;

import com.apicatalog.cborld.CborLd;
import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;
import com.apicatalog.jsonld.document.RdfDocument;
import com.apicatalog.jsonld.loader.DocumentLoader;
import com.apicatalog.jsonld.loader.DocumentLoaderOptions;
import com.apicatalog.rdf.RdfDataset;
import com.apicatalog.rdf.RdfDatasetSupplier;
import eu.ostrzyciel.jelly.convert.titanium.TitaniumJellyReader;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.apache.commons.io.input.CountingInputStream;

import java.net.URI;
import java.util.zip.GZIPInputStream;

public class Main {
    private static final DocumentLoader CBOR_LD_LOADER = new DocumentLoader() {
        private static JsonDocument context = null;

        static {
            try (var is = Main.class.getResourceAsStream("/context.jsonld")) {
                // parse with jakarta
                var parser = Json.createParser(is);
                parser.next();
                context = JsonDocument.of(parser.getObject());
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        @Override
        public Document loadDocument(URI url, DocumentLoaderOptions options) throws JsonLdError {
            // Hack: just return the context we have loaded...
            return context;
        }
    };

    public static void main(String[] args) throws Exception {
        var contextUri = Main.class.getResource("/context.jsonld").toURI();
        JsonObject context = Json.createObjectBuilder()
            .add("@context", contextUri.toString())
            .build();

        var inputFileName = "/assist-iot-weather-graphs_10k.jelly.gz";
        try (
            var is = Main.class.getResourceAsStream(inputFileName);
            var gzipIs = new GZIPInputStream(is);
            var cis = new CountingInputStream(gzipIs)
        ) {
            TitaniumJellyReader jellyReader = TitaniumJellyReader.factory();
            int i = 0;
            long lastByteCount = 0;
            while (cis.available() > 0) {
                var rdfDatasetSupplier = new RdfDatasetSupplier();
                jellyReader.parseFrame(rdfDatasetSupplier, cis);
                RdfDataset rdfDataset = rdfDatasetSupplier.get();
                JsonObject jsonLdDocument = JsonLd.fromRdf(RdfDocument.of(rdfDataset))
                    .get()
                    .getFirst().asJsonObject();
                JsonObject jsonLdCompacted = JsonLd
                    .compact(JsonDocument.of(jsonLdDocument), JsonDocument.of(context))
                    .compactArrays(true).get();

                byte[] cborLdBytes = CborLd.createEncoder()
                    .loader(CBOR_LD_LOADER)
                    .build()
                    .encode(jsonLdCompacted);


                if (i % 10 == 0) {
                    System.out.println("frame\tJelly\tJSON-LD\tJSON-LD (compact)\tCBOR-LD");
                }

                long byteCount = cis.getByteCount();
                System.out.println(i + "\t" + (byteCount - lastByteCount) + "\t" +
                        jsonLdDocument.toString().length() + "\t" +
                        jsonLdCompacted.toString().length() + "\t" +
                        cborLdBytes.length
                );
                lastByteCount = byteCount;

                if (i > 100) {
                    break;
                }
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace(); // We are just experimenting here...
            System.exit(1);
        }
    }
}
