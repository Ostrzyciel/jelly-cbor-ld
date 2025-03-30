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
import com.apicatalog.rdf.nquads.NQuadsReader;
import eu.ostrzyciel.jelly.convert.titanium.TitaniumJellyEncoder;
import eu.ostrzyciel.jelly.convert.titanium.TitaniumJellyReader;
import eu.ostrzyciel.jelly.convert.titanium.TitaniumJellyWriter;
import eu.ostrzyciel.jelly.core.JellyOptions$;
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame;
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame$;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.apache.commons.io.input.CountingInputStream;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
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

        System.out.println("\n\n-------------------\n\n");

        // Try to convert a VC to Jelly
        try (var is = Main.class.getResourceAsStream("/vc.nq")) {
            var nqBytes = is.readAllBytes();
            var nqString = new String(nqBytes);
            var jellyEncoder = TitaniumJellyEncoder.factory(JellyOptions$.MODULE$.smallStrict());

            System.out.println("frame\tN-Quads\tJelly");
            RdfStreamFrame frame0 = null;
            RdfStreamFrame frame1 = null;

            for (int i = 0; i < 20; i++) {
                new NQuadsReader(new StringReader(nqString)).provide(jellyEncoder);
                var jellyRows = jellyEncoder.getRowsScala();
                var jellyFrame = RdfStreamFrame$.MODULE$.of(
                    jellyRows, scala.collection.immutable.Map$.MODULE$.empty()
                );
                System.out.println(i + "\t" + nqBytes.length + "\t" + jellyFrame.toByteArray().length);

                if (i == 0) {
                    frame0 = jellyFrame;
                } else if (i == 1) {
                    frame1 = jellyFrame;
                }
            }

            System.out.println("\nFrame 0:\n");
            System.out.println(frame0.toProtoString());
            System.out.println("\nFrame 1:\n");
            System.out.println(frame1.toProtoString());
            System.out.println("\nFrame 1 as hex:\n");
            for (byte b : frame1.toByteArray()) {
                System.out.print(String.format("%02X ", b));
            }
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace(); // We are just experimenting here...
            System.exit(1);
        }
    }
}
