package eu.ostrzyciel.experiments.jelly_cbor_ld;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

public class Compression {
    public static void main(String[] args) {
        String[] samples = {
            "/vc.cborld",
            "/vc.jsonld",
            "/vc.nq",
            "/vc_frame_0.jelly",
            "/vc_frame_1.jelly",
        };

        Map<String, Integer> sizes = new HashMap<>();

        for (String sample : samples) {
            byte[] data = null;
            try (var is = Compression.class.getResourceAsStream(sample)) {
                data = is.readAllBytes();
            } catch (Exception e) {
                e.printStackTrace();
            }

            // zstd
            for (int level = -7; level <= 22; level++) {
                var name = sample + ".zstd." + level;
                var bos = new ByteArrayOutputStream();
                try (var zos = new ZstdCompressorOutputStream(bos, level)) {
                    zos.write(data);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                sizes.put(name, bos.size());
            }

            // gzip
            for (int level = 0; level <= 9; level++) {
                var name = sample + ".gz." + level;
                var bos = new ByteArrayOutputStream();
                var params = new GzipParameters();
                params.setCompressionLevel(level);
                try (var zos = new GzipCompressorOutputStream(bos, params)) {
                    zos.write(data);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                sizes.put(name, bos.size());
            }
        }

        sizes.forEach((k, v) -> System.out.println(k + ": " + v));
    }
}
