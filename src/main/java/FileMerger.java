import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class FileMerger {

    private final static String FILE_PATH_PREFIX = "src/main/resources/";
    private final static String CSV_FILE_SUFFIX = ".csv";
    private final static String RESULTING_FILE_NAME = "mergedSegments";
    private final static String SEGMENT_HEADER = "segment_id";
    private final static String DELIMITER = ",";

    public void merge(List<String> segmentsToMerge, List<String> segmentsToExclude, int mergerIndex) {
        Set<String> excludingKeys = getExcludingKeys(mergerIndex, segmentsToExclude);
        createResultingFile(segmentsToMerge, mergerIndex, excludingKeys);
    }

    private Set<String> getExcludingKeys(final int mergerIndex, final List<String> segmentsToExclude) {
        return segmentsToExclude.stream()
                .map(o -> getExcludingSegmentKeys(mergerIndex, o))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private Set<String> getExcludingSegmentKeys(final int mergerIndex, final String excludingSegment) {
        try (BufferedReader segmentReader = createBufferedReader(excludingSegment)) {
            return segmentReader.lines()
                    .map(o -> getMergingKey(o, mergerIndex))
                    .collect(Collectors.toSet());
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
        return Collections.emptySet();
    }

    private void createResultingFile(final List<String> segmentsToMerge, final int mergerIndex, final Set<String> excludingKeys) {
        try (BufferedWriter writer = createBufferedWriter(RESULTING_FILE_NAME)) {
            AtomicBoolean atomicBoolean = new AtomicBoolean();
            segmentsToMerge.forEach(o -> mergeSegments(mergerIndex, excludingKeys, o, writer, atomicBoolean));
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private void mergeSegments(final int mergerIndex,
                               final Set<String> keys,
                               final String segment,
                               BufferedWriter writer,
                               AtomicBoolean atomicBoolean) {
        try (BufferedReader segmentReader = createBufferedReader(segment)) {
            boolean secondarySegment = atomicBoolean.getAndSet(true);
            if (!secondarySegment) {
                writer.write(appendCsvValue(segmentReader.readLine(), SEGMENT_HEADER));
            }
            segmentReader.lines()
                    .skip(secondarySegment ? 1 : 0)
                    .filter(line -> isLineCouldBeProcessed(line, mergerIndex, keys))
                    .forEach(line -> writeLineToFile(writer, line, segment));
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private BufferedWriter createBufferedWriter(String segment) throws URISyntaxException, IOException {
        File file = new File(getFilePath(segment));
        file.delete();
        return Files.newBufferedWriter(Paths.get(file.getAbsolutePath()));
    }

    private BufferedReader createBufferedReader(String segment) throws URISyntaxException, IOException {
        File file = new File(getFilePath(segment));
        return Files.newBufferedReader(Paths.get(file.getAbsolutePath()));
    }

    private void writeLineToFile(final BufferedWriter writer, final String line, final String segment) {
        try {
            writer.newLine();
            writer.write(appendCsvValue(line, segment));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean isLineCouldBeProcessed(String string, int mergerIndex, Set<String> keys) {
        String mergingKey = getMergingKey(string, mergerIndex);
        boolean contains = keys.contains(mergingKey);
        if (!contains) {
            keys.add(mergingKey);
        }
        return !contains;
    }

    private String getMergingKey(final String string, final int mergerIndex) {
        String[] fields = string.split(DELIMITER);
        return fields[mergerIndex];
    }

    private String getFilePath(final String segment) {
        return FILE_PATH_PREFIX + segment + CSV_FILE_SUFFIX;
    }

    private String appendCsvValue(String row, String value) {
        return row + DELIMITER + value;
    }

}
