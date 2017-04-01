import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        FileMerger fileMerger = new FileMerger();
        List<String> segmentsToMerge = Arrays.asList("segment1", "segment2", "segment3");
        List<String> segmentsToExclude = Collections.singletonList("segment4");
        fileMerger.merge(segmentsToMerge, segmentsToExclude, 0);
    }
}
