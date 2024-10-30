package w08r_flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.functions.KeySelector;

public class Keyer implements KeySelector<Tuple2<String, Long>, String> {
    public String getKey(Tuple2<String, Long> in) {
        return in.getField(0);
    }
}
