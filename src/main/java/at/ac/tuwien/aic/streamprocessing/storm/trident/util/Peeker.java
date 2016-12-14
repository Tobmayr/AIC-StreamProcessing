package at.ac.tuwien.aic.streamprocessing.storm.trident.util;


import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * usage Stream.peek(new Peeker("XXX"))
 */

public class Peeker implements Consumer {
    public String id = "";
    public Peeker (String id) {
        this.id = id;
    }

    @Override
    public void accept(TridentTuple input) {
        System.out.println("Peeker(id:" + this.id + "): " + input.getValues());
    }
}
