package at.ac.tuwien.aic.streamprocessing.model.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Timestamp {
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static LocalDateTime parse(String s) {
        return LocalDateTime.parse(s, dateTimeFormatter);
    }

    public static String toString(LocalDateTime dt) {
        return dateTimeFormatter.format(dt);
    }
}
