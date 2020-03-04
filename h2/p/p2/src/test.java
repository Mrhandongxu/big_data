/*
 * test.java
 *
 * @author Dongxu Han {@literal <dh5913@rit.edu>}
 */

import javax.sound.midi.SoundbankResource;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class test {
    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("\"(.*?)\"");
        Matcher matcher = pattern.matcher("\"The Chemist\",\"The India " +
            "Rubber Head\", \"fdas\"");
        while (matcher.find()) {
            System.out.println(matcher.group());
        }
    }
}
