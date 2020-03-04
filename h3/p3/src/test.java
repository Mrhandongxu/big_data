/*
 * test.java
 *
 * @author Dongxu Han {@literal <dh5913@rit.edu>}
 */

import java.io.*;
import java.util.*;
/**
 *
 */
public class test {
    public static void main(String[] args) {
        setEqualWIthSameContentes();
    }

    private static void setEqualWIthSameContentes() {
        Set<String> set1 = new HashSet<>();
        Set<String> set2 = new HashSet<>();
        set1.add("1");
        set1.add("21");
        set1.add("32");
        System.out.println(set1);
        set2.add("32");
        set2.add("21");
        set2.add("1");
        System.out.println(set2);
        System.out.println(set1.equals(set2));
        Map<Set<String>, String> map = new HashMap<>();
        map.put(set1, "1");
        map.put(set2, "2");
        System.out.println(map.size());
    }
}
