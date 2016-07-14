package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;

/**
 * Created by endy on 2015/10/12.
 */

@Description(name = "murmur_hash3_array",
        value = "_FUNC_(ArrayList[, key_array]) - Returns the sorted entries of a map minus key value pairs, the for a given set of keys "
)
public class MurmurHash3Array extends UDF {
    public ArrayList<Integer> evaluate(ArrayList<String> list, IntWritable seed) {
        ArrayList<Integer> hashList = new ArrayList<Integer>();

        if (list == null || list.size() < 1) {
            return hashList;
        }

        for (String str : list) {
            hashList.add(hash_str(str, seed.get()));
        }

        return hashList;
    }

    public ArrayList<Integer> evaluate(ArrayList<String> list) {
        ArrayList<Integer> hashList = new ArrayList<Integer>();

        if (list == null || list.size() < 1) {
            return hashList;
        }

        for (String str : list) {
            hashList.add(hash_str(str));
        }

        return hashList;
    }

    private static int hash_str(String item, int seed) {
        // Offset: 0
        return mhash(item.getBytes(), 0, item.length(), seed);
    }

    private static int hash_str(String item) {
        // Offset: 0
        // Seed: 1
        return mhash(item.getBytes(), 0, item.length(), 1);
    }

    private static int mhash(byte[] data, int offset, int len, int seed) {

        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch (len & 0x03) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= data[roundedEnd] & 0xff;
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
                k1 *= c2;
                h1 ^= k1;
            default:
        }

        // finalization
        h1 ^= len;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

}
