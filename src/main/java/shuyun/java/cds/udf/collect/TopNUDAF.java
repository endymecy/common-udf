package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.LinkedList;

/**
 * Created by endy on 2015/10/13.
 */
public class TopNUDAF extends UDAF{
    public static class StringDoublePair implements Comparable<StringDoublePair> {
        public StringDoublePair() {
            this.key = null;
            this.value = null;
        }

        public StringDoublePair(String key, double value) {
            this.key = key;
            this.value = value;
        }

        public String getString() {
            return this.key;
        }

        public int compareTo(StringDoublePair o) {
            return o.value.compareTo(this.value);
        }

        private String key;
        private Double value;
    }

    public static class UDAFTopNState {
        // The head of the queue should contain the smallest element.
        //	private PriorityQueue<StringDoublePair> queue;
        private LinkedList<StringDoublePair> queue;
        private Integer N;
    }

    public static class UDAFTopNEvaluator implements UDAFEvaluator {

        UDAFTopNState state;

        public UDAFTopNEvaluator() {
            super();
            state = new UDAFTopNState();
            init();
        }

        public void init() {
            //state.queue = new PriorityQueue<StringDoublePair>();
            state.queue = new LinkedList<StringDoublePair>();
            state.N = null;
        }

        public boolean iterate(String key, Double value, Integer N) {
            //	    if (N == null || (state.N != null  && state.N != N)) {
            //		throw new UDAFTopNException();
            //	    }
            if (state.N == null) {
                state.N = N;
            }
            if (value != null) {
                state.queue.add(new StringDoublePair(key, value));
                prune(state.queue, state.N);
            }
            return true;
        }

        public UDAFTopNState terminatePartial() {
            if (state.queue.size() > 0) {
                return state;
            } else {
                return null;
            }
        }

        public boolean merge(UDAFTopNState o) {
            //	public boolean merge(UDAFTopNState o) throws UDAFTopNException {
            if (o != null) {
                state.queue.addAll(o.queue);
                if (o.N != state.N) {
                    //		    throw new UDAFTopNException();
                }
                prune(state.queue, state.N);
            }
            return true;
        }

        void prune(LinkedList<StringDoublePair> queue, int N) {
            while (queue.size() > N) {
                //		queue.remove();
                queue.removeLast();
            }
        }

        /**
         * Terminates the aggregation and return the final result.
         */
        public LinkedList<String> terminate() {
            LinkedList<String> result = new LinkedList<String>();
            while (state.queue.size() > 0) {
                StringDoublePair p = state.queue.poll();
                result.addFirst(p.getString());
            }
            return result;
        }
    }

}
