package shuyun.java.cds.udf.bi;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.Text;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by endy on 2015/10/12.
 */
@Description(
        name = "select_oldest_by_date_id",
        value = "_FUNC_(arg1,arg2,arg3,arg4)"
)
public class SelectOldestByDateAndIdUDAF extends UDAF {
    public static class SelectLatestUDAFEvaluator implements UDAFEvaluator {
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private SelectOldestByDateAndIdUDAF.SelectLatestUDAFEvaluator.PartialResult partialResult;

        public SelectLatestUDAFEvaluator() {
        }

        public void init() {
            this.partialResult = null;
        }

        public int dateCompare(String dateS1, String tid1, String dateS2, String tid2) {
            Date date1 = null;
            Date date2 = null;

            try {
                date1 = this.sdf.parse(dateS1);
                date2 = this.sdf.parse(dateS2);
                return date1.getTime() - date2.getTime() > 0L?1:(date1.getTime() - date2.getTime() == 0L?(new BigInteger(tid1, 16)).compareTo(new BigInteger(tid2, 16)):-1);
            } catch (ParseException var8) {
                var8.printStackTrace();
                return 0;
            }
        }

        public boolean iterate(String col, String com, String tid) {
            if(col != null && com != null && tid != null) {
                if(this.partialResult == null) {
                    this.partialResult = new SelectOldestByDateAndIdUDAF.SelectLatestUDAFEvaluator.PartialResult();
                    this.partialResult.column = col;
                    this.partialResult.compareDate = com;
                    this.partialResult.tid = tid;
                } else if(this.dateCompare(com, tid, this.partialResult.compareDate, this.partialResult.tid) < 0) {
                    this.partialResult.column = col;
                    this.partialResult.compareDate = com;
                    this.partialResult.compareDate = tid;
                }

                return true;
            } else {
                return true;
            }
        }

        public SelectOldestByDateAndIdUDAF.SelectLatestUDAFEvaluator.PartialResult terminatePartial() {
            return this.partialResult;
        }

        public boolean merge(SelectOldestByDateAndIdUDAF.SelectLatestUDAFEvaluator.PartialResult other) {
            if(other == null) {
                return true;
            } else {
                if(this.partialResult == null) {
                    this.partialResult = new SelectOldestByDateAndIdUDAF.SelectLatestUDAFEvaluator.PartialResult();
                    this.partialResult.column = other.column;
                    this.partialResult.compareDate = other.compareDate;
                    this.partialResult.tid = other.tid;
                } else if(this.dateCompare(other.compareDate, other.tid, this.partialResult.compareDate, this.partialResult.tid) < 0) {
                    this.partialResult.column = other.column;
                    this.partialResult.compareDate = other.compareDate;
                    this.partialResult.tid = other.tid;
                }

                return true;
            }
        }

        public Text terminate() {
            return this.partialResult == null?null:new Text(this.partialResult.column);
        }

        public static class PartialResult {
            String column;
            String compareDate;
            String tid;

            public PartialResult() {
            }
        }
    }
}
