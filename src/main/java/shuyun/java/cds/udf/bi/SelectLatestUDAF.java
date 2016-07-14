package shuyun.java.cds.udf.bi;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by endy on 2015/10/12.
 */
@Description(
        name = "select_latest",
        value = "_FUNC_(arg1,arg2)"
)
public class SelectLatestUDAF extends UDAF {
    public static class SelectLatestUDAFEvaluator implements UDAFEvaluator {
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private SelectLatestUDAF.SelectLatestUDAFEvaluator.PartialResult partialResult;

        public SelectLatestUDAFEvaluator() {
        }

        public void init() {
            this.partialResult = null;
        }

        public int dateCompare(String dateS1, String dateS2) {
            Date date1 = null;
            Date date2 = null;

            try {
                date1 = this.sdf.parse(dateS1);
                date2 = this.sdf.parse(dateS2);
                return date1.getTime() - date2.getTime() > 0L?1:(date1.getTime() - date2.getTime() == 0L?0:-1);
            } catch (ParseException var6) {
                var6.printStackTrace();
                return 0;
            }
        }

        public boolean iterate(String col, String com) {
            if(col != null && com != null) {
                if(this.partialResult == null) {
                    this.partialResult = new SelectLatestUDAF.SelectLatestUDAFEvaluator.PartialResult();
                    this.partialResult.column = col;
                    this.partialResult.compareDate = com;
                } else if(this.dateCompare(com, this.partialResult.compareDate) > 0) {
                    this.partialResult.column = col;
                    this.partialResult.compareDate = com;
                }

                return true;
            } else {
                return true;
            }
        }

        public SelectLatestUDAF.SelectLatestUDAFEvaluator.PartialResult terminatePartial() {
            return this.partialResult;
        }

        public boolean merge(SelectLatestUDAF.SelectLatestUDAFEvaluator.PartialResult other) {
            if(other == null) {
                return true;
            } else {
                if(this.partialResult == null) {
                    this.partialResult = new SelectLatestUDAF.SelectLatestUDAFEvaluator.PartialResult();
                    this.partialResult.column = other.column;
                    this.partialResult.compareDate = other.compareDate;
                } else if(this.dateCompare(other.compareDate, this.partialResult.compareDate) > 0) {
                    this.partialResult.column = other.column;
                    this.partialResult.compareDate = other.compareDate;
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

            public PartialResult() {
            }
        }
    }

    }
