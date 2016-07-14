package shuyun.java.cds.udf.bi;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by endy on 2015/10/10.
 * 返回输入的列是否有相同的值，是则返回0，否则返回1
 */

@Description(
        name = "is_column_value_same",
        value = " whether the column value is same"
)
public class IsColumnValueSame extends UDF {
    private static int MAX_VALUE = 50;
    private static String[] comparedColumn=new String[MAX_VALUE];
    private static int rowNum=1;

    public int evaluate(Object... args) {
        if(args==null)
            return 1;

        String[] columnValue = new String[args.length];
        int i;
        for(i = 0; i < args.length; ++i) {
            columnValue[i] = args[i].toString();
        }

        if(comparedColumn[0] == null) {
            for(i = 0; i < columnValue.length; ++i) {
                comparedColumn[i] = columnValue[i];
            }
            return rowNum;
        }
        else
        {
            for(i = 0; i < columnValue.length - 1; ++i) {
                if(!comparedColumn[i].equals(columnValue[i])) {
                    for(int j = 0; j < columnValue.length; ++j) {
                        comparedColumn[j] = columnValue[j];
                    }
                    rowNum = 1;
                    return rowNum;
                }
            }

            if(comparedColumn[args.length - 1].equals(columnValue[args.length - 1])) {
                rowNum = 0;
                return rowNum;
            } else {
                comparedColumn[args.length - 1] = columnValue[args.length - 1];
                rowNum = 1;
                return rowNum;
            }
        }
    }
}
