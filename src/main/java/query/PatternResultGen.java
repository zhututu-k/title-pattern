package query;

import common.SparkSessionPool;
import org.apache.spark.sql.SparkSession;
import schema.VisitTableConstants;

public abstract class PatternResultGen
{
    public static String GLOBAL_TMP_NAMESPACE = "global_temp";
    protected static String visitTableName = GLOBAL_TMP_NAMESPACE + "." + VisitTableConstants.visitViewName;
    protected static String pathTableName = GLOBAL_TMP_NAMESPACE + "." + VisitTableConstants.sessionPathViewName;

    protected SparkSession sparkSession;

    public PatternResultGen()
    {
        this.sparkSession = SparkSessionPool.getSparkSession();
    }
}
