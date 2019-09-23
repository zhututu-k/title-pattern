package common;

import org.apache.spark.sql.SparkSession;

public class SparkSessionPool
{
    private static String masterUrl = "local";
    private static String appName = "Netflix Title Patterns";

    public static SparkSession getSparkSession()
    {
        return SparkSession.builder()
                           .master(masterUrl)
                           .appName(appName)
                           .getOrCreate();
    }
}
