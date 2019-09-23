import common.SparkSessionPool;
import dataprocess.DataProcessor;
import dataprocess.input.DataSetFileInput;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.BeforeClass;
import org.junit.Test;
import query.ActionPatternResultGen;
import query.OnlyVisitPatternResultGen;
import query.VisitPathPatternResultGen;
import query.VisitedOncePatternResultGen;
import query.VisitedPatternResultGen;
import schema.Page;

import java.util.Arrays;
import java.util.function.Supplier;

public class PatternResultGenIntegrationTest
{
    @BeforeClass
    public static void setup()
    {
        Supplier<Dataset<Row>> dfProvider = new DataSetFileInput("input.csv", SparkSessionPool::getSparkSession);
        DataProcessor dp = new DataProcessor(dfProvider, false);
        dp.createVisitTable();
        dp.createSessionPathTable(SparkSessionPool.getSparkSession());
    }

    @Test
    public void testVisitedPattern()
    {
        VisitedPatternResultGen patternResultGen = new VisitedPatternResultGen();
        Dataset<Row> res = patternResultGen.forPage("OurPlanetTitle");
        res.coalesce(1)
           .write()
           .mode(SaveMode.Overwrite)
           .format("com.databricks.spark.csv")
           .option("header", "true")
           .csv("result/a");
        res.show();
    }

    @Test
    public void testVisitedOncePattern()
    {
        VisitedOncePatternResultGen patternResultGen = new VisitedOncePatternResultGen();
        Dataset<Row> res = patternResultGen.forPage("OurPlanetTitle");
        res.coalesce(1)
           .write()
           .mode(SaveMode.Overwrite)
           .format("com.databricks.spark.csv")
           .option("header", "true")
           .csv("result/b");
    }

    @Test
    public void testOnlyVisitPattern()
    {
        OnlyVisitPatternResultGen patternResultGen = new OnlyVisitPatternResultGen();
        Dataset<Row> res = patternResultGen.forPage("OurPlanetTitle");
        res.coalesce(1)
           .write()
           .mode(SaveMode.Overwrite)
           .format("com.databricks.spark.csv")
           .option("header", "true")
           .csv("result/e");
    }

    @Test
    public void testVisitPathPattern()
    {
        VisitPathPatternResultGen patternResultGen = new VisitPathPatternResultGen();
        Dataset<Row> res1 = patternResultGen.forPath(Arrays.asList("HomePage", "OriginalsGenre", "OurPlanetTitle", "HomePage"), VisitPathPatternResultGen.MatchType.EXACT);
        Dataset<Row> res2 = patternResultGen.forPath(Arrays.asList("OurPlanetTitle", "HomePage"), VisitPathPatternResultGen.MatchType.LOOSE);
        res1.coalesce(1)
           .write()
           .mode(SaveMode.Overwrite)
           .format("com.databricks.spark.csv")
           .option("header", "true")
           .csv("result/c/1");
        res2.coalesce(1)
           .write()
           .mode(SaveMode.Overwrite)
           .format("com.databricks.spark.csv")
           .option("header", "true")
           .csv("result/c/2");
    }

    @Test
    public void testActionPattern()
    {
        ActionPatternResultGen patternResultGen = new ActionPatternResultGen();
        Dataset<Row> res = patternResultGen.pageJump(Page.PageType.TITLE,"LogIn");
        res.coalesce(1)
           .write()
           .mode(SaveMode.Overwrite)
           .format("com.databricks.spark.csv")
           .option("header", "true")
           .csv("result/d");
    }
}
