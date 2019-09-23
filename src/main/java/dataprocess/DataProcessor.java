package dataprocess;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schema.Page;

import javax.annotation.Nullable;
import java.util.function.Supplier;

import static schema.VisitTableConstants.*;

public class DataProcessor
{
    private static final Logger sLogger = LoggerFactory.getLogger(DataProcessor.class);

    private Dataset<Row> df;
    private boolean isPersistData;

    public DataProcessor(Supplier<Dataset<Row>> dfSupplier,
                         @Nullable Boolean isPersistData)
    {
        this.df = dfSupplier.get();
        this.isPersistData = isPersistData == null ? false : isPersistData;
    }

    public void createVisitTable() {
        sLogger.info("Started building visit table.");

        WindowSpec sessionWindowSpec = Window.partitionBy(df.col(SESSION_ID)).orderBy(df.col(TIMESTAMP));
        UserDefinedFunction typeUdf = functions.udf(
                (String name, String url) -> Page.PageType.of(name, url).toString(), DataTypes.StringType
        );
        UserDefinedFunction pageIdUdf = functions.udf(
                (UDF2<String, String, Object>) Page::getPageId, DataTypes.StringType
        );

        df = df.withColumn(PREV_PAGE_NAME, functions.lag(NAVIGATION_PAGE, 1).over(sessionWindowSpec)).
                withColumn(PREV_PAGE_URL, functions.lag(URL, 1).over(sessionWindowSpec))
               .withColumn(STEP_NO, functions.rank().over(sessionWindowSpec))
               .withColumn(PAGE_TYPE, typeUdf.apply(df.col(NAVIGATION_PAGE), df.col(URL)))
               .withColumn(PAGE_ID, pageIdUdf.apply(df.col(NAVIGATION_PAGE), df.col(URL)));

        WindowSpec sessionWholeWindowSpec = sessionWindowSpec.rangeBetween(Window.unboundedPreceding(), Window.unboundedFollowing());
        df = df.withColumn(TOTAL_STEPS, functions.last(STEP_NO).over(sessionWholeWindowSpec))
               .withColumn(PREV_PAGE_TYPE, typeUdf.apply(df.col(PREV_PAGE_NAME), df.col(PREV_PAGE_URL)));
        df = df.drop(PREV_PAGE_URL);
        df.show();
        df.createOrReplaceGlobalTempView(visitViewName);
        sLogger.debug("Finished building visit table the %d rows.", df.count());
        if (isPersistData) {
            sLogger.info("Persisting visit table data.");
            persistData(df);
        }
    }

    public void createSessionPathTable(SparkSession sparkSession) {
        sLogger.info("Started building session_path table.");

        sparkSession.udf().register(ConcatPathUDAF.name, new ConcatPathUDAF());
        WindowSpec windowSpecPrev = Window.partitionBy(df.col(SESSION_ID)).orderBy(df.col(TIMESTAMP));

        df = df.withColumn(PATH, functions.callUDF(ConcatPathUDAF.name, df.col(PAGE_ID)).over(windowSpecPrev));
        df = df.filter(STEP_NO + " = " + TOTAL_STEPS).select(df.col(USER_ID), df.col(SESSION_ID), df.col(PATH));
        df.show(false);
        df.createOrReplaceGlobalTempView(sessionPathViewName);
        sLogger.debug("Finished building session_path table the %d rows.", df.count());
        if(isPersistData) {
            sLogger.info("Persisting session_path table data.");
            persistData(df);
        }
    }

    private void persistData(Dataset<Row> df) {
        df.persist();
    }
}
