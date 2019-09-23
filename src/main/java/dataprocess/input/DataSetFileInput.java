package dataprocess.input;

import com.google.common.io.Resources;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

public class DataSetFileInput implements Supplier<Dataset<Row>>
{
    private final String resourceName;
    private final Supplier<SparkSession> sparkSessionSupplier;

    public DataSetFileInput(String resourceName, Supplier<SparkSession> sparkSessionSupplier)
    {
        this.resourceName = resourceName;
        this.sparkSessionSupplier = sparkSessionSupplier;
    }

    @Override
    public Dataset<Row> get()
    {
        String resourcePath = Resources.getResource(resourceName).toString();
        return sparkSessionSupplier.get()
                                   .read()
                                   .option("header", "true")
                                   .csv(resourcePath);
    }
}