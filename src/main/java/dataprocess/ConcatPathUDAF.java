package dataprocess;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static schema.VisitTableConstants.PAGE_ID;


public class ConcatPathUDAF extends UserDefinedAggregateFunction
{
    public static String concatChar = "::";
    static String name = "ConcatPathFun";
    private static String bufferFieldName = "pathBuffer";
    private StructType inputSchema;
    private StructType bufferSchema;

    ConcatPathUDAF()
    {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField(PAGE_ID, DataTypes.StringType, false));
        inputSchema = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField(bufferFieldName, DataTypes.StringType, false));
        bufferSchema = DataTypes.createStructType(bufferFields);
    }

    public StructType inputSchema()
    {
        return inputSchema;
    }

    public StructType bufferSchema()
    {
        return bufferSchema;
    }

    public DataType dataType()
    {
        return DataTypes.StringType;
    }

    public boolean deterministic()
    {
        return true;
    }

    public void initialize(MutableAggregationBuffer buffer)
    {
        buffer.update(0, "");
    }

    public void update(MutableAggregationBuffer buffer, Row input)
    {
        if (!input.isNullAt(0)) {
            String newValue = buffer.getString(0).length() == 0 ? input.getString(0) : buffer.getString(0) + concatChar + input.getString(0) ;
            buffer.update(0, newValue);
        }
    }

    public void merge(MutableAggregationBuffer buffer1, Row buffer2)
    {
        buffer1.update(0, buffer1.getString(0) + concatChar + buffer2.getString(0));
    }

    public String evaluate(Row buffer)
    {
        return buffer.getString(0);
    }
}
