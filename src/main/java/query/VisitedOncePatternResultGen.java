package query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static schema.VisitTableConstants.NAVIGATION_PAGE;
import static schema.VisitTableConstants.USER_ID;


public class VisitedOncePatternResultGen extends PatternResultGen
{
    public VisitedOncePatternResultGen()
    {
        super();
    }

    /**
     * Find users that have visited the given page only once
     *
     * Example sql:
     *  SELECT * FROM visits
     *  WHERE nagivation_page = 'LogIn'
     *  GROUP BY user_id
     *  HAVING count = 1
     *
     * @param pageName name of the given page
     * @return result dataset
     */
    public Dataset<Row> forPage(String pageName) {
        Dataset<Row> df = sparkSession.read().table(visitTableName);
        return df.filter(NAVIGATION_PAGE + " = '" + pageName + "'")
                 .groupBy(df.col(USER_ID))
                 .count()
                 .filter("count = 1");
    }
}
