package query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static schema.VisitTableConstants.NAVIGATION_PAGE;

public class VisitedPatternResultGen extends PatternResultGen
{

    public VisitedPatternResultGen()
    {
        super();
    }

    /**
     * Find users that have visited the given page
     *
     * Example sql:
     *  SELECT * FROM visits
     *  WHERE nagivation_page = 'LogIn'
     *
     * @param pageName name of the given page
     * @return result dataset
     */
    public Dataset<Row> forPage(String pageName) {
        return sparkSession.read().table(visitTableName)
                           .filter(NAVIGATION_PAGE + " = '" + pageName + "'");
    }
}
