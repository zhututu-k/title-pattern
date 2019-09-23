package query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static schema.VisitTableConstants.NAVIGATION_PAGE;
import static schema.VisitTableConstants.TOTAL_STEPS;

public class OnlyVisitPatternResultGen extends PatternResultGen
{
    public OnlyVisitPatternResultGen()
    {
        super();
    }

    /**
     * Find users that have only visited the given page
     *
     * Example sql:
     *  SELECT * FROM visits
     *  WHERE nagivation_page = 'LogIn'
     *    AND total_steps = 1
     *
     * @param pageName name of the given page
     * @return result dataset
     */
    public Dataset<Row> forPage(String pageName) {
        return sparkSession.read().table(visitTableName)
                           .filter(NAVIGATION_PAGE + " = '" + pageName + "'")
                           .filter(TOTAL_STEPS + " = 1" );
    }
}