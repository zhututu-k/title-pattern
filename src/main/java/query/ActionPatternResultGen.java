package query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import schema.Page;

import static schema.VisitTableConstants.NAVIGATION_PAGE;
import static schema.VisitTableConstants.PREV_PAGE_TYPE;


public class ActionPatternResultGen extends PatternResultGen
{
    public ActionPatternResultGen()
    {
        super();
    }

    /**
     * Find users that have jumped to the current page from the given type
     *
     * Example sql:
     *  SELECT * FROM visits
     *  WHERE nagivation_page = 'LogIn'
     *    AND prev_page_type = 'TITLE'
     *
     * @param prevPageType type of the previous page
     * @param curPageName name of the current page
     * @return result dataset
     */
    public Dataset<Row> pageJump(Page.PageType prevPageType, String curPageName) {
        return sparkSession.read().table(visitTableName)
                           .filter(NAVIGATION_PAGE + " = '" + curPageName + "'")
                           .filter(PREV_PAGE_TYPE + " = '" + prevPageType.toString() + "'");
    }
}
