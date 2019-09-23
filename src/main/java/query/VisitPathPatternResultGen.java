package query;

import dataprocess.ConcatPathUDAF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import schema.Page;

import java.util.List;
import java.util.stream.Collectors;

import static schema.VisitTableConstants.PATH;

public class VisitPathPatternResultGen extends PatternResultGen
{
    public VisitPathPatternResultGen()
    {
        super();
    }


    /**
     * Find users that have visited the given page
     *
     * Example sql for path login -> home with EXACT match:
     *  SELECT * FROM session_paths
     *  WHERE path.contains('L::H');
     *
     * Example sql for path login -> home with LOOSE match:
     *  SELECT * FROM session_paths
     *  WHERE path like ('%L%::%H%');
     *
     * @param pages name of the given page
     * @param matchType EXACT or LOOSE
     * @return result dataset
     */
    public Dataset<Row> forPath(List<String> pages, MatchType matchType) {
        Dataset<Row> pathDF = sparkSession.read().table(pathTableName);
        switch (matchType) {
            case EXACT:
                return pathDF.filter(pathDF.col(PATH).contains(getExactPagePath(pages)));
            case LOOSE:
                return pathDF.filter(pathDF.col(PATH).like(getPagePathRegex(pages)));
            default:
                    throw new UnsupportedOperationException(String.format("%s match type is not supported for visited path pattern!", matchType));

        }
    }

    private String getExactPagePath(List<String> pages)
    {
        return pages.stream().map(Page.pageNameIdMappings::get).collect(Collectors.joining(ConcatPathUDAF.concatChar));
    }

    private String getPagePathRegex(List<String> pages)
    {
        String sqlLikeChar = "%";
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < pages.size(); i++) {
            if(i == 0) {
                sb.append(sqlLikeChar).append(Page.pageNameIdMappings.get(pages.get(i)));
            } else {
                sb.append(sqlLikeChar).append(ConcatPathUDAF.concatChar).append(sqlLikeChar).append(Page.pageNameIdMappings.get(pages.get(i)));
            }
        }
        sb.append(sqlLikeChar);
        return sb.toString();
    }


    public enum MatchType {
        EXACT,
        LOOSE
    }
}