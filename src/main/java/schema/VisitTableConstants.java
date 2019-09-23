package schema;

public class VisitTableConstants
{
    public static String visitViewName = "visits";
    public static String sessionPathViewName = "session_paths";

    public static String USER_ID = "user_id";
    public static String SESSION_ID = "session_id";
    public static String NAVIGATION_PAGE = "navigation_page";
    public static String URL = "url";
    public static String TIMESTAMP = "timestamp";

    //new custom columns in visit table
    public static String PAGE_TYPE = "page_type";
    public static String PAGE_ID = "page_id";
    public static String STEP_NO = "step_num";
    public static String TOTAL_STEPS = "total_steps";
    public static String PREV_PAGE_NAME = "prev_page_name";
    public static String PREV_PAGE_URL = "prev_page_url";
    public static String PREV_PAGE_TYPE = "prev_page_type";

    //new custom columns in session path table
    public static String PATH = "path";

}
