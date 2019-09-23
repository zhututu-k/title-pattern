package schema;

import java.util.HashMap;
import java.util.Map;

public class Page
{
    // This map can be pushed to a table in database when dealing with large volume of data,
    // and add cache on top of it
    public static Map<String, String> pageNameIdMappings = new HashMap<>();

    public enum PageType {
        NULL,
        HOME,
        LOGIN,
        TITLE,
        ORIGINALS,
        OTHER;

        public static PageType of(String pageName, String url) {
            if(pageName == null) {
                return NULL;
            }
            if(pageName.equals("HomePage")) {
                return HOME;
            }
            if(pageName.equals("OriginalsGenre")) {
                return ORIGINALS;
            }
            if(pageName.equals("LogIn")) {
                return LOGIN;
            }
            if(url.startsWith("https://www.netflix.com/title/")) {
                return TITLE;
            }
            return OTHER;
        }
    }

    /**
     * Generate page id based on page name and url.
     *
     * Map known static pages to simple characters,
     * while map title pages using its suffix.
     *
     * @param name page name
     * @param url page url
     * @return page id
     */
    public static String getPageId(String name, String url) {
        if(name == null) {
            return null;
        }
        String id = null;
        if(name.equals("HomePage")) {
            id = "H";
        }
        if(name.equals("OriginalsGenre")) {
            id = "O";
        }
        if(name.equals("LogIn")) {
            id = "L";
        }
        if(url.startsWith("https://www.netflix.com/title/")) {
            id = url.substring(30);//after https://www.netflix.com/title/
        }
        if(id != null) {
            pageNameIdMappings.put(name, id);
            return id;
        }
        throw new UnsupportedOperationException(String.format("cannot generate pageId for pageName %s, url %s", name, url));
    }
}
