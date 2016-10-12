package app;

import app.tools.GetValue;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by lw_co on 2016/10/8.
 */
public class AllInfo {
    public static void main(String[] args) throws IOException, ParseException {
        AverageInfo.main(args);
        PM25Ratio.main(args);
        GetValue.stop();
    }
}
