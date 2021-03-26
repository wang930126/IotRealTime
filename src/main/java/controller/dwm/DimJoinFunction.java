package controller.dwm;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {

    public void join(T input, JSONObject obj);
    public String getKeyId(T input);

}
