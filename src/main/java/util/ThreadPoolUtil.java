package util;

import java.util.concurrent.*;

public class ThreadPoolUtil {

    public static ExecutorService pool = null;

    public static ExecutorService getPool(){
        if(pool == null){
            synchronized (ThreadPoolUtil.class){
                if(pool == null){
                    pool = new ThreadPoolExecutor(
                            4,//corePoolSize
                            20,//maxPoolSize
                            300,//idle的线程过300秒后销毁
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>(Integer.MAX_VALUE),
                            new ThreadPoolExecutor.AbortPolicy()
                    );
                }
            }
        }
        return pool;
    }

}
