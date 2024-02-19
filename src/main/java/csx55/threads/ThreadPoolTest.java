package csx55.threads;

public class ThreadPoolTest {
    public static void main (String [] args) {
        ThreadPool pool = new ThreadPool(6, 10);
        for (int i = 0; i < 6; i++) {
            int taskNo = i;

            //submit task
            pool.submit(() -> {
                String msg = Thread.currentThread().getName() + " : Task " + taskNo;
                System.out.println(msg);
            });
        }
        //pool.stop();
    }
}
