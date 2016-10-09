/**
 * Singleton design pattern is used when you only want to restrict instantiation of a class to only one object
 * across your system. An example would be Calendar class.
 *
 * This is an extended version (higher concurrency) of lazy initialisation based on wiki page:
 * https://en.wikipedia.org/wiki/Singleton_pattern
 *
 * Created by Haitao (James) Li on 09/10/2016.
 */

public final class Singleton {

    public static Singleton instance = null;

    private Singleton() {}

    public static Singleton getInstance() {
        if (instance == null) {
            synchronized (Singleton.class) {
                if (instance == null) {
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }
}