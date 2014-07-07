package org.ww.spark.example;


import java.io.File;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * : zachary
 * Date: 14-5-2
 * Time: 下午3:38
 * PC：ubuntu'IDEA in home
 */
public class JarUtil {
    /**
     * 根据指定关键字添加jar
     *
     * @param jar       项目jar包
     * @param jarsArray classpath下需要加入的jar
     * @return
     */
    public static String[] addClassPathByName(String jar, String[] jarsArray) {
        Set<String> classpath = new HashSet<String>();
        classpath.add(jar);
        Properties p = System.getProperties();
        String value = p.getProperty("java.class.path");
        String[] jars = value.split(";");
        for (int i = 0; i < jars.length; i++) {
            boolean isAdd = false;
            //不是以jar为结尾的不需要加入。
            if (!jars[i].endsWith("jar")) {
                continue;
            }

            for (String s : jarsArray) {
                if (jars[i].contains(s)) {
                    isAdd = true;
                    break;
                }
            }
            if (isAdd) {
                classpath.add(jars[i]);
            }
        }

        String[] jarArray = new String[classpath.size()];

        return classpath.toArray(jarArray);
    }

    /**
     * 根据指定关键字添加jar,yarn模式不需要项目
     *
     * @param jarsArray classpath下需要加入的jar
     * @return
     */
    public static String[] addClassPathByName(String[] jarsArray) {
        Set<String> classpath = new HashSet<String>();
        Properties p = System.getProperties();
        String value = p.getProperty("java.class.path");
        String[] jars = value.split(";");
        for (int i = 0; i < jars.length; i++) {
            boolean isAdd = false;
            //不是以jar为结尾的不需要加入。
            if (!jars[i].endsWith("jar")) {
                continue;
            }

            for (String s : jarsArray) {
                if (jars[i].contains(s)) {
                    isAdd = true;
                    break;
                }
            }
            if (isAdd) {
                classpath.add(jars[i]);
            }
        }

        String[] jarArray = new String[classpath.size()];

        return classpath.toArray(jarArray);
    }


    /**
     * 按照指定关键字排除掉某些jar
     *
     * @param jar       项目jar包
     * @param jarsArray classpath下需要加入的jar
     * @return
     */
    public static String[] addClassPathButSomeJar(String jar, String[] jarsArray) {
        Set<String> classpath = new HashSet<String>();
        classpath.add(jar);
        Properties p = System.getProperties();
        String value = p.getProperty("java.class.path");
        String[] jars = value.split(";");
        for (int i = 0; i < jars.length; i++) {
            boolean isAdd = true;
            //不是以jar为结尾的不需要加入。
            if (!jars[i].endsWith("jar")) {
                continue;
            }

            for (String s : jarsArray) {
                if (jars[i].contains(s)) {
                    isAdd = false;
                    break;
                }
            }
            if (isAdd) {
                classpath.add(jars[i]);
            }
            System.out.println(jars[i]);
        }

        String[] jarArray = new String[classpath.size()];

        return classpath.toArray(jarArray);
    }


    /**
     * 按照指定关键字排除掉某些jar，提交到yarn不需要项目jar
     *
     * @param jarsArray classpath下需要加入的jar
     * @return
     */
    public static String[] addClassPathButSomeJar(String[] jarsArray) {
        Set<String> classpath = new HashSet<String>();
        Properties p = System.getProperties();
        String value = p.getProperty("java.class.path");
        String[] jars = value.split(";");
        for (int i = 0; i < jars.length; i++) {
            boolean isAdd = true;
            //不是以jar为结尾的不需要加入。
            if (!jars[i].endsWith("jar")) {
                continue;
            }

            for (String s : jarsArray) {
                if (jars[i].contains(s)) {
                    isAdd = false;
                    break;
                }
            }
            if (isAdd) {
                classpath.add(jars[i]);
            }
        }

        String[] jarArray = new String[classpath.size()];

        return classpath.toArray(jarArray);
    }


    /**
     * 添加项目所有jar
     *
     * @param jar 项目jar包
     * @return
     */
    public static String[] addClassPathAll(String jar) {
        Set<String> classpath = new HashSet<String>();
        classpath.add(jar);
        Properties p = System.getProperties();
        String value = p.getProperty("java.class.path");
        String[] jars = value.split(";");
        for (int i = 0; i < jars.length; i++) {
            boolean isAdd = false;
            //不是以jar为结尾的不需要加入。
            if (!jars[i].endsWith("jar")) {
                continue;
            }
            classpath.add(jars[i]);
        }

        String[] jarArray = new String[classpath.size()];

        return classpath.toArray(jarArray);
    }

    
    public static String[] lisJars(String str){
//    	if
    	File file = new File(str);
        
                String[] filelist = file.list();
                
                String[] jar = new String[filelist.length];
                for (int i = 0; i < filelist.length; i++) {
                   jar[i] = str+filelist[i];
                }
                return jar;
    }
    
    public static void main(String[] args) throws InterruptedException {
//    	JarUtil.addClassPathButSomeJar(System.getenv("SPARK_YARN_APP_JAR"), new String[]{"4j",
//            "http", "spark"});
    	String[] jars = lisJars("/home/hadoop/Documents/workspace-sts-3.5.1.RELEASE/MicroGridPower/lib/");
    	for(String s : jars){
    		System.out.println(s);
    	}
    }

}