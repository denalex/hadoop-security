package standalone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class ImpersonatingClient {

    private Configuration conf;
    private FileSystem fs;

    public ImpersonatingClient() {
        conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://c6401.ambari.apache.org:8020");
    }

    public String getFilePath(Path path) {
        String full = path.toString();
        return full.substring(full.indexOf('/', full.lastIndexOf(':')));
    }

    public void listDirectory(String path) throws IOException {
        System.out.println("Listing directory " + path + " :");
        fs = FileSystem.get(conf);
        FileStatus[] fsStatus = fs.listStatus(new Path(path));
        for (int i = 0; i < fsStatus.length; i++) {
            System.out.println(getFilePath(fsStatus[i].getPath()));
        }
    }

    public void tryListDirectory(String path) throws IOException {
        try {
            listDirectory(path);
        } catch (IOException e) {
            System.err.println(e.toString());
        }
    }

    public static void main(String[] args) throws Exception {

        // String parent = args.length > 0 ? args[0] : "/";
        final ImpersonatingClient client = new ImpersonatingClient();

        // now impersonate user "alex" and get the listing

        // system/service user able to proxy
        UserGroupInformation proxyUser = UserGroupInformation.getCurrentUser();

        client.tryListDirectory("/user/alex"); // should fail
        client.tryListDirectory("/user/adenissov");

        // user = user to impersonate
        UserGroupInformation ugi = UserGroupInformation.createProxyUser("alex", proxyUser);
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws IOException {
                    client.listDirectory("/user/alex");
                    return null;
                }
            });
        } catch (InterruptedException e) { e.printStackTrace(); }

    }
}