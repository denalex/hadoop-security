package standalone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class KerberosClient {

    private Configuration conf;
    private FileSystem fs;
    private UserGroupInformation loginUGI;

    public KerberosClient() throws IOException {
        conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://c6401.ambari.apache.org:8020");
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        /*
         this requires a keytab created with the KDC of Hadoop cluster for principal gpadmin/_HOST@<REALM>
         on KDC host (in Vagrant VM), execute as root :
         > /usr/sbin/kadmin.local
         kadmin.local: addprinc -randkey gpadmin@AMBARI.APACHE.ORG
         kadmin.local: xst -norandkey -k /tmp/pxf.service.keytab gpadmin@AMBARI.APACHE.ORG
         kadmin.local: exit
         then copy /tmp/pxf.service.keytab and Kerberos configuration to your machine:
         > cp /tmp/pxf.service.keytab /vagrant/
         > cp /etc/krb5.conf /vagrant/
         > exit
         Now in your local machine, copy the keytab file from Vagrant directory to /tmp/pxf.service.keytab
         Make the krb5.conf file available to JVM: https://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html

         The file is included in resources directory here (assuming Vagrant setup) and is passed to this program via
         -Djava.security.krb5.conf=/tmp/krb5.conf

        */
        // TODO principal should include hostname
        loginUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI("gpadmin@AMBARI.APACHE.ORG","/tmp/pxf.service.keytab");
        //UserGroupInformation.loginUserFromKeytab("gpadmin@AMBARI.APACHE.ORG","/tmp/pxf.service.keytab");
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

        final KerberosClient client = new KerberosClient();

        client.tryListDirectory("/user/adenissov");  // should fail
        Thread.sleep(500);

        client.tryListDirectory("/gpdata");          // should succeed as it is owned by gpadmin Hadoop user
        Thread.sleep(500);

        /*
        // user = user to impersonate
        UserGroupInformation ugi = UserGroupInformation.createProxyUser("adenissov", client.loginUGI);
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws IOException {
                    client.listDirectory("/user/adenissov");
                    return null;
                }
            });
        } catch (InterruptedException e) { e.printStackTrace(); }
        */

    }
}
