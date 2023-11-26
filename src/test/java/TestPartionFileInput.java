import com.antgroup.geaflow.Util.PartionFileInput;
import com.google.common.io.Resources;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class TestPartionFileInput {
    static String PWD="";

    static void test(int n,int m){
        int cnt=0;
        try {
            FileWriter writer = new FileWriter(new File(PWD + "SampleFile"));
            for (int i = 0; i < m; i++) {
                writer.append(i + "|" + i + "|" + (i * 100 + i / 100D) + "|AAAAAAAAA\n");
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            for (int i = 0; i < n; i++) {
                PartionFileInput f = new PartionFileInput(PWD + "SampleFile", n, i);

                while (f.nextLine()) {
                    Long col1=f.nextLong();
                    Long col2=f.nextLong();
                    Double col3=f.nextDouble();
                    cnt++;
                    assert (col1==cnt);
                    assert (col2==cnt);
                    assert (col3.equals(cnt*100+cnt/100D));
                }

            }
            assert (cnt==m);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        Random rand=new Random();
        int m=rand.nextInt(100000);
        int n= rand.nextInt(m/10);
        System.out.println("Test : " + m +" Lines with "+n+" Threads...");
        test(n,m);
    }
}
