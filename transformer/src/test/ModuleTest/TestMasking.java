package ModuleTest;

/**
 * Created by Liu Kun on 2018/4/22.
 */

import com.alibaba.datax.transport.transformer.maskingMethods.differentialPrivacy.EpsilonDifferentialPrivacyImpl;
import com.alibaba.datax.transport.transformer.maskingMethods.cryptology.RSAEncryptionImpl;
import com.alibaba.datax.transport.transformer.maskingMethods.cryptology.AES;
import com.alibaba.datax.transport.transformer.maskingMethods.irreversibleInterference.MD5EncryptionImpl;
import com.alibaba.datax.transport.transformer.maskingMethods.cryptology.FormatPreservingEncryptionImpl;
import org.junit.Test;

import java.util.Random;

public class TestMasking {

    private String originStr = "中文test";
    private double originDouble = 1.234;

    @Test
    public void testEDP(){

        try{
            double epsilon = 100;
            EpsilonDifferentialPrivacyImpl masker = new EpsilonDifferentialPrivacyImpl();
            double result = masker.maskOne(originDouble, epsilon);
            System.out.println(result);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

    @Test
    public void testRSA(){
        RSAEncryptionImpl masker = new RSAEncryptionImpl();
        for(int i=0;i<100;i++){
            try{
                String str="zxcvbnmlkjhgfdsaqwertyuiopQWERTYUIOPASDFGHJKLZXCVBNM1234567890";
                Random random = new Random();
                StringBuffer random_str = new StringBuffer();
                for(int j=0;j<6;j++) {
                    char ch = str.charAt(random.nextInt(62));
                    random_str.append(ch);
                }
                String content = new String("123小熊跳舞321 "+ random_str );
                RSAEncryptionImpl rsatest = new RSAEncryptionImpl();
                int PaddingType = rsatest.PKCS1;
                System.out.println("RSA加密解密\n数据加密前：" + content);
                System.out.println("将原始数据转换为16进制表示的字串：" + rsatest.changeBytesToString(content.getBytes()));
                String masked = rsatest.publicEncrypt(rsatest.getPublicKey(), content);
                System.out.println("公钥加密后：" + masked);
                String decoded = rsatest.privateDecrypt(rsatest.getPrivateKey(), masked);
                System.out.println("解密后：" + decoded);
            }
            catch (Exception e){
                System.out.println(e);
            }
        }
    }

    @Test
    public void testAES(){
        try{
            String encodeRule = "666";
            AES encoder = AES.getInstance(encodeRule);
            String result = encoder.encode(originStr);
            System.out.println(result);
            AES.delInstance();
            AES decoder = AES.getInstance("666");
            System.out.println(decoder.decode(result));
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

    @Test
    public void testMD5(){
        MD5EncryptionImpl masker = new MD5EncryptionImpl();
        try{
            String result = masker.execute(originStr);
            System.out.println(result);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

    @Test
    public void testFPE(){
        try{
            FormatPreservingEncryptionImpl masker = new FormatPreservingEncryptionImpl();
            String result = masker.execute("abcdefg");
            System.out.println(result);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

}
