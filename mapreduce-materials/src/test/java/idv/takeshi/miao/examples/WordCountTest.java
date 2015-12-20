package idv.takeshi.miao.examples;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WordCountTest {
  private static final String WORD_COUNT_DIR = "/WordCount/";
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  
  private Configuration conf;
  private FileSystem fs;
  private Path rootP;
  
  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.startMiniMapReduceCluster();
  }
  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getTestFileSystem();
    rootP = TEST_UTIL.getDefaultRootDirPath();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniMapReduceCluster();
  }
  
  @Test
  public void testWordCount_basic() throws Exception {
    // 1. prepare src file
    Path srcP = new Path(rootP, "input_basic");
    fs.mkdirs(srcP);
    OutputStream os = null;
    InputStream is = null;
    String ifName = "input_basic.txt";
    Path inputP = new Path(srcP, ifName);
    os = fs.create(inputP);
    is = WordCountTest.class.getResourceAsStream(WORD_COUNT_DIR + ifName);
    
    IOUtils.copy(is, os);
    
    IOUtils.closeQuietly(is);
    IOUtils.closeQuietly(os);
    
    // 2. run test
    Path outputP = new Path(rootP, "output_basic");
    Job job = WordCount.createJob(conf, 
      new String[]{inputP.toString(), outputP.toString()});
    assertTrue(job.waitForCompletion(true));
    
    // 2.1 get result files for later assertions
//    FileStatus[] fStats = fs.listStatus(outputP);
//    Path tmpP = null;
//    String tmpDestP = "/tmp/mvn-hadoop-2.2.0/part-files";
//    for(FileStatus fStat : fStats) {
//      System.out.println("output:" + fStat.getPath().toString());
//      if(fStat.isFile()) {
//        tmpP = fStat.getPath();
//        is = fs.open(tmpP);
//        os = new FileOutputStream(new File(tmpDestP, tmpP.getName()));
//        IOUtils.copy(is, os);
//        IOUtils.closeQuietly(is);
//        IOUtils.closeQuietly(os);
//      }
//    }
    
    // 3. compare output file with assertion file
    FileStatus[] fStats = fs.listStatus(outputP);
    Path tmpP = null;
    String resultFN = "part-r-00000";
    InputStream is2 = null;
    boolean compared = false;
    for(FileStatus fStat : fStats) {
      System.out.println("output:" + fStat.getPath().toString());
      if(fStat.isFile()) {
        tmpP = fStat.getPath();
        if(tmpP.getName().endsWith(resultFN)) {
          is = fs.open(tmpP);
          is2 = WordCountTest.class.getResourceAsStream(
            WORD_COUNT_DIR + resultFN);
          assertTrue(IOUtils.contentEquals(is, is2));
          IOUtils.closeQuietly(is);
          IOUtils.closeQuietly(is2);
          compared = true;
          break;
        }
      }
    }
    assertTrue(compared);
  }
  
  

}
