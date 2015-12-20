package idv.takeshi.miao.examples;

import static org.junit.Assert.*;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class WordCountTest {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  
  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.startMiniMapReduceCluster();
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
    Configuration conf = TEST_UTIL.getConfiguration();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path rootP = TEST_UTIL.getDefaultRootDirPath();
    
    Path srcP = new Path(rootP, "input_basic");
    fs.mkdirs(srcP);
    FSDataOutputStream os = null;
    InputStream is = null;
    String ifName = "input_basic.txt";
    Path inputP = new Path(srcP, ifName);
    os = fs.create(inputP);
    is = WordCountTest.class.getResourceAsStream("/" + ifName);
    
    IOUtils.copy(is, os);
    
    IOUtils.closeQuietly(is);
    IOUtils.closeQuietly(os);
    
    // 2. run test
    Path outputP = new Path(rootP, "output_basic");
    Job job = WordCount.createJob(conf, 
      new String[]{inputP.toString(), outputP.toString()});
    assertTrue(job.waitForCompletion(true));
  }
  
  

}
