/**
 * Put your copyright and license info here.
 */
package com.mammothdata.apex.example;

import com.datatorrent.api.LocalMode;
import com.datatorrent.contrib.kafka.KafkaOperatorTestBase;
import com.datatorrent.contrib.kafka.KafkaTestProducer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Ignore;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolationException;
import java.io.File;
import java.util.List;

/**
 * Test the application in local mode.
 */
public class ApplicationTest
{
  private final KafkaOperatorTestBase kafkaLauncher = new KafkaOperatorTestBase();
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  private static final String TOPIC = "apex-example-pipeline-test";

  @Before
  public void beforeTest() throws Exception {
    kafkaLauncher.baseDir = "target/" + this.getClass().getName();
    FileUtils.deleteDirectory(new File(kafkaLauncher.baseDir));
    kafkaLauncher.startZookeeper();
    kafkaLauncher.startKafkaServer();
    kafkaLauncher.createTopic(0, TOPIC);
  }

  @After
  public void afterTest() {
    kafkaLauncher.stopKafkaServer();
    kafkaLauncher.stopZookeeper();
  }

  @Test
  public void testApplication() throws Exception {
    try {
      // Pull in sample data and shovel it into kafka
      KafkaTestProducer p = new KafkaTestProducer(TOPIC);
      List<String> lines = IOUtils.readLines(
              this.getClass().getResourceAsStream("/sample.short.json"),
              "UTF-8"
      );
      p.setMessages(lines);
      
      new Thread(p).start();

      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      conf.set("dt.operator.KafkaInput.prop.topic", TOPIC);
      conf.set("dt.operator.KafkaInput.prop.zookeeper", "localhost:" + KafkaOperatorTestBase.TEST_ZOOKEEPER_PORT[0]);
      conf.set("dt.operator.KafkaInput.prop.maxTuplesPerWindow", "1"); // consume one string per window

      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();

      LOG.info("Application run started");
      lc.run(10000);
      LOG.info("Application run finished");

      lc.shutdown();

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
