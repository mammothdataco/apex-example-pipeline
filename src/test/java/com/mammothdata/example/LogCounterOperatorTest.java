package com.mammothdata.apex.example;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.Map;

import com.datatorrent.lib.testbench.CollectorTestSink;

public class LogCounterOperatorTest {

  @Test
  public void testLogCounter() throws Exception {
    LogCounterOperator operator = new LogCounterOperator();
    CollectorTestSink userSink = new CollectorTestSink();
    operator.setup(null);
    operator.output.setSink(userSink);
    operator.input.process("INFO lalalala");
    operator.endWindow();

    Assert.assertEquals("Collected maps", 1, userSink.collectedTuples.size());
    Map<String,Integer> map = (Map<String,Integer>) userSink.collectedTuples.get(0);
    Integer entries =  map.get("INFO");
    Assert.assertEquals("Counted user id / day aggregation", 1, entries.intValue());
  }
}