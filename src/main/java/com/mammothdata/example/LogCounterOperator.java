package com.mammothdata.apex.example;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public class LogCounterOperator extends BaseOperator {
  
  private HashMap<String, Integer> counter;
  private final static Logger LOG = LoggerFactory.getLogger(LogCounterOperator.class);

  public transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
      @Override
      public void process(String text) {
        String type = text.substring(0, text.indexOf(' ')); 
        Integer currentCounter = counter.getOrDefault(type, 0);
        counter.put(type, currentCounter+1);
      }
    };
  
  public transient DefaultOutputPort<Map<String, Integer>> output = new DefaultOutputPort<>();
  

  @Override
  public void endWindow() {
    output.emit(counter);
  }

  @Override 
  public void setup(OperatorContext context){
    counter = new HashMap();
  }

  @Override
  public void teardown(){
    counter.clear();
  }
}