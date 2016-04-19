package com.mammothdata.apex.example;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;
  
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ApplicationAnnotation(name="Apex Example")
public class Application implements StreamingApplication {
  
  private final static Logger LOG = LoggerFactory.getLogger(Application.class);
  
  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("KafkaInput", new KafkaSinglePortStringInputOperator());
    kafkaInput.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());
    LogCounterOperator logCounter = dag.addOperator("LogCounterOperator", new LogCounterOperator());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());
   
    dag.addStream("LogLines", kafkaInput.outputPort, logCounter.input);
    dag.addStream("Console", logCounter.output, console.input);
  }
}
