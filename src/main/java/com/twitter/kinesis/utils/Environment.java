package com.twitter.kinesis.utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TreeSet;

public class Environment implements AWSCredentialsProvider {
  private static final Logger logger = LoggerFactory.getLogger(Environment.class);
  private static Properties props;

  public void configure() {
    try {
      logger.info("loading properties from classpath");
      InputStream properties = Environment.class.getClassLoader().getResourceAsStream("config.properties");
      props = new Properties();
      props.load(properties);
      logProperties();
    } catch (IOException e) {
      logger.error("Could not load properties, streams cannot be configured");
      throw new RuntimeException("Could not load properties");
    }
  }

  public void logProperties() {
    TreeSet<String> keys = new TreeSet<String>(props.stringPropertyNames());

    for (String key : keys) {
      logger.info(key + ": " + props.get(key));
    }
  }

  public String userName() {
    return getStringProperty("gnip.user.name");
  }

  public String userPassword() {
    return getStringProperty("gnip.user.password");
  }

  public String streamLabel() {
    return getStringProperty("gnip.stream.label");
  }

  public String accountName() {
    return getStringProperty("gnip.account.name");
  }

  public String product() {
    return getStringProperty("gnip.product");
  }

  public int clientId() {
    return getIntProperty("gnip.client.id");
  }

  public String publisher() {
    return getStringProperty("gnip.publisher", "twitter");
  }

  public int getProducerThreadCount() {
    return getIntProperty("producer.thread.count", 30);
  }

  public double getRateLimit() {
    return getDoubleProperty("rate.limit", -1);
  }

  public int getReportInterval() {
    return getIntProperty("metric.report.interval.seconds", 60);
  }

  public String kinesisStreamName() {
    return getStringProperty("aws.kinesis.stream.name");
  }

  public int shardCount() {
    return getIntProperty("aws.kinesis.shard.count");
  }

  public int getMessageQueueSize() {
    return getIntProperty("message.queue.size");
  }

  public Boolean isReplay() {
    return Boolean.parseBoolean(props.getProperty("gnip.replay"));
  }

  public Date getReplayDate(String gnipDateString) {
    try {
      SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
      format.parse(props.getProperty(gnipDateString));
      return date;
    } catch (ParseException e) {
      e.printStackTrace();
      return null;
    }
  }

  public Date getReplayFromDate() {
    return this.getReplayDate("gnip.from.date");
  }


  public Date getReplayToDate() {
    return this.getReplayDate("gnip.to.date");
  }

  @Override
  public AWSCredentials getCredentials() {
    AWSCredentials credentials = new AWSCredentials() {

      @Override
      public String getAWSAccessKeyId() {
        return getStringProperty("aws.access.key");
      }

      @Override
      public String getAWSSecretKey() {
        return getStringProperty("aws.secret.key");
      }
    };
    return credentials;
  }

  @Override
  public void refresh() {
    // No-op
  }

  private String getStringProperty(String propName, String def) {
    String envVarName = propNameToEnvVar(propName);
    String prop = System.getenv(envVarName);
    if (prop == null) {
      prop = props.getProperty(propName, def);
    }

    return prop;
  }

  private String getStringProperty(String propName) {
    return getStringProperty(propName, null);
  }

  private int getIntProperty(String propName) {
    return getIntProperty(propName, null);
  }

  private Integer getIntProperty(String propName, Integer def) {
    String prop = getStringProperty(propName);
    if (prop != null) {
      return Integer.parseInt(prop);
    }

    return def;
  }

  private double getDoubleProperty(String propName, double def) {
    String prop = getStringProperty(propName);
    if (prop != null) {
      return Double.parseDouble(prop);
    }

    return def;
  }

  private static String propNameToEnvVar(String propName) {
    return propName.toUpperCase().replace('.', '_');
  }
}
