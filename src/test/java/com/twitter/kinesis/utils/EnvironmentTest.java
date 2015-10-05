package com.twitter.kinesis.utils;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Environment.class})
public class EnvironmentTest {

    @Test
    public void testGetsFromPropertyWhenNoEnvVar() {
        Environment environment = new Environment();

        environment.configure();

        assertEquals("YOUR_AWS_ACCESS_KEY", environment.getCredentials().getAWSAccessKeyId());
    }

    @Test
    public void testPreferEnvVarToProperty() {
        PowerMockito.mockStatic(System.class);
        Mockito.when(System.getenv("PRODUCER_THREAD_COUNT")).thenReturn("100");

        Environment environment = new Environment();
        environment.configure();

        assertEquals(100, environment.getProducerThreadCount());

        PowerMockito.verifyStatic();
    }
}
