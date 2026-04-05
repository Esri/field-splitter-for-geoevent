package com.esri.geoevent.processor.fieldsplitter;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;

import com.esri.ges.core.Uri;
import com.esri.ges.core.geoevent.Field;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.FieldExpression;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorDefinition;

@ExtendWith(MockitoExtension.class)
class FieldSplitterTest {

  private static MockedStatic<BundleLoggerFactory> loggerFactoryMock;
  private static BundleLogger mockLog;

  private MockedConstruction<FieldExpression> fieldExprMock;
  private FieldSplitter splitter;

  @Mock private GeoEventProcessorDefinition definition;
  @Mock private GeoEventProducer producer;
  @Mock private GeoEvent sourceEvent;
  @Mock private GeoEvent clonedEvent;
  @Mock private Field mockField;

  @BeforeAll
  static void initStatic() {
    mockLog = mock(BundleLogger.class);
    loggerFactoryMock = mockStatic(BundleLoggerFactory.class);
    loggerFactoryMock.when(() -> BundleLoggerFactory.getLogger(any(Class.class)))
        .thenReturn(mockLog);
  }

  @AfterAll
  static void tearDownStatic() {
    loggerFactoryMock.close();
  }

  @BeforeEach
  void setUp() throws Exception {
    fieldExprMock = mockConstruction(FieldExpression.class);
    splitter = new FieldSplitter(definition);
    setPrivateField(splitter, "fieldToSplit", "tags");
    setPrivateField(splitter, "fieldSplitter", ",");
    setPrivateField(splitter, "geoEventProducer", producer);
    lenient().doReturn(new Uri("com.esri.test", "FieldSplitter", "1.0")).when(definition).getUri();
    clearInvocations(mockLog);
  }

  @AfterEach
  void tearDown() {
    fieldExprMock.close();
  }

  // ---- GeoEventSplitter.run() happy paths ----

  @Test
  void run_splitsDelimitedField_sendsOneEventPerValue() throws Exception {
    stubCloneAndField("a,b,c");

    newSplitterRunner().run();

    verify(producer, times(3)).send(clonedEvent);
    verify(clonedEvent).setField("tags", "a");
    verify(clonedEvent).setField("tags", "b");
    verify(clonedEvent).setField("tags", "c");
  }

  @Test
  void run_singleValue_sendsOneEvent() throws Exception {
    stubCloneAndField("hello");

    newSplitterRunner().run();

    verify(producer, times(1)).send(clonedEvent);
    verify(clonedEvent).setField("tags", "hello");
  }

  @Test
  void run_setsEventProperties() throws Exception {
    stubCloneAndField("value");

    newSplitterRunner().run();

    verify(clonedEvent).setProperty(GeoEventPropertyName.TYPE, "event");
  }

  // ---- GeoEventSplitter.run() broken paths ----

  @Test
  void run_nullProducer_returnsImmediately() throws Exception {
    setPrivateField(splitter, "geoEventProducer", null);

    newSplitterRunner().run();

    verifyNoInteractions(sourceEvent);
  }

  @Test
  void run_nullField_sendsEventWithNullValue() throws Exception {
    when(sourceEvent.clone()).thenReturn(clonedEvent);
    when(sourceEvent.getField(any(FieldExpression.class))).thenReturn(null);
    when(clonedEvent.getProperties()).thenReturn(Collections.emptySet());

    newSplitterRunner().run();

    verify(producer).send(clonedEvent);
    verify(clonedEvent).setField("tags", (String) null);
  }

  @Test
  void run_nullFieldValue_sendsEventWithNullValue() throws Exception {
    when(sourceEvent.clone()).thenReturn(clonedEvent);
    when(sourceEvent.getField(any(FieldExpression.class))).thenReturn(mockField);
    when(mockField.getValue()).thenReturn(null);
    when(clonedEvent.getProperties()).thenReturn(Collections.emptySet());

    newSplitterRunner().run();

    verify(producer).send(clonedEvent);
    verify(clonedEvent).setField("tags", (String) null);
  }

  @Test
  void run_fieldExceptionOnSetField_logsFieldError() throws Exception {
    when(sourceEvent.clone()).thenReturn(clonedEvent);
    when(sourceEvent.getField(any(FieldExpression.class))).thenReturn(mockField);
    when(mockField.getValue()).thenReturn("value");
    FieldException ex = new FieldException("test");
    doThrow(ex).when(clonedEvent).setField(anyString(), any());

    newSplitterRunner().run();

    verify(producer, never()).send(any());
    verify(mockLog).error(
        eq(FieldSplitterDefinition.LOG_SPLIT_FAILED_FIELD), same(ex), eq("tags"), same(sourceEvent));
  }

  @Test
  void run_messagingExceptionOnSend_logsAndContinues() throws Exception {
    stubCloneAndField("a,b");
    MessagingException ex = mock(MessagingException.class);
    doThrow(ex).when(producer).send(clonedEvent);

    newSplitterRunner().run();

    verify(producer, times(2)).send(clonedEvent);
    verify(mockLog, times(2)).error(
        eq(FieldSplitterDefinition.LOG_SPLIT_FAILED), same(ex), same(sourceEvent));
  }

  @Test
  void run_unexpectedException_logsSplitFailed() throws Exception {
    when(sourceEvent.clone()).thenThrow(new RuntimeException("boom"));

    newSplitterRunner().run();

    verify(mockLog).error(
        eq(FieldSplitterDefinition.LOG_SPLIT_FAILED), any(RuntimeException.class), same(sourceEvent));
  }

  // ---- FieldSplitter public method tests ----

  @Test
  void send_nullProducer_doesNotThrow() throws Exception {
    setPrivateField(splitter, "geoEventProducer", null);
    assertDoesNotThrow(() -> splitter.send(sourceEvent));
  }

  @Test
  void send_nullGeoEvent_doesNotSend() throws Exception {
    splitter.send(null);
    verify(producer, never()).send(any());
  }

  @Test
  void send_delegatesToProducer() throws Exception {
    splitter.send(sourceEvent);
    verify(producer).send(sourceEvent);
  }

  @Test
  void getEventDestinations_nullProducer_returnsEmptyList() throws Exception {
    setPrivateField(splitter, "geoEventProducer", null);
    assertTrue(splitter.getEventDestinations().isEmpty());
  }

  @Test
  void getEventDestinations_returnsProducerDestination() {
    EventDestination dest = new EventDestination("test");
    when(producer.getEventDestination()).thenReturn(dest);

    List<EventDestination> result = splitter.getEventDestinations();

    assertEquals(1, result.size());
    assertSame(dest, result.get(0));
  }

  @Test
  void getEventDestination_nullProducer_returnsNull() throws Exception {
    setPrivateField(splitter, "geoEventProducer", null);
    assertNull(splitter.getEventDestination());
  }

  @Test
  void isConnected_nullProducer_returnsFalse() throws Exception {
    setPrivateField(splitter, "geoEventProducer", null);
    assertFalse(splitter.isConnected());
  }

  @Test
  void getStatusDetails_nullProducer_returnsEmptyString() throws Exception {
    setPrivateField(splitter, "geoEventProducer", null);
    assertEquals("", splitter.getStatusDetails());
  }

  @Test
  void disconnect_nullProducer_doesNotThrow() throws Exception {
    setPrivateField(splitter, "geoEventProducer", null);
    assertDoesNotThrow(() -> splitter.disconnect());
  }

  @Test
  void process_returnsNull() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    setPrivateField(splitter, "executor", executor);
    try {
      assertNull(splitter.process(sourceEvent));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void shutdown_terminatesExecutor() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    setPrivateField(splitter, "executor", executor);

    splitter.shutdown();

    assertTrue(executor.isShutdown());
  }

  @Test
  void shutdown_nullExecutor_doesNotThrow() throws Exception {
    setPrivateField(splitter, "executor", null);
    assertDoesNotThrow(() -> splitter.shutdown());
  }

  // ---- helpers ----

  private FieldSplitter.GeoEventSplitter newSplitterRunner() {
    return splitter.new GeoEventSplitter(sourceEvent);
  }

  private void stubCloneAndField(String fieldValue) throws Exception {
    when(sourceEvent.clone()).thenReturn(clonedEvent);
    when(sourceEvent.getField(any(FieldExpression.class))).thenReturn(mockField);
    when(mockField.getValue()).thenReturn(fieldValue);
    when(clonedEvent.getProperties()).thenReturn(Collections.emptySet());
  }

  private static void setPrivateField(Object target, String fieldName, Object value) throws Exception {
    Class<?> clazz = target.getClass();
    while (clazz != null) {
      try {
        java.lang.reflect.Field f = clazz.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName + " not found in hierarchy of " + target.getClass().getName());
  }
}
