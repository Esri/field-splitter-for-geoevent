/*
  Copyright 2017 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.​

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
*/

package com.esri.geoevent.processor.fieldsplitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.Field;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.FieldExpression;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;

// GeoEvent SDK callback uses java.util.Observable in the EventUpdatable contract.
@SuppressWarnings("deprecation")
public class FieldSplitter extends GeoEventProcessorBase implements GeoEventProducer, EventUpdatable {
  private static final BundleLogger LOG = BundleLoggerFactory.getLogger(FieldSplitter.class);

  private Messaging messaging;
  private GeoEventProducer geoEventProducer;
  private String fieldToSplit;
  private String fieldSplitter;

  private ExecutorService executor;

  protected FieldSplitter(GeoEventProcessorDefinition definition) throws ComponentException {
    super(definition);
    LOG.info(FieldSplitterDefinition.LOG_PROCESSOR_INSTANTIATED);
  }

  @Override
  public void afterPropertiesSet() {
    fieldToSplit = getProperty(FieldSplitterDefinition.PROPERTY_FIELD_TO_SPLIT).getValueAsString();
    fieldSplitter = getProperty(FieldSplitterDefinition.PROPERTY_FIELD_SPLITTER).getValueAsString();

    executor = Executors.newFixedThreadPool(10);
  }

  @Override
  public void setId(String id) {
    super.setId(id);
    geoEventProducer = messaging.createGeoEventProducer(new EventDestination(id + ":event"));
  }

  @Override
  public GeoEvent process(GeoEvent geoEvent) throws Exception {
    GeoEventSplitter splitter = new GeoEventSplitter(geoEvent);
    executor.execute(splitter);

    return null;
  }

  @Override
  public List<EventDestination> getEventDestinations() {
    return (geoEventProducer != null) ? Arrays.asList(geoEventProducer.getEventDestination()) : new ArrayList<>();
  }

  @Override
  public void validate() throws ValidationException {
    super.validate();
    List<String> errors = new ArrayList<>();
    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (String message : errors)
        sb.append(message).append("\n");
      throw new ValidationException(
          LOG.translate(FieldSplitterDefinition.LOG_VALIDATION_FAILED, this.getClass().getName(), sb.toString()));
    }
  }

  @Override
  public EventDestination getEventDestination() {
    return (geoEventProducer != null) ? geoEventProducer.getEventDestination() : null;
  }

  @Override
  public void send(GeoEvent geoEvent) throws MessagingException {
    if (geoEventProducer != null && geoEvent != null) {
      geoEventProducer.send(geoEvent);
    }
  }

  public void setMessaging(Messaging messaging) {
    this.messaging = messaging;
  }

  private void splitGeoEvent(GeoEvent sourceGeoEvent) throws MessagingException {
    GeoEvent geoEventOut = (GeoEvent) sourceGeoEvent.clone();
    try {
      Field field = sourceGeoEvent.getField(new FieldExpression(fieldToSplit));
      if (field == null) {
        return;
      }
      String fieldValueToSplit = (String) field.getValue();
      if (fieldValueToSplit == null) {
        return;
      }
      String[] fieldValues = fieldValueToSplit.split(fieldSplitter);
      if (fieldValues == null) {
        return;
      }
      for (String value : fieldValues) {
        geoEventOut.setField(fieldToSplit, value);
        geoEventOut.setProperty(GeoEventPropertyName.TYPE, "event");
        geoEventOut.setProperty(GeoEventPropertyName.OWNER_ID, getId());
        geoEventOut.setProperty(GeoEventPropertyName.OWNER_URI, definition.getUri());
        for (Map.Entry<GeoEventPropertyName, Object> property : geoEventOut.getProperties()) {
          if (!geoEventOut.hasProperty(property.getKey()))
            geoEventOut.setProperty(property.getKey(), property.getValue());
        }
        send(geoEventOut);
      }
    } catch (FieldException e) {
      LOG.error(FieldSplitterDefinition.LOG_SPLIT_FAILED, e, e.getMessage());
    }
  }

  @Override
  public void disconnect() {
    if (geoEventProducer != null)
      geoEventProducer.disconnect();
  }

  @Override
  public String getStatusDetails() {
    return (geoEventProducer != null) ? geoEventProducer.getStatusDetails() : "";
  }

  @Override
  public void init() throws MessagingException {
    ;
  }

  @Override
  public boolean isConnected() {
    return (geoEventProducer != null) ? geoEventProducer.isConnected() : false;
  }

  @Override
  public void setup() throws MessagingException {
    ;
  }

  @Override
  public void update(Observable o, Object arg) {
    ;
  }

  @Override
  public void shutdown() {
    if (executor != null) {
      executor.shutdown();
      while (!executor.isTerminated()) {
      }
      executor = null;
    }
  }

  class GeoEventSplitter implements Runnable {
    private GeoEvent sourceGeoEvent;

    public GeoEventSplitter(GeoEvent sourceGeoEvent) {
      this.sourceGeoEvent = sourceGeoEvent;
    }

    @Override
    public void run() {
      if (geoEventProducer == null)
        return;

      String[] fieldValues = null;
      try {
        GeoEvent geoEventOut = (GeoEvent) sourceGeoEvent.clone();

        Field field = sourceGeoEvent.getField(new FieldExpression(fieldToSplit));
        if (field != null) {
          String fieldValueToSplit = (String) field.getValue();
          if (fieldValueToSplit != null) {
            fieldValues = fieldValueToSplit.split(fieldSplitter);
          }
        }

        if (fieldValues == null) {
          fieldValues = new String[1];
          fieldValues[0] = null;
        }

        for (String value : fieldValues) {
          geoEventOut.setField(fieldToSplit, value);
          geoEventOut.setProperty(GeoEventPropertyName.TYPE, "event");
          geoEventOut.setProperty(GeoEventPropertyName.OWNER_ID, getId());
          geoEventOut.setProperty(GeoEventPropertyName.OWNER_URI, definition.getUri());
          for (Map.Entry<GeoEventPropertyName, Object> property : geoEventOut.getProperties()) {
            if (!geoEventOut.hasProperty(property.getKey()))
              geoEventOut.setProperty(property.getKey(), property.getValue());
          }

          try {
            geoEventProducer.send(geoEventOut);
          } catch (MessagingException e) {
            LOG.error(FieldSplitterDefinition.LOG_SPLIT_FAILED, e, e.getMessage());
          }
        }
      } catch (FieldException e) {
        LOG.error(FieldSplitterDefinition.LOG_SPLIT_FAILED, e, e.getMessage());
      }
    }
  }
}
