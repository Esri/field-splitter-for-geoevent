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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
    LOG.info("Field Splitter instantiated.");
  }

  @Override
  public void afterPropertiesSet() {
    fieldToSplit = getProperty(FieldSplitterDefinition.PROPERTY_FIELD_TO_SPLIT).getValueAsString();
    fieldSplitter = getProperty(FieldSplitterDefinition.PROPERTY_FIELD_SPLITTER).getValueAsString();
    LOG.debug("Field to Split: {0}", fieldToSplit);
    LOG.debug("Field Splitter: {0}", fieldSplitter);
    executor = Executors.newFixedThreadPool(10);
    LOG.trace("Initialized Field Splitter executor service: {0}", executor);
  }

  @Override
  public void setId(String id) {
    super.setId(id);
    geoEventProducer = messaging.createGeoEventProducer(new EventDestination(id + ":event"));
    LOG.trace("Created GeoEventProducer: {0}", geoEventProducer);
  }

  @Override
  public GeoEvent process(GeoEvent geoEvent) throws Exception {
    if (executor != null) {
      if (geoEvent != null) {
        GeoEventSplitter splitter = new GeoEventSplitter(geoEvent);
        executor.execute(splitter);
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.error("Field Splitter executor is not initialized. Event will be dropped: {0}", geoEvent);
      } else {
        LOG.error(
            "Field Splitter executor is not initialized. Event will be dropped. To see dropped events, enable debug logging for this processor.");
      }
    }

    return null;
  }

  @Override
  public List<EventDestination> getEventDestinations() {
    return (geoEventProducer != null) ? Arrays.asList(geoEventProducer.getEventDestination()) : new ArrayList<>();
  }

  @Override
  public void validate() throws ValidationException {
    super.validate();
    String configuredFieldToSplit = (fieldToSplit != null)
        ? fieldToSplit
        : getProperty(FieldSplitterDefinition.PROPERTY_FIELD_TO_SPLIT).getValueAsString();
    String configuredFieldSplitter = (fieldSplitter != null)
        ? fieldSplitter
        : getProperty(FieldSplitterDefinition.PROPERTY_FIELD_SPLITTER).getValueAsString();

    List<String> errors = new ArrayList<>();
    if (configuredFieldToSplit == null || configuredFieldToSplit.trim().isEmpty()) {
      errors.add("Property \"fieldToSplit\" is required and cannot be blank.");
    }
    if (configuredFieldSplitter == null || configuredFieldSplitter.isEmpty()) {
      errors.add("Property \"fieldSplitter\" is required and cannot be blank.");
    } else {
      try {
        Pattern.compile(configuredFieldSplitter);
      } catch (PatternSyntaxException e) {
        errors.add("Property \"fieldSplitter\" is not a valid regular expression: " + e.getDescription());
      }
    }

    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (String message : errors)
        sb.append(message).append("\n");
      throw new ValidationException(this.getClass().getName() + " validation failed: " + sb.toString());
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
    // no-op - producer is initialized in setId() callback to ensure unique event destination per processor instance
  }

  @Override
  public boolean isConnected() {
    return (geoEventProducer != null) ? geoEventProducer.isConnected() : false;
  }

  @Override
  public void setup() throws MessagingException {
    // no-op - producer is initialized in setId() callback to ensure unique event destination per processor instance
  }

  @Override
  public void update(Observable o, Object arg) {
    // no-op - not maintaining any internal state based on sent events
  }

  @Override
  public void shutdown() {
    ExecutorService executorToShutdown = executor;
    executor = null;
    if (executorToShutdown != null) {
      executorToShutdown.shutdown();
      while (!executorToShutdown.isTerminated()) {
      }
      executor = null;
    }
  }

  class GeoEventSplitter implements Runnable {
    private final GeoEvent sourceGeoEvent;

    public GeoEventSplitter(GeoEvent sourceGeoEvent) {
      this.sourceGeoEvent = sourceGeoEvent;
    }

    @Override
    public void run() {
      LOG.trace("Running GeoEventSplitter for GeoEvent: {0}", sourceGeoEvent);
      if (geoEventProducer != null) {
        String[] fieldValues = null;
        try {
          GeoEvent geoEventOut = (GeoEvent) sourceGeoEvent.clone();

          Field field = sourceGeoEvent.getField(new FieldExpression(fieldToSplit));
          if (field != null) {
            String fieldValueToSplit = (String) field.getValue();
            LOG.trace("Splitting GeoEvent field {0} with value: {1}", fieldToSplit, fieldValueToSplit);
            if (fieldValueToSplit != null) {
              fieldValues = fieldValueToSplit.split(fieldSplitter);
            } else {
              LOG.warn("Configured split field \"{0}\" has a null value in incoming GeoEvent.", fieldToSplit);
            }
          } else {
            LOG.warn("Configured split field \"{0}\" was not found in incoming GeoEvent.", fieldToSplit);
          }

          if (fieldValues == null) {
            fieldValues = new String[1];
            fieldValues[0] = null;
          }

          int index = 0;
          for (String value : fieldValues) {
            LOG.trace("Creating split GeoEvent {0} for field {1} with value: {2}", ++index, fieldToSplit, value);
            geoEventOut.setField(fieldToSplit, value);
            geoEventOut.setProperty(GeoEventPropertyName.TYPE, "event");
            geoEventOut.setProperty(GeoEventPropertyName.OWNER_ID, getId());
            geoEventOut.setProperty(GeoEventPropertyName.OWNER_URI, definition.getUri());
            for (Map.Entry<GeoEventPropertyName, Object> property : geoEventOut.getProperties()) {
              if (!geoEventOut.hasProperty(property.getKey()))
                geoEventOut.setProperty(property.getKey(), property.getValue());
            }

            try {
              LOG.debug("Sending split GeoEvent {0}: {1}", index, geoEventOut);
              send(geoEventOut);
            } catch (MessagingException e) {
              LOG.error("Failed to split GeoEvent: {0}", e, geoEventOut);
            }
          }
        } catch (FieldException e) {
          LOG.error("Failed to split field \"{0}\" in GeoEvent: {1}", e, fieldToSplit, sourceGeoEvent);
        } catch (Exception e) {
          LOG.error("Failed to split GeoEvent: {0}", e, sourceGeoEvent);
        }
      }
    }
  }
}
