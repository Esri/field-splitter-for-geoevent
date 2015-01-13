package com.esri.geoevent.processor.fieldsplitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Observable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.Field;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.FieldExpression;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;

public class FieldSplitter extends GeoEventProcessorBase implements GeoEventProducer, EventUpdatable
{
  private static final Log                     log          = LogFactory.getLog(FieldSplitter.class);

  private Messaging                            messaging;
  private GeoEventCreator                      geoEventCreator;
  private GeoEventProducer                     geoEventProducer;
  private String                               fieldToSplit;
  private String                               fieldSplitter;

  final Object                                 lock1        = new Object();

  protected FieldSplitter(GeoEventProcessorDefinition definition) throws ComponentException
  {
    super(definition);
    log.info("Field Splitter instantiated.");
  }

  public void afterPropertiesSet()
  {
    fieldToSplit = getProperty("fieldToSplit").getValueAsString();
    fieldSplitter = getProperty("fieldSplitter").getValueAsString();
  }

  @Override
  public void setId(String id)
  {
    super.setId(id);
    geoEventProducer = messaging.createGeoEventProducer(new EventDestination(id + ":event"));

  }

  @Override
  public GeoEvent process(GeoEvent geoEvent) throws Exception
  {
    splitGeoEvent(geoEvent);

    return null;
  }

  @Override
  public List<EventDestination> getEventDestinations()
  {
    return (geoEventProducer != null) ? Arrays.asList(geoEventProducer.getEventDestination()) : new ArrayList<EventDestination>();
  }

  @Override
  public void validate() throws ValidationException
  {
    super.validate();
    List<String> errors = new ArrayList<String>();
    if (errors.size() > 0)
    {
      StringBuffer sb = new StringBuffer();
      for (String message : errors)
        sb.append(message).append("\n");
      throw new ValidationException(this.getClass().getName() + " validation failed: " + sb.toString());
    }
  }

  @Override
  public EventDestination getEventDestination()
  {
    return (geoEventProducer != null) ? geoEventProducer.getEventDestination() : null;
  }

  @Override
  public void send(GeoEvent geoEvent) throws MessagingException
  {
    if (geoEventProducer != null && geoEvent != null)
    {
      geoEventProducer.send(geoEvent);
    }
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
    geoEventCreator = messaging.createGeoEventCreator();
  }

  private void splitGeoEvent(GeoEvent sourceGeoEvent) throws MessagingException
  {
    if (geoEventCreator != null)
    {
      try
      {
        Field field = sourceGeoEvent.getField(new FieldExpression(fieldToSplit));
        if (field == null)
        {
          return;
        }
        String fieldValueToSplit = (String) field.getValue();
        if (fieldValueToSplit == null)
        {
          return;
        }
        String[] fieldValues = fieldValueToSplit.split(fieldSplitter);
        if (fieldValues == null)
        {
          return;
        }
        for (String value : fieldValues)
        {
          GeoEvent geoEventOut = geoEventCreator.create(sourceGeoEvent.getGeoEventDefinition().getGuid(), sourceGeoEvent.getAllFields());
          geoEventOut.setField(fieldToSplit, value);
          geoEventOut.setProperty(GeoEventPropertyName.TYPE, "event");
          geoEventOut.setProperty(GeoEventPropertyName.OWNER_ID, getId());
          geoEventOut.setProperty(GeoEventPropertyName.OWNER_URI, definition.getUri());
          for (Map.Entry<GeoEventPropertyName, Object> property : geoEventOut.getProperties())
          {
            if (!geoEventOut.hasProperty(property.getKey()))
             geoEventOut.setProperty(property.getKey(), property.getValue());
          }
          send(geoEventOut);          
        }
      }
      catch (FieldException e)
      {
        log.error("Failed to split GeoEvent: " + e.getMessage());
      }
    }
  }

  @Override
  public void disconnect()
  {
    if (geoEventProducer != null)
      geoEventProducer.disconnect();
  }

  @Override
  public String getStatusDetails()
  {
    return (geoEventProducer != null) ? geoEventProducer.getStatusDetails() : "";
  }

  @Override
  public void init() throws MessagingException
  {
    ;
  }

  @Override
  public boolean isConnected()
  {
    return (geoEventProducer != null) ? geoEventProducer.isConnected() : false;
  }

  @Override
  public void setup() throws MessagingException
  {
    ;
  }

  @Override
  public void update(Observable o, Object arg)
  {
    ;
  }
}
