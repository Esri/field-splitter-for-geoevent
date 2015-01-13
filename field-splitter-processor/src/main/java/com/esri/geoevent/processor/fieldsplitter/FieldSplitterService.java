package com.esri.geoevent.processor.fieldsplitter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.processor.GeoEventProcessor;
import com.esri.ges.processor.GeoEventProcessorServiceBase;

public class FieldSplitterService extends GeoEventProcessorServiceBase
{
  private Messaging messaging;
  final private static Log LOG = LogFactory.getLog(FieldSplitterService.class);

  public FieldSplitterService()
  {
    definition = new FieldSplitterDefinition();
    LOG.info("FieldSplitterService instantiated.");
  }

  @Override
  public GeoEventProcessor create() throws ComponentException
  {
    FieldSplitter detector = new FieldSplitter(definition);
    detector.setMessaging(messaging);
    return detector;
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
  }
}