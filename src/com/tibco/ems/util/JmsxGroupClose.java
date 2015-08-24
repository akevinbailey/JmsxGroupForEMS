/*
 * Copyright 2015.  TIBCO Software Inc.  ALL RIGHTS RESERVED.
 */
package com.tibco.ems.util;

import com.tibco.as.space.ASException;
import com.tibco.as.space.Metaspace;
import com.tibco.ems.JmsxGroup;
import org.apache.logging.log4j.LogManager;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.List;

/**
 * Title:        JmsxGroupClose
 * Description:  This is class hooks into the JVM and is run before the JVM exits.
 * @author A. Kevin Bailey
 * @version 0.1
 */
@SuppressWarnings({"UnusedDeclaration"})
public class JmsxGroupClose extends Thread
{
    private List<Connection> lJmsConnections;
    private List<Metaspace> lAsMetaspaces;

    public JmsxGroupClose()
    {
        lJmsConnections = new ArrayList<>();
        lAsMetaspaces = new ArrayList<>();
    }

    public JmsxGroupClose(List<Connection> lJmsConnections, List<Metaspace> lAsMetaspaces)
    {
        this.lJmsConnections = lJmsConnections;
        this.lAsMetaspaces = lAsMetaspaces;
    }

    public void addJmsConnection(Connection jmsConnection)
    {
        lJmsConnections.add(jmsConnection);
    }


    public void setJmsConnections(List<Connection> lJmsConnections)
    {
        this.lJmsConnections = lJmsConnections;
    }

    public void addAsMetaspace(Metaspace asMetaspace)
    {
        lAsMetaspaces.add(asMetaspace);
    }


    public void setAsMetaspaces(List<Metaspace> lAsMetaspaces)
    {
        this.lAsMetaspaces = lAsMetaspaces;
    }

    public void clear() {
        lJmsConnections.clear();
        lAsMetaspaces.clear();
    }

    public void run()
    {
        if (lJmsConnections != null && !lJmsConnections.isEmpty())
        {
            for (Connection c: lJmsConnections) {
                try {
                    if (c != null) c.close();
                }
                catch (JMSException jmsE) {
                    jmsE.printStackTrace();
                }
            }
            LogManager.getLogger().info("EMS connections closed.");
        }

        if (lAsMetaspaces != null && !lAsMetaspaces.isEmpty())
        {
            for (Metaspace m: lAsMetaspaces) {
                try {
                    if (m != null) m.close();
                }
                catch (ASException asE) {
                    asE.printStackTrace();
                }
            }
            LogManager.getLogger().info("ActiveSpaces connections closed.");
        }

        System.out.println("\n " + JmsxGroup.APP_NAME + " stopped.");
        LogManager.getLogger().info(JmsxGroup.APP_NAME + " stopped.");
    }
}