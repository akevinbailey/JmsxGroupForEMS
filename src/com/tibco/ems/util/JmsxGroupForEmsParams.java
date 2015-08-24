/*
 * Copyright 2015.  TIBCO Software Inc.  ALL RIGHTS RESERVED.
 */
package com.tibco.ems.util;

import com.sun.istack.internal.NotNull;
import com.tibco.as.space.Member;
import com.tibco.ems.JmsxGroup;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

import java.util.List;

/**
 * Title:        ConnectionParams
 * Description:  This is the class holds attributes read from the JmsxGroupForEMS config XML file.
 * @author A. Kevin Bailey
 * @version 0.5
 */
@SuppressWarnings({"UnusedDeclaration"})
@Root(name= JmsxGroup.APP_NAME)
@Namespace(reference="http://www.tibco.com/ems/JmsxGroup")
public class JmsxGroupForEmsParams
{
    @Element
    public String clientId;
    @Element(required = false)
    public Short routingThreads;
    @Element(required = false)
    public Integer connectionTimeout;
    @Element(required = false)
    public Boolean recordMsgStats;
    @Element(required = false)
    public String asDiscoveryUrl;
    @Element(required = false)
    public String asListenUrl;
    @Element(required = false)
    public Member.DistributionRole asDistributionRole;
    @Element(required = false)
    public Integer asMaxMemSpaceCount;
    @Element(required = false)
    public String asDatastoreDir;
    @Element
    public String emsConnectionUrl;
    @Element
    public String emsUser;
    @Element(required = false)
    public String emsPassword;
    @Element
    public String emsClientIdProperty;
    @ElementList
    public List<DestinationParams> destinations;

    public JmsxGroupForEmsParams() {
        clientId = JmsxGroup.APP_NAME;
        routingThreads = 1;
        connectionTimeout = JmsxGroup.RECONNECTION_TIMEOUT;
        recordMsgStats = false;
        asDiscoveryUrl = null;
        asListenUrl = "tcp://localhost:50000";
        asDistributionRole = Member.DistributionRole.SEEDER;
        asMaxMemSpaceCount = 10000000;
        asDatastoreDir = ".";
        emsConnectionUrl = null;
        emsUser = null;
        emsPassword = null;
        emsClientIdProperty = null;
        destinations = null;
    }

    public JmsxGroupForEmsParams(@NotNull String clientId, Short routingThreads, Integer connectionTimeout, Boolean recordMsgStats,
                                 String asDiscoveryUrl, String asListenUrl, Member.DistributionRole asDistributionRole,
                                 Integer asMaxMemSpaceCount, String asDatastoreDir, @NotNull String emsConnectionUrl, @NotNull String emsUser,
                                 @NotNull String emsPassword, @NotNull String emsClientIdProperty, @NotNull List<DestinationParams> destinations) {
        this.clientId = clientId;
        this.routingThreads = routingThreads;
        this.connectionTimeout = connectionTimeout;
        this.recordMsgStats = recordMsgStats;
        this.asDiscoveryUrl = asDiscoveryUrl;
        this.asListenUrl = asListenUrl;
        this.asDistributionRole = asDistributionRole;
        this.asMaxMemSpaceCount = asMaxMemSpaceCount;
        this.asDatastoreDir = asDatastoreDir;
        this.emsConnectionUrl = emsConnectionUrl;
        this.emsUser = emsUser;
        this.emsPassword = emsPassword;
        this.emsClientIdProperty = emsClientIdProperty;
        this.destinations = destinations;
    }
}