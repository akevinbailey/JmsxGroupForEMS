/*
 * Copyright 2015.  TIBCO Software Inc.  ALL RIGHTS RESERVED.
 */
package com.tibco.ems.util;

import javax.jms.Connection;

import com.sun.istack.internal.NotNull;
import com.tibco.as.space.Space;
import org.apache.logging.log4j.Logger;

/**
 * Title:        ThreadParams
 * Description:  This is the class holds connection attributes for Logging, ActiveSpaces, and EMS.
 * @author A. Kevin Bailey
 * @version 0.5
 */
@SuppressWarnings({"UnusedDeclaration"})
public class ThreadParams
{
    public Logger logger;
    public Integer timeout;
    public Boolean recordMsgStats;
    public Space asClientSpace;
    public Space asClientGroupSpace;
    public Space asClientStatsSpace;
    public Space asClientGroupStatsSpace;
    public Space asGroupSequenceSpace;
    public Space asGroupSequenceCacheSpace;
    public Space asLatestValueAlternateSpace;
    public String emsUrl;
    public String emsAdminUser;
    public String emsPassword;
    public Connection emsConnection;
    public String emsClientIdProperty;
    public Short routingThreads;
    public Short threadId;
    public DestinationParams destinationParams;

    public ThreadParams()
    {
        logger = null;
        timeout = null;
        recordMsgStats = null;
        asClientSpace = null;
        asClientGroupSpace = null;
        asGroupSequenceSpace = null;
        asGroupSequenceCacheSpace = null;
        asLatestValueAlternateSpace = null;
        emsUrl = null;
        emsAdminUser = null;
        emsPassword = null;
        emsConnection = null;
        emsClientIdProperty = null;
        routingThreads = null;
        threadId = null;
        destinationParams = null;
    }

    public ThreadParams(@NotNull Logger logger,  Integer timeout, @NotNull Boolean recordMsgStats, @NotNull Space asClientSpace,
                        @NotNull Space asClientGroupSpace, @NotNull Space asClientStatsSpace, @NotNull Space asClientGroupStatsSpace,
                        @NotNull Space asGroupSequenceSpace, @NotNull Space asGroupSequenceCacheSpace, @NotNull Space asLatestValueAlternateSpace,
                        @NotNull String emsUrl, @NotNull String emsAdminUser, @NotNull String emsPassword, @NotNull Connection emsConnection,
                        @NotNull String emsClientIdProperty, @NotNull Short routingThreads,  Short threadId, @NotNull DestinationParams destinationParams)
    {
        this.logger = logger;
        this.timeout = timeout;
        this.recordMsgStats = recordMsgStats;
        this.asClientSpace = asClientSpace;
        this.asClientGroupSpace = asClientGroupSpace;
        this.asClientStatsSpace = asClientStatsSpace;
        this.asClientGroupStatsSpace = asClientGroupStatsSpace;
        this.asGroupSequenceSpace = asGroupSequenceSpace;
        this.asGroupSequenceCacheSpace = asGroupSequenceCacheSpace;
        this.asLatestValueAlternateSpace = asLatestValueAlternateSpace;
        this.emsAdminUser = emsAdminUser;
        this.emsPassword = emsPassword;
        this.emsUrl = emsUrl;
        this.emsConnection = emsConnection;
        this.emsClientIdProperty = emsClientIdProperty;
        this.routingThreads = routingThreads;
        this.threadId = threadId;
        this.destinationParams = destinationParams;
    }
}