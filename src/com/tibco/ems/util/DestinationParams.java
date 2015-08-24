/*
 * Copyright 2015.  TIBCO Software Inc.  ALL RIGHTS RESERVED.
 */
package com.tibco.ems.util;

import com.sun.istack.internal.NotNull;

/**
 * Title:        DestinationParams
 * Description:  This is a child class of ThreadParams that holds attributes for destination.
 * @author A. Kevin Bailey
 * @version 0.5
 */
@SuppressWarnings({"UnusedDeclaration"})
public class DestinationParams
{
    public enum EmsDestType {Q, T}
    public enum SeqType {GROUP_ONLY, GROUP_EXACT_SEQUENCE, LATEST_VALUE_ONLY}

    public String inDest;
    public EmsDestType inDestType;
    public Boolean inUseDurableTopic;
    public String outDest;
    public EmsDestType outDestType;
    public SeqType sequenceType;
    public String latestValueAlternateIdProperty;
    public String latestValueDateTimeProperty;
    public Long maxMsgSize;

    public DestinationParams() {
        inDest = null;
        inDestType = null;
        inUseDurableTopic = null;
        outDest = null;
        outDestType = null;
        sequenceType = null;
        latestValueAlternateIdProperty = null;
        latestValueDateTimeProperty = null;
        maxMsgSize = null;
    }

    public DestinationParams(@NotNull String inDest, @NotNull EmsDestType inDestType, Boolean inUseDurableTopic, @NotNull String outDest,
                             @NotNull EmsDestType outDestType, @NotNull SeqType sequenceType, String latestValueAlternateIdProperty,
                             String latestValueDateTimeProperty, Long maxMsgSize) {
        this.inDest = inDest;
        this.inDestType = inDestType;
        this.inUseDurableTopic = inUseDurableTopic;
        this.outDest = outDest;
        this.outDestType = outDestType;
        this.sequenceType = sequenceType;
        this.latestValueAlternateIdProperty = latestValueAlternateIdProperty;
        this.latestValueDateTimeProperty = latestValueDateTimeProperty;
        this.maxMsgSize = maxMsgSize;
    }
}
