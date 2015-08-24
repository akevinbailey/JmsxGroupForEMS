/*
 * Copyright 2015.  TIBCO Software Inc.  ALL RIGHTS RESERVED.
 */
package com.tibco.ems.workers;

import com.sun.istack.internal.NotNull;
import com.tibco.ems.JmsxGroup;
import com.tibco.ems.util.ThreadParams;

import com.tibco.as.space.*;
import com.tibco.as.space.browser.Browser;
import com.tibco.as.space.browser.BrowserDef;
import com.tibco.ems.util.DestinationParams;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import javax.jms.*;
import java.io.*;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * Title:        ClientUpdate
 * Description:  This class listens for client connections with a JmsxGroupID, and
 * adds and deletes them to the ActiveSpaces cache, adds JMSXGroupC.
 * @author A. Kevin Bailey
 * @version 0.5
 */
@SuppressWarnings({"UnusedDeclaration"})
public class JmsxGroupRouter implements Runnable
{
    public final static String EMS_GROUP_ID_PROPERTY = "JMSXGroupID";
    public final static String EMS_GROUP_SEQ_PROPERTY = "JMSXGroupSeq";
    public final static short AS_MAX_ERRORS = 1;
    public final static short AS_LOCK_WAIT = 500;

    private Logger logger;
    private int timeout;
    private boolean recordMsgStats;
    private Metaspace asMetaspace;
    private Space asClient;
    private Space asClientGroup;
    private Space asClientStats;
    private Space asClientGroupStats;
    private Space asGroupSequence;
    private Space asGroupSequenceCache;
    private Space asLatestValueAlternateSpace;
    private Connection emsConnection;
    private Session emsSession;
    private String emsClientIdProperty;
    private short threadId;
    private DestinationParams destParams;
    private boolean blnContinue;
    private long intNextAssign;

    public JmsxGroupRouter(@NotNull ThreadParams threadParams) {
        logger = threadParams.logger;
        timeout = threadParams.timeout;
        recordMsgStats = threadParams.recordMsgStats;
        asClient = threadParams.asClientSpace;
        asClientGroup = threadParams.asClientGroupSpace;
        asClientStats = threadParams.asClientStatsSpace;
        asClientGroupStats = threadParams.asClientGroupStatsSpace;
        asGroupSequence = threadParams.asGroupSequenceSpace;
        asGroupSequenceCache = threadParams.asGroupSequenceCacheSpace;
        asLatestValueAlternateSpace = threadParams.asLatestValueAlternateSpace;
        emsConnection = threadParams.emsConnection;
        emsClientIdProperty = threadParams.emsClientIdProperty;
        threadId = threadParams.threadId;
        destParams = threadParams.destinationParams;
        intNextAssign = threadId;  // Set to threadId so multiple threads will not have the same start client.
        emsSession = null;

        try {
            asMetaspace = asClient.getMetaspace();
            blnContinue = true;
        }
        catch (ASException asE) {
            logger.error(asE.getMessage(), asE);
            blnContinue = false;
        }
    }

    private DateTime parseDateTime(@NotNull String strDateTime) throws ParseException {
        DateTime dateTime;
        Calendar calendar;

        calendar = javax.xml.bind.DatatypeConverter.parseDateTime(strDateTime); // Requires Java 8 or JAXB API.
        dateTime = DateTime.create(calendar);

        return dateTime;
    }

    private Tuple assignToClient() throws ASException {
        Browser asBrowser;
        Tuple asTuple = null;
        String strClientId = null;
        long intCount;

        if (asClient == null || asClientGroup == null) return null;

        asBrowser = asClient.browse(BrowserDef.BrowserType.GET, BrowserDef.create().setTimeScope(BrowserDef.TimeScope.SNAPSHOT).setPrefetch(BrowserDef.PREFETCH_ALL));
        intCount = asBrowser.size();
        if (intNextAssign > intCount) intNextAssign = 1; // Reset if greater than count
        for (int i=0; i < intNextAssign; i++) {
            asTuple = asBrowser.next();
        }
        intNextAssign++;

        return asTuple;
    }

    private void updateGroupCount(@NotNull String strClientId) throws ASException {
        Browser asBrowser;
        Tuple asTuplePut;
        Tuple asTupleGet;
        Integer intCount;

        asTuplePut = Tuple.create();
        asTuplePut.put("ClientId", strClientId);
        asTupleGet = asClient.lock(asTuplePut, LockOptions.create().setLockWait(AS_LOCK_WAIT));

        // Calculate group count in the EmsClient Space
        asBrowser = asClientGroup.browse(BrowserDef.BrowserType.GET, BrowserDef.create().setTimeScope(BrowserDef.TimeScope.SNAPSHOT).setPrefetch(BrowserDef.PREFETCH_ALL),
                                        "ClientId = '" + strClientId + "'");
        asTupleGet.put("GroupCount", asBrowser.size());
        asClient.put(asTupleGet, PutOptions.create().setUnlock(true).setLockWait(AS_LOCK_WAIT));
    }

    private void updateStats(@NotNull String strGroupId, @NotNull String strClientId) throws ASException {
        Tuple asTupleGet;
        Tuple asTuplePut;
        DateTime now = DateTime.create();

        // Update Client Group Stats
        asTuplePut = Tuple.create();
        asTuplePut.put("GroupId", strGroupId);
        asTupleGet = asClientGroupStats.lock(asTuplePut, LockOptions.create().setLockWait(AS_LOCK_WAIT));
        if (asTupleGet == null) { // Create new tuple
            asTuplePut.put("Created", DateTime.create());
            asTuplePut.put("MessageCount", 1L);
            asTuplePut.put("LastUpdate", DateTime.create());
        }
        else { // Update existing tuple
            asTuplePut = asTupleGet; // Copy the existing values
            asTuplePut.put("MessageCount", asTupleGet.getLong("MessageCount") == null ? 1L : asTupleGet.getLong("MessageCount") + 1L);
            asTuplePut.put("LastUpdate", now);
        }
        asClientGroupStats.put(asTuplePut, PutOptions.create().setUnlock(true));

        // Update Client Stats
        asTuplePut = Tuple.create();
        asTuplePut.put("ClientId", strClientId);
        asTupleGet = asClientStats.lock(asTuplePut, LockOptions.create().setLockWait(AS_LOCK_WAIT));
        if (asTupleGet == null) { // Create new tuple
            asTuplePut.put("Created", DateTime.create());
            asTuplePut.put("MessageCount", 1L);
        }
        else { // Update existing tuple
            asTuplePut = asTupleGet;  // Copy the existing values
            asTuplePut.put("MessageCount", asTupleGet.getLong("MessageCount") == null ? 1L : asTupleGet.getLong("MessageCount") + 1L);
            asTuplePut.put("LastUpdate", now);
        }

        asClientStats.put(asTuplePut, PutOptions.create().setUnlock(true));
    }

    private Tuple createSeqTuple(@NotNull String strGroupId, Long intSeqNum, @NotNull DateTime timestamp) throws ASException {
        Tuple asTuplePut = Tuple.create();

        asTuplePut.put("GroupId", strGroupId);
        asTuplePut.put("LastSeqNumber", intSeqNum);
        asTuplePut.put("LastUpdate", timestamp);
        asTuplePut.put("Created", timestamp);
        // Set sequence tuple
        asGroupSequence.put(asTuplePut, PutOptions.create().setUnlock(true).setLockWait(AS_LOCK_WAIT));
        return asTuplePut;
    }

    @NotNull
    public static byte[] convertMsgToBytes(@NotNull Message msg, @NotNull Logger logger) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput objectOutput = null;
        byte[] msgBytes = null;

        try {
            objectOutput = new ObjectOutputStream(bos);
            objectOutput.writeObject(msg);
            msgBytes = bos.toByteArray();
        }
        catch (IOException ioE) {
            logger.error(ioE.getMessage(), ioE);
        }
        finally {
            try {
                if (objectOutput != null) objectOutput.close();
                bos.close();
            }
            catch (IOException ex) {/* ignore close exception */}
        }
        return msgBytes;
    }

    @NotNull
    public static Message convertBytesToMsg(@NotNull byte[] msgBytes, @NotNull Logger logger) {
        ByteArrayInputStream bis = new ByteArrayInputStream(msgBytes);
        ObjectInput objectInput = null;
        Message msg = null;

        try {
            objectInput = new ObjectInputStream(bis);
            msg = (Message)(objectInput.readObject());
        }
        catch (IOException|ClassNotFoundException E) {
            logger.error(E.getMessage(), E);
        }
        finally {
            try {
                bis.close();
                if (objectInput != null) objectInput.close();
            }
            catch (IOException ex) {/* ignore close exception */}
        }
        return msg;
    }

    @SuppressWarnings("unchecked")
    @NotNull
    public static Message createWritableMessageProps(@NotNull Message msg) throws JMSException {
        HashMap <String, Object> properties = new HashMap <> ();
        Enumeration srcProperties = msg.getPropertyNames();

        while (srcProperties.hasMoreElements()) {
            String propertyName = (String) srcProperties.nextElement ();
            properties.put(propertyName, msg.getObjectProperty (propertyName));
        }

        msg.clearProperties();

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String propertyName = entry.getKey ();
             Object value = entry.getValue ();
             msg.setObjectProperty(propertyName, value);
        }

        return msg;
    }

    public void interruptReceiver() throws JMSException {
        blnContinue = false;
        if (emsSession != null) emsSession.close();
    }

    @SuppressWarnings("ConstantConditions")
    public void run() {
        short intErrors = 0;
        Tuple asTupleGet = null;
        Tuple asTuplePut;
        Browser asBrowser;
        MessageConsumer emsConsumer;
        MessageProducer emsProducer;
        Message emsReceiveMessage;
        Message emsSendMessage;
        String strClientId = null;
        String strGroupId = null;
        String strAlternateIdValue;
        boolean inAsTransaction = false;
        boolean blnLaoAlternateId = false;

        try {
            emsSession = emsConnection.createSession(true, Session.SESSION_TRANSACTED);
            if (destParams.inDestType == DestinationParams.EmsDestType.T) {
                if (destParams.latestValueAlternateIdProperty != null && destParams.sequenceType == DestinationParams.SeqType.LATEST_VALUE_ONLY) { // Group logic not required
                    emsConsumer = emsSession.createDurableConsumer(emsSession.createTopic(destParams.inDest), emsConnection.getClientID() + "_" + destParams.inDest);
                    blnLaoAlternateId = true;
                }
                else // Group logic required and select only messages that have a group property.
                    emsConsumer = emsSession.createDurableConsumer(emsSession.createTopic(destParams.inDest), emsConnection.getClientID() + "_" + destParams.inDest,
                            EMS_GROUP_ID_PROPERTY + " IS NOT NULL", false);
            }
            else if (destParams.inDestType == DestinationParams.EmsDestType.Q) {
                if (destParams.latestValueAlternateIdProperty != null && destParams.sequenceType == DestinationParams.SeqType.LATEST_VALUE_ONLY) { // Group logic not required
                    emsConsumer = emsSession.createConsumer(emsSession.createQueue(destParams.inDest));
                    blnLaoAlternateId = true;
                }
                else // Group logic required and select only messages that have a group property.
                    emsConsumer = emsSession.createConsumer(emsSession.createQueue(destParams.inDest), EMS_GROUP_ID_PROPERTY + " IS NOT NULL", false);
            }
            else {
                throw new JMSException("ERROR", "Invalid destination type for inDestType.");
            }

            if (destParams.outDestType == DestinationParams.EmsDestType.T) {
                emsProducer = emsSession.createProducer(emsSession.createTopic(destParams.outDest));
            }
            else if (destParams.outDestType == DestinationParams.EmsDestType.Q) {
                emsProducer = emsSession.createProducer(emsSession.createQueue(destParams.outDest));
            }
            else {
                throw new JMSException("ERROR", "Invalid destination type for outDestType.");
            }

            emsConnection.start();
            logger.info("Monitoring " + destParams.inDest + " for messages and routing them to " + destParams.outDest + " (Routing Thread " + threadId + ").");

            while (blnContinue) {
                strAlternateIdValue = null;
                inAsTransaction = false;
                emsReceiveMessage = emsConsumer.receive();
                if (emsReceiveMessage == null) break; // If the message is null the receiver was interrupted and there is not need to continue.

                if (logger.getLevel() == Level.DEBUG) logger.debug("Message received: " + emsReceiveMessage.toString());

                try {
                    if (blnLaoAlternateId) { // This process is not dependent on Group ID and only requires the latest-value-only.
                        strAlternateIdValue = emsReceiveMessage.getStringProperty(destParams.latestValueAlternateIdProperty);

                        if (strAlternateIdValue == null) { // Expected ID property but it doesn't exists.  Send the message anyway.
                            emsProducer.send(emsReceiveMessage);
                            logger.info("Received message on '" + destParams.inDest + "' that does not have the JMS Property '" + destParams.latestValueAlternateIdProperty +
                                    "'.  No processing done.  Routing message to '" + destParams.outDest + "'.\n");
                        }
                        else {
                            DateTime msgDateTime;
                            try {
                                if (destParams.latestValueDateTimeProperty == null || emsReceiveMessage.getStringProperty(destParams.latestValueDateTimeProperty) == null)
                                    msgDateTime = DateTime.create(emsReceiveMessage.getJMSTimestamp());
                                else
                                    msgDateTime = parseDateTime(emsReceiveMessage.getStringProperty(destParams.latestValueDateTimeProperty));

                                // We will update a Tuple anyway, so we start the transaction here.
                                // The transaction is a bit much, but we want to commit the JMS message very close to the same time as AS.
                                asMetaspace.beginTransaction();
                                inAsTransaction = true;

                                asTuplePut = Tuple.create();
                                asTuplePut.put("JmsOutDest", destParams.outDest);
                                asTuplePut.put("AlternateIdProperty", destParams.latestValueAlternateIdProperty);
                                asTuplePut.put("AlternateIdValue", strAlternateIdValue);
                                asTupleGet = asLatestValueAlternateSpace.lock(asTuplePut, LockOptions.create().setLockWait(AS_LOCK_WAIT));
                                if (asTupleGet == null) { // Does not exist, so create a new entry.
                                    asTuplePut.put("LastMsgDateTime", msgDateTime);
                                    asTuplePut.put("Created", DateTime.create());
                                    asLatestValueAlternateSpace.put(asTuplePut, PutOptions.create().setLockWait(AS_LOCK_WAIT));
                                    emsProducer.send(emsReceiveMessage);
                                }
                                else {  // Id exists.
                                    // WARNING:  It is assumed that messages on the same GroupID will not be produced at a rate greater than one per millisecond.
                                    if (asTupleGet.getDateTime("LastMsgDateTime").getTimeInMillis() < msgDateTime.getTimeInMillis()) {
                                        asTupleGet.put("LastMsgDateTime", msgDateTime);
                                        asLatestValueAlternateSpace.put(asTupleGet, PutOptions.create().setUnlock(true).setLockWait(AS_LOCK_WAIT));
                                        emsProducer.send(emsReceiveMessage);
                                    }
                                    else {
                                        logger.info("Message is older or the same as the previous message and will be purged:\n" + emsReceiveMessage.toString());
                                        asLatestValueAlternateSpace.unlock(asTupleGet, UnlockOptions.create().setLockWait(AS_LOCK_WAIT));
                                    }
                                }
                            }
                            catch (ParseException pE) {
                                logger.error("The JMS property '" + destParams.latestValueDateTimeProperty + "' has an invalid XML datetime: '" +
                                        emsReceiveMessage.getStringProperty(destParams.latestValueDateTimeProperty) + "'.  This message will be purged:\n" +
                                        emsReceiveMessage.toString());
                            }
                        }
                    }
                    else {  // This is a process dependent on Group ID.
                        strGroupId = emsReceiveMessage.getStringProperty(EMS_GROUP_ID_PROPERTY);

                        Tuple asClientTuple;
                        // See if GroupId exists.  Do not start a AS transaction yet, because we might not need it.
                        asTuplePut = Tuple.create();
                        asTuplePut.put("GroupId", strGroupId);
                        asTupleGet = asClientGroup.get(asTuplePut);
                        if (asTupleGet == null) {  // Does not exist, so assign it to a client.
                            asMetaspace.beginTransaction(); // Putting the transaction before the first 'if' would significantly reduce performance for non sequence routing.
                            inAsTransaction = true;
                            asTupleGet = asClientGroup.get(asTuplePut);  // Get the tuple again to make sure it is null;
                            if (asTupleGet == null) {
                                asClientTuple = assignToClient();
                                if (asClientTuple == null) throw new ASException(ASStatus.NOT_FOUND, "No clients found in '" + asClient.getName() + "' space.");
                                strClientId = asClientTuple.getString("ClientId");
                                asTuplePut = Tuple.create();
                                asTuplePut.put("GroupId", strGroupId);
                                asTuplePut.put("ClientId", strClientId);
                                asTuplePut.put("ConnectionId", asClientTuple.getString("ConnectionId"));
                                asTuplePut.put("Created", DateTime.create());
                                asClientGroup.put(asTuplePut, PutOptions.create().setLockWait(AS_LOCK_WAIT));
                                updateGroupCount(strClientId);
                                asTupleGet = asTuplePut;
                            }
                        }

                        // Assign the ClientID for later use.
                        strClientId = asTupleGet.getString("ClientId");

                        /********** JMSXGroupID Only (no sequence) **********/
                        if (destParams.sequenceType == DestinationParams.SeqType.GROUP_ONLY) {
                            emsSendMessage = createWritableMessageProps(emsReceiveMessage); // The emsReceiveMessage properties are read only.  Need to copy it to a new message.
                            emsSendMessage.setStringProperty(emsClientIdProperty, strClientId);
                            emsProducer.send(emsSendMessage);
                        }
                        /********** Latest Value Only **********/
                        else if (destParams.sequenceType == DestinationParams.SeqType.LATEST_VALUE_ONLY) {
                            DateTime msgDateTime;
                            Long intSeqNum = null;
                            long intLastMsgDateTimeMillis;

                            try {
                                if (destParams.latestValueDateTimeProperty == null || emsReceiveMessage.getStringProperty(destParams.latestValueDateTimeProperty) == null)
                                    msgDateTime = DateTime.create(emsReceiveMessage.getJMSTimestamp());
                                else
                                    msgDateTime = parseDateTime(emsReceiveMessage.getStringProperty(destParams.latestValueDateTimeProperty));

                                if (!inAsTransaction) {
                                    asMetaspace.beginTransaction();
                                    inAsTransaction = true;
                                }

                                asTuplePut = Tuple.create();
                                asTuplePut.put("GroupId", strGroupId);
                                asTupleGet = asGroupSequence.lock(asTuplePut, LockOptions.create().setLockWait(AS_LOCK_WAIT));

                                if (asTupleGet == null) { // No existing Sequence, so create one.
                                    asTupleGet = createSeqTuple(strGroupId, intSeqNum, DateTime.create(emsReceiveMessage.getJMSTimestamp()));
                                }

                                intLastMsgDateTimeMillis = asTupleGet.getDateTime("LastMsgDateTime") == null ? 0 : asTupleGet.getDateTime("LastMsgDateTime").getTimeInMillis();

                                // WARNING:  It is assumed that messages on the same GroupID will not be produced at a rate greater than one per millisecond.
                                if (intLastMsgDateTimeMillis < msgDateTime.getTimeInMillis()) {
                                    emsSendMessage = createWritableMessageProps(emsReceiveMessage); // The emsReceiveMessage properties are read only.  Need to copy it to a new message.
                                    emsSendMessage.setStringProperty(emsClientIdProperty, strClientId);
                                    emsProducer.send(emsSendMessage);
                                    asTupleGet.put("LastSeqNumber", intSeqNum);
                                    asTupleGet.put("LastMsgDateTime", msgDateTime);
                                    // Set Sequence Tuple
                                    asGroupSequence.put(asTupleGet, PutOptions.create().setUnlock(true).setLockWait(AS_LOCK_WAIT));
                                }
                                else {
                                    logger.info("Message is older or the same as the previous message and will be purged:\n" + emsReceiveMessage.toString());
                                    asGroupSequence.unlock(asTupleGet, UnlockOptions.create().setLockWait(AS_LOCK_WAIT));
                                }
                            }
                            catch (ParseException pE) {
                                logger.error("The JMS property '" + destParams.latestValueDateTimeProperty + "' has an invalid XML datetime: '" +
                                        emsReceiveMessage.getStringProperty(destParams.latestValueDateTimeProperty) + "'.  This message will be purged:\n" +
                                        emsReceiveMessage.toString());
                            }
                        }
                        /********** Preserve exact sequence with JMSXGroupID and JMSXGroupSeq **********/
                        else if (destParams.sequenceType == DestinationParams.SeqType.GROUP_EXACT_SEQUENCE) {
                            Long intSeqNum;
                            Long intLastSeqNum;

                            try { intSeqNum = emsReceiveMessage.getLongProperty(EMS_GROUP_SEQ_PROPERTY); }
                            catch (NumberFormatException nfe) { intSeqNum = null; } // JMS JMSXGroupSeq property not set

                            if (intSeqNum == null) { // If there is no JMSXGroupSeq then dump the message.
                                logger.info("Message does not have a valid JMS " + EMS_GROUP_SEQ_PROPERTY + " property and will be purged:\n" + emsReceiveMessage.toString());
                            }
                            else {
                                if (!inAsTransaction) {
                                    asMetaspace.beginTransaction();
                                    inAsTransaction = true;
                                }

                                // Get Sequence Tuple
                                asTuplePut.put("GroupId", strGroupId);
                                asTupleGet = asGroupSequence.lock(asTuplePut, LockOptions.create().setLockWait(AS_LOCK_WAIT));

                                if (asTupleGet == null) { // No existing Sequence, so create one.
                                    asTupleGet = createSeqTuple(strGroupId, null, DateTime.create(emsReceiveMessage.getJMSTimestamp()));
                                }

                                // If the field is null assign the last sequence number as the message sequence number minus one.  That way the logic will work with out needing to duplicate the code.
                                intLastSeqNum = asTupleGet.getLong("LastSeqNumber") == null ? intSeqNum - 1 : asTupleGet.getLong("LastSeqNumber");

                                if (intSeqNum == (intLastSeqNum + 1L)) { // Message is in of order.
                                    Tuple seqCacheTuple;
                                    do {
                                        emsSendMessage = createWritableMessageProps(emsReceiveMessage); // The emsReceiveMessage properties are read only.  Need to copy it to a new message.
                                        emsSendMessage.setStringProperty(emsClientIdProperty, strClientId);
                                        emsProducer.send(emsSendMessage);
                                        asTupleGet.put("LastSeqNumber", intSeqNum);
                                        asTupleGet.put("LastMsgDateTime", DateTime.create(emsReceiveMessage.getJMSTimestamp()));
                                        // Set sequence tuple
                                        asGroupSequence.put(asTupleGet); // Unlock after checking for previous messages.

                                        // Check for previous out of order messages
                                        asTuplePut = Tuple.create();
                                        asTuplePut.put("GroupId", strGroupId);
                                        asTuplePut.put("SeqNumber", intSeqNum + 1L);
                                        seqCacheTuple = asGroupSequenceCache.take(asTuplePut);
                                        if (seqCacheTuple != null) { // Send the previous in order messages
                                            emsReceiveMessage = convertBytesToMsg(seqCacheTuple.getBlob("JmsMessage"), logger);
                                            try { intSeqNum = emsReceiveMessage.getLongProperty(EMS_GROUP_SEQ_PROPERTY); }
                                            catch (NumberFormatException nfe) {
                                                seqCacheTuple = null;
                                                intSeqNum = null;
                                            } // JMS JMSXGroupSeq property not set
                                        }
                                    } while (seqCacheTuple != null);

                                    asGroupSequence.unlock(asTupleGet, UnlockOptions.create().setLockWait(AS_LOCK_WAIT));
                                }
                                else { // Message is out of order.  Store in sequence data tuple.
                                    Tuple seqCacheTuple = Tuple.create();
                                    seqCacheTuple.put("GroupId", strGroupId);
                                    seqCacheTuple.put("SeqNumber", intSeqNum);
                                    seqCacheTuple.put("JmsMessage", convertMsgToBytes(emsReceiveMessage, logger));
                                    seqCacheTuple.put("Created", DateTime.create());
                                    asGroupSequenceCache.put(seqCacheTuple);
                                }
                            }
                        }
                        else {
                            logger.error("Bad SeqType '" + destParams.sequenceType + "' for destination.  Check " + JmsxGroup.APP_NAME + " configuration file.");
                            logger.error("Message purged due to SeqType error:\n" + emsReceiveMessage.toString());
                        }
                    }

                    /********* Commit Transactions *********/
                    if (inAsTransaction) {
                        inAsTransaction = false;
                        asMetaspace.commitTransaction();
                    }
                    emsSession.commit(); // Commit will removes the message from the inDest and publishes it on the outDest.
                    /***************************************/

                    if (recordMsgStats && !blnLaoAlternateId && asTupleGet != null) { // The inserting stats can cause record locks.
                        updateStats(strGroupId, strClientId);
                    }

                    if (logger.getLevel() == Level.DEBUG) {
                        if (blnLaoAlternateId)
                            logger.debug("Routed message from '" + destParams.inDest + "' to '" + destParams.outDest + "' with " + destParams.latestValueAlternateIdProperty +
                                    "='" + strAlternateIdValue + "'.");
                        else
                            logger.debug("Routed message from '" + destParams.inDest + "' to '" + destParams.outDest + "' for " + EMS_GROUP_ID_PROPERTY +
                                    "='" + strGroupId + "' and assigned it to ClientID='" + strClientId + "'.");
                    }

                    intErrors = 0; // Completed successfully, reset error count.
                }
                catch (ASException|RuntimeASException E) {
                    logger.error(E.getMessage(), E);
                    if (inAsTransaction) try {asMetaspace.rollbackTransaction();} catch(ASException e) {logger.error(e.getMessage(), e);}
                    emsSession.rollback();
                    if (++intErrors == AS_MAX_ERRORS) {
                        logger.error("Maximum number of errors exceeded on destination '" + destParams.inDest + "'.  Shutting down router on this destination.");
                        blnContinue = false;
                    }
                }
            }
        }
        catch (JMSException|NullPointerException E) {
            logger.error(E.getMessage(), E);
            try {
                if (inAsTransaction) asMetaspace.rollbackTransaction();
                emsSession.rollback();
            }
            catch(ASException|JMSException e) {/* Do nothing */}
        }
        finally {
            try {emsSession.close();} catch (JMSException e) {logger.error(e.getMessage(), e);}
        }
    }
}
