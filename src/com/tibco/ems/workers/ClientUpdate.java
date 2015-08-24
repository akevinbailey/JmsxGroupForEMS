/*
 * Copyright 2015.  TIBCO Software Inc.  ALL RIGHTS RESERVED.
 */
package com.tibco.ems.workers;

import com.sun.istack.internal.NotNull;
import com.tibco.as.space.*;
import com.tibco.as.space.browser.Browser;
import com.tibco.as.space.browser.BrowserDef;
import com.tibco.tibjms.admin.*;
import com.tibco.ems.util.DestinationParams;
import com.tibco.ems.util.ThreadParams;
import org.apache.logging.log4j.Logger;

import javax.jms.*;
import java.util.HashMap;

/**
 * Title:        ClientUpdate
 * Description:  This class listens for client connections with a JmsxGroupID and
 * adds and deletes them to the ActiveSpaces cache.
 * @author A. Kevin Bailey
 * @version 0.5
 */
@SuppressWarnings({"UnusedDeclaration"})
public final class ClientUpdate implements Runnable
{
    public final static String EMS_CONSUMER_CREATE_TOPIC = "$sys.monitor.consumer.*";
    public final static short AS_MAX_ERRORS = 1;

    private Logger logger;
    private Metaspace asMetaspace;
    private Space asClient;
    private Space asClientGroup;
    private Space asGroupSequence;
    private String emsUrl;
    private String emsAdminUser;
    private String emsPassword;
    private Connection emsConnection;
    private Session emsSession;
    private String emsClientIdProperty;
    private int timeout;
    private boolean recordMsgStats;
    private DestinationParams destParams;
    private boolean blnContinue;
    private long intNextAssign;

    private short routingThreads;
    private JmsxGroupRouter[] jmsxGroupRouter;
    private Thread[] tJmsxGroupRouter;
    private ThreadParams threadParams;

    public ClientUpdate(ThreadParams threadParams) {
        this.threadParams = threadParams;
        logger = threadParams.logger;
        asClient = threadParams.asClientSpace;
        asClientGroup = threadParams.asClientGroupSpace;
        timeout = threadParams.timeout;
        recordMsgStats = threadParams.recordMsgStats;
        emsUrl = threadParams.emsUrl;
        emsAdminUser = threadParams.emsAdminUser;
        emsPassword = threadParams.emsPassword;
        emsConnection = threadParams.emsConnection;
        emsClientIdProperty = threadParams.emsClientIdProperty;
        routingThreads = threadParams.routingThreads;
        destParams = threadParams.destinationParams;
        intNextAssign = 1;
        jmsxGroupRouter = new JmsxGroupRouter[routingThreads];
        tJmsxGroupRouter = new Thread[routingThreads];

        try {
            asMetaspace = asClient.getMetaspace();
            blnContinue = true;
        }
        catch (ASException asE) {
            logger.error(asE.getMessage(), asE);
            blnContinue = false;
        }
    }

    private void addEmsClient(@NotNull String strClientId, @NotNull String strEmsConnectionID) throws ASException {
        Tuple asTuplePut = Tuple.create();
        asTuplePut.put("ClientId", strClientId);
        asTuplePut.put("ConnectionId", strEmsConnectionID);
        asTuplePut.put("GroupCount", 0);
        asTuplePut.put("Created", DateTime.create());
        asClient.put(asTuplePut);

        startRoutingThreads();
    }

    private void startRoutingThreads() {
        for (int i = 0; i < routingThreads; i++) {
            // Only create the inDest Router if they don't exist already.
            if (tJmsxGroupRouter[i] == null) {
                threadParams.threadId = (short)(i + 1); // Thread Id's should start with 1;
                jmsxGroupRouter[i] = new JmsxGroupRouter(threadParams);
                tJmsxGroupRouter[i] = new Thread(jmsxGroupRouter[i]);
                tJmsxGroupRouter[i].start();
            }
        }
    }

    private void removeEmsClient(@NotNull String strClientId, Boolean isStartUp) throws ASException {

        // Delete the ClientId form EmsClient
        Tuple asTuplePut = Tuple.create();
        asTuplePut.put("ClientId", strClientId);
        asMetaspace.beginTransaction();
        asClient.take(asTuplePut, TakeOptions.create().setForget(true).setLockWait(timeout)); // This call can freeze if there is row locking or transactions with high volumes on the Client space!

        // Delete the ClientId from EmsClientGroup and reassign JmsxGroupIDs to other clients.
        Browser asBrowser = asClientGroup.browse(BrowserDef.BrowserType.TAKE, BrowserDef.create().setTimeScope(BrowserDef.TimeScope.SNAPSHOT).setPrefetch(BrowserDef.PREFETCH_ALL).setTimeout(timeout),
                "ClientId = '" + strClientId + "'");

        if (isStartUp == null || !isStartUp) {
            if (asBrowser.size() == 0) {  // There are no registered groups in in the ClientGroup space.
                stopJmsxGroupId();
            }
            else {
                if (asClient.size() == 0) stopJmsxGroupId(); // This is the last Client stop the routers.
                //noinspection StatementWithEmptyBody
                while ((asBrowser.next()) != null) {
                    // Do nothing.  The next will take the tuple(s) from the space.
                }
            }
        }

        asMetaspace.commitTransaction();
        reassignUnconsumedMessages(strClientId); // If the client stops abruptly there could be unconsumed messages that need to be put back in the inDest for reassignment.
    }

    /**
     * Add and removes ClientIDs from the EmsClient Space based on the
     * clients connected to EMS and Tubles in the EmsClient Space on
     * initial startup.  Since the JmsxGroupForEMS application creates
     * a Durable on the $sys.monitor.consumer.* advisory topic, it will
     * receive all the missed connection and disconnections while it is
     * it is stopped.  Therefore, this method is of little use, unless
     * initial JmsxGroupForEMS is started after the client consumers
     * connections, which is not advisable.
     *
     * @throws JMSException
     */
    @Deprecated
    private void onStartUpCheckForExistingClients() throws JMSException {
        Browser asBrowser;
        Tuple asTuplePut;
        Tuple asTupleGet;
        TibjmsAdmin emsAdmin;
        DestinationInfo destinationInfo;
        ConsumerInfo emsConsumers[];
        ConnectionInfo emsConnections[];
        HashMap<String, Long> hmClientIDs = new HashMap<>(10);

        try {
            // Get clients from AS.
            asBrowser = asClient.browse(BrowserDef.BrowserType.LOCK, BrowserDef.create().setTimeout(JmsxGroupRouter.AS_LOCK_WAIT));

            // Get destination consumers from EMS.
            emsAdmin = new TibjmsAdmin(emsUrl, emsAdminUser, emsPassword);
            if (destParams.outDestType == DestinationParams.EmsDestType.T) {
                TopicInfo topic = new TopicInfo(destParams.outDest);
                emsConsumers = emsAdmin.getConsumers(null, null, topic, false, TibjmsAdmin.GET_SELECTOR);
            }
            else if (destParams.outDestType == DestinationParams.EmsDestType.Q) {
                QueueInfo queue = new QueueInfo(destParams.outDest);
                emsConsumers = emsAdmin.getConsumers(null, null, queue, false, TibjmsAdmin.GET_SELECTOR);
            }
            else {
                throw new JMSException("ERROR", "Invalid destination type for inDestType.");
            }

            // Get ClientID for the consumers.
            emsConnections = emsAdmin.getConnections(); // This could be a several thousand records, but we don't have an alternative choice.
            for (ConsumerInfo consumerInfo : emsConsumers)
                for (ConnectionInfo connectionInfo : emsConnections)
                    if (consumerInfo.getConnectionID() == connectionInfo.getID())
                        hmClientIDs.put(connectionInfo.getClientID(), connectionInfo.getID());

            // Find Clients that are in AS, but are not connected to EMS.
            while ((asTupleGet = asBrowser.next()) != null) {
                if (hmClientIDs.containsKey(asTupleGet.getString("ClientId"))) {// ClientID is not connected to EMS
                    removeEmsClient(asTupleGet.getString("ClientId"), true); // Remove ClientID from AS
                    hmClientIDs.remove(asTupleGet.getString("ClientId"));  // Remove ClientID from map so we don't have to iterate over it again.
                }
            }

            // Find Clients that are connected to EMS, but are not in AS
            asTuplePut = Tuple.create();
            for (HashMap.Entry<String, Long> eClientId : hmClientIDs.entrySet()) {
                asTuplePut.put("ClientId", eClientId.getKey());
                if (asClient.get(asTuplePut) == null) //ClientID is not in AS
                    addEmsClient(eClientId.getKey(), eClientId.getValue().toString()); // Add ClientID to AS
            }

            // Unlock the AS Clients.
            asBrowser.reset();
            while ((asTupleGet = asBrowser.next()) != null) {
                asClient.unlock(asTupleGet);
            }
        }
        catch (ASException|TibjmsAdminException E) {
            logger.error(E.getMessage(), E);
        }
    }

    private void updateGroupCount(@NotNull String strClientId) throws ASException {
        Tuple asTuplePut;
        Tuple asTupleGet;
        Integer intCount;

        asTuplePut = Tuple.create();
        asTuplePut.put("ClientId", strClientId);
        asTupleGet = asClient.lock(asTuplePut, LockOptions.create().setLockWait(timeout));
        // Increment the group count in the EmsClient Space
        intCount = asTupleGet.getInt("GroupCount") == null ? 1 : asTupleGet.getInt("GroupCount") + 1;
        asTupleGet.put("GroupCount", intCount);
        asClient.put(asTupleGet, PutOptions.create().setUnlock(true));
    }

    private void reassignUnconsumedMessages(@NotNull String strClientId) {
        Session emsSession = null;
        MessageConsumer emsConsumer;
        MessageProducer emsProducer;
        Message emsReceiveMessage;
        Message emsSendMessage;
        String strDurableName = null;
        long intCount = 0;

        try {
            emsSession = emsConnection.createSession(true, Session.SESSION_TRANSACTED);
            // Create a consumer to get the unconsumed messages.
            if (destParams.outDestType == DestinationParams.EmsDestType.T) {
                // Need to find the durable name, pull out the messages, and remove the durable if present.
                TibjmsAdmin emsAdmin = new TibjmsAdmin(emsUrl, emsAdminUser, emsPassword);
                @SuppressWarnings("deprecation") DurableInfo durableInfos[] = emsAdmin.getDurables(destParams.outDest); // This method is deprecated, but is best way to find what we are looking for.

                for (DurableInfo durableInfo : durableInfos)
                        if (durableInfo.getClientID().equals(strClientId))
                            strDurableName = durableInfo.getDurableName();

                if (strDurableName == null) // The client is a basic Topic consumer.
                    emsConsumer = emsSession.createConsumer(emsSession.createTopic(destParams.outDest), emsClientIdProperty + "='" + strClientId + "'");
                else // The client has a durable consumer.
                    emsConsumer = emsSession.createDurableConsumer(emsSession.createTopic(destParams.outDest), strDurableName, emsClientIdProperty +
                            "='" + strClientId + "'", false);
            }
            else if (destParams.outDestType == DestinationParams.EmsDestType.Q) {
                emsConsumer = emsSession.createConsumer(emsSession.createQueue(destParams.outDest),  emsClientIdProperty + "='" + strClientId + "'");
            }
            else {
                throw new JMSException("ERROR", "Invalid destination type for inDestType.");
            }

            // Create a producer to put the unconsumed messages back in the inDest.
            if (destParams.inDestType == DestinationParams.EmsDestType.T) {
                emsProducer = emsSession.createProducer(emsSession.createTopic(destParams.inDest));
            }
            else if (destParams.inDestType == DestinationParams.EmsDestType.Q) {
                emsProducer = emsSession.createProducer(emsSession.createQueue(destParams.inDest));
            }
            else {
                throw new JMSException("ERROR", "Invalid destination type for outDestType.");
            }

            do {
                emsReceiveMessage = emsConsumer.receiveNoWait();
                if (emsReceiveMessage != null) {
                    emsSendMessage = JmsxGroupRouter.createWritableMessageProps(emsReceiveMessage);
                    emsSendMessage.setStringProperty(emsClientIdProperty, null);
                    emsProducer.send(emsSendMessage);
                    ++intCount;
                }
            } while (emsReceiveMessage != null);

            emsSession.commit();

            if (destParams.outDestType == DestinationParams.EmsDestType.T && strDurableName != null)  // Remove durable subscriber if it's a topic.
                emsSession.unsubscribe(strDurableName);

            emsSession.close();
            if (intCount > 0 ) logger.info("Reassigned " + intCount + " unconfirmed messages for " + strClientId + " on " + destParams.outDest + ".");
        }
        catch (JMSException|TibjmsAdminException jmsE) {
            if (emsSession != null) try {emsSession.rollback(); emsSession.close();} catch (JMSException e) {/* Do nothing */}
            logger.error(jmsE.getMessage(), jmsE);
        }
    }

    private void stopJmsxGroupId() {
        for (int i=0; i < routingThreads; i++)
        if (jmsxGroupRouter[i] != null) {
            try {
                jmsxGroupRouter[i].interruptReceiver();
                tJmsxGroupRouter[i].join(); // Wait for the thread to finish.
            }
            catch (JMSException|InterruptedException e) {
                logger.error(e.getMessage(), e);
            }

            if (!tJmsxGroupRouter[i].isAlive()) {
                tJmsxGroupRouter[i] = null;
                jmsxGroupRouter[i] = null;
            }
        }
    }

    public void interruptReceiver() throws JMSException {
        blnContinue = false;
        if (emsSession != null) emsSession.close();
    }

    @SuppressWarnings("ConstantConditions")
    public void run() {
        short intErrors = 0;
        Tuple asTupleGet;
        Tuple asTuplePut;
        Tuple asClientTuple;
        MessageConsumer emsConsumer;
        Message emsMessage;
        String strClientId;

        try {
            emsSession = emsConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            emsConsumer = emsSession.createDurableConsumer(emsSession.createTopic(EMS_CONSUMER_CREATE_TOPIC), emsConnection.getClientID() + "_" + destParams.outDest,
                    "target_dest_name = '" + destParams.outDest + "'", false);

            emsConnection.start();
            logger.info("Monitoring " + destParams.outDest + " for new consumers.");

            // Check AS to see if there are existing Clients.  If so, start to Routing threads.
            try {
                Browser asBrowser = asClient.browse(BrowserDef.BrowserType.GET, BrowserDef.create().setTimeout(JmsxGroupRouter.AS_LOCK_WAIT));
                if (asBrowser.size() > 0) startRoutingThreads();
            }
            catch (ASException E) {
                logger.error(E.getMessage(), E);
                blnContinue = false;
            }
            //onStartUpCheckForExistingClients();

            while (blnContinue) {
                emsMessage = emsConsumer.receive();
                strClientId = emsMessage.getStringProperty("conn_clientid");

                try {
                    if (strClientId == null) {
                        logger.error("A JMS consumer without a ClientID connected to '" + destParams.outDest + "'.");
                    }
                    else if (!strClientId.equals(emsConnection.getClientID())) {  // Do not add self as a client.
                        if (emsMessage.getStringProperty("event_action").equals("create")) { // A new client connected to the destination.
                            addEmsClient(strClientId, emsMessage.getStringProperty("conn_connid"));
                            logger.info("Added " + strClientId + " to " + asClient.getName() + ".");
                        }
                        else if (emsMessage.getStringProperty("event_action").equals("delete")) { // An existing client disconnected from the destination.
                            removeEmsClient(strClientId, false);
                            logger.info("Removed " + strClientId + " from " + asClient.getName() + " and " + asClientGroup.getName() + ".");
                        }
                    }
                    emsMessage.acknowledge();
                    intErrors = 0; // Completed successfully, reset error count.
                }
                catch (ASException|RuntimeASException asE) {
                    try {asMetaspace.rollbackTransaction();} catch(ASException e) {/* Do nothing */}
                    logger.error(asE.getMessage(), asE);
                    if (++intErrors == AS_MAX_ERRORS) blnContinue = false;
                }
            }
        }
        catch (JMSException jmsE) {
            //try {asMetaspace.rollbackTransaction();} catch(ASException e) {logger.error(e.getMessage(), e);}
            logger.error(jmsE.getMessage(), jmsE);
        }
        catch (NullPointerException E) {
            try {asMetaspace.rollbackTransaction();} catch(ASException e) {/* Do nothing. */}
            // Do nothing.
        }
        finally {
            stopJmsxGroupId();
            try {
                for (Thread tGroupId : tJmsxGroupRouter) if (tGroupId != null) tGroupId.join();
                if (emsSession != null) emsSession.close();
            }
            catch (JMSException|InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
