/*
 * Copyright 2015.  TIBCO Software Inc.  ALL RIGHTS RESERVED.
 */
package com.tibco.ems;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.tibco.as.space.*;
import com.tibco.ems.workers.ClientUpdate;
import com.tibco.tibjms.TibjmsConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.tibco.ems.util.*;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;

import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 * Title:        JmsxGroupForEMS
 * Description:  This is the main class for the JmsxGroupForEMS application.
 *               Must use Java 8 or higher to run.
 * @author A. Kevin Bailey
 * @version 0.6
 */
public final class JmsxGroup implements Runnable
{
    public final static String APP_NAME = "JmsxGroupForEMS";
    public final static String APP_VERSION = "0.6";
    public final static String APP_DATE = "2015-08-25";

    public final static String APP_COMPANY = "TIBCO Software Inc.";
    public final static String APP_AUTHOR = "A. Kevin Bailey";
    public final static String APP_AUTHOR_EMAIL = "abailey@tibco.com";

    public final static int  RECONNECTION_COUNT = 10;
    public final static int  RECONNECTION_DELAY = 2000;
    public final static int  RECONNECTION_TIMEOUT = 2000;
    public final static int  READ_TIMEOUT = 1000;
    public final static int  MIN_SEEDERS = 1;
    public final static String AS_METASPACE = "JmsxGroupForEMS";
    public final static String AS_CLIENT_SPACE = "EmsClient";
    public final static String AS_CLIENT_STATS_SPACE = "EmsClientStats";
    public final static String AS_GROUP_SPACE = "EmsClientGroup";
    public final static String AS_GROUP_STATS_SPACE = "EmsClientGroupStats";
    public final static String AS_SEQUENCE_SPACE = "EmsGroupSequence";
    public final static String AS_SEQUENCE_CACHE_SPACE = "EmsGroupSequenceCache";
    public final static String AS_LATEST_VALUE_ALTERNATE_SPACE = "EmsLatestValueAlternate";

    private Logger logger = LogManager.getLogger();
    private boolean blnRun = false;
    private boolean blnDeleteDatastore = false;
    private JmsxGroupForEmsParams jmsxGroupForEmsParams;
    private JmsxGroupClose shutdownThread;

    public static void main(String args[]) {
        new JmsxGroup(args);
    }

    private JmsxGroup(String args[]) {
        // Create shutdown hook to close connections and files
        jmsxGroupForEmsParams = null;
        shutdownThread = new JmsxGroupClose();
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        argParse(args);
        if (blnRun) run();
    }

    private void argParse(String args[]) {
        //Vector ssl_trusted = new Vector();

        if (args.length == 0 || args[0].charAt(0) != '-') {
            argError("No arguments");
        }

        try {
            // read arguments
            for (int i = 0; i < args.length; i++) {
                if (args[i].equalsIgnoreCase("-configFile") && args[i + 1].charAt(0) != '-') {
                    String strConfigFileName = args[i + 1];
                    Serializer serializer = new Persister();
                    File configFile = new File(strConfigFileName);

                    jmsxGroupForEmsParams = serializer.read(JmsxGroupForEmsParams.class, configFile);
                    blnRun = true;
                    i++;
                }
                else if (args[i].equalsIgnoreCase("-unsubscribe")) {
                    blnRun = false;
                    if (jmsxGroupForEmsParams != null) unsubscribe();
                }
                else if (args[i].equalsIgnoreCase("-newAsDatastore")) {
                    blnDeleteDatastore = true;
                    blnRun = true;
                }
                else if (args[i].equalsIgnoreCase("-createConfig") && args[i + 1].charAt(0) != '-') {
                    // Create an example configuration file.
                    String strConfigFileName = args[i + 1];
                    Serializer serializer = new Persister();
                    File configFile = new File(strConfigFileName);
                    List<DestinationParams> destinations = new ArrayList<>();
                    DestinationParams destinationParams1 = new DestinationParams("queue.sample.one", DestinationParams.EmsDestType.Q, null, "queue.sample.one.group",
                            DestinationParams.EmsDestType.Q, null, null, null, null);
                    DestinationParams destinationParams2 = new DestinationParams("topic.sample.two", DestinationParams.EmsDestType.T, true, "topic.sample.two.group",
                            DestinationParams.EmsDestType.T, DestinationParams.SeqType.GROUP_ONLY, null, null, 10485760L);
                    DestinationParams destinationParams3 = new DestinationParams("queue.sample.three", DestinationParams.EmsDestType.Q, true, "queue.sample.three.group",
                            DestinationParams.EmsDestType.Q, DestinationParams.SeqType.LATEST_VALUE_ONLY, "DataID", "DataDateTime", null);
                    destinations.add(destinationParams1);
                    destinations.add(destinationParams2);
                    destinations.add(destinationParams3);
                    JmsxGroupForEmsParams exampleFile = new JmsxGroupForEmsParams(APP_NAME + "01", (short)1, 2000, false, "tibpgm://50000", "tcp://0.0.0.0:7888",
                            Member.DistributionRole.SEEDER, 10000000, "/opt/tibco/as", "tcp://localhost:7222", "admin", "admin", "JMSXGroupClientID", destinations);

                    serializer.write(exampleFile, configFile);
                    System.out.println(" Created config file " + strConfigFileName);
                    break;
                }
                else if (args[i].equalsIgnoreCase("-version")) {
                    System.out.println(" Name:       " + APP_NAME);
                    System.out.println(" Version:    " + APP_VERSION);
                    System.out.println(" Build Date: " + APP_DATE);
                    System.exit(0);
                }
                else if (args[i].equalsIgnoreCase("-author")) {
                    System.out.println(" Company: " + APP_COMPANY);
                    System.out.println(" Author:  " + APP_AUTHOR);
                    System.out.println(" Email:   " + APP_AUTHOR_EMAIL);
                    System.exit(0);
                }
                else if (args[i].equalsIgnoreCase("-notes")) {
                    System.out.println(
                            "\n DESCRIPTION:" +
                            "\n     TIBCO Enterprise Message Service (EMS) does not natively support JmsxGroup functions." +
                            "\n     JmsxGroupForEMS uses TIBCO ActiveSpaces to add Message Groups (JmsxGroup) capabilities" +
                            "\n     to EMS. Basic JmsxGroup ensures that all messages for the same JMSXGroupId will be sent" +
                            "\n     to the same JMS consumer - while that consumer stays alive. As soon as the consumer" +
                            "\n     dies, another will be chosen." +
                            "\n     JmsxGroupForEMS also supports latest value only, were only messages with newer dates" +
                            "\n     than a previous messages are sent to the JmsxGroup, and it supports JMSXGroupSeq, where" +
                            "\n     the messages in a JmsxGroup are sent to the consumers in the numerical order of the" +
                            "\n     JMSXGroupSeq property." +
                            "\n NOTES:" +
                            "\n 1)  Initially, JmsxGroupForEMS should be started immediately after EMS and before any" +
                            "\n     consumers connect to EMS.  After JmsxGroupForEMS first connects to EMS, durable" +
                            "\n     subscribers are created on EMS's connection advisory notifications and will therefor" +
                            "\n     process any missed connections, disconnections or client messages." +
                            "\n 2)  JmsxGroupForEMS uses transaction for all EMS and ActiveSpaces interactions.  This" +
                            "\n     results in slightly lower performance, but will guarantee consistency and zero data " +
                            "\n     loss." +
                            "\n 3)  By default the ActiveSpaces created by JmsxGroupForEMS have shared nothing persistence." +
                            "\n     This allows, after the initial startup, consistency to be maintained even if EMS" +
                            "\n     or JmsxGroupForEMS were to go down.  Restarting either EMS and/or JmsxGroupForEMS" +
                            "\n     is all that is necessary.  No data loss should occur." +
                            "\n 4)  Assigning or reassigning a JMSXGroupID to a new consumer is done by round-robbin.  If" +
                            "\n     the 'routingThreads' configuration setting is greater than 1, then the multiple" +
                            "\n     threads could assign new or reassigned JMSXGroupIDs to the same consumer.  This is only" +
                            "\n     an initial occurrence and over time the JMSXGroupIDs will balance across the consumers." +
                            "\n 5)  JmsxGroupForEMS is optimized for the fast processing, and the Spaces (tables) schemas " +
                            "\n     are optimized to minimize record locking." +
                            "\n WARNINGS:" +
                            "\n 1)  Setting 'recordMsgStats' to 'true' can cause record locks on the EmsClientStats" +
                            "\n     and EmsClientGroupStats Spaces under very high volumes.  If this occurs, set" +
                            "\n     'recordMsgStats' to 'false'." +
                            "\n 2)  For LATEST_VALUE_ONLY sequence type, messages with the same JMSXGroupID or" +
                            "\n     'latestValueAlternateIdProperty' must have different timestamps.  The application" +
                            "\n     will treat messages with the same JMSTimestamp or 'latestValueDateTimeProperty'" +
                            "\n     as duplicates and will discard the later arriving messages with the same timestamp." +
                            "\n 3)  LATEST_VALUE_ONLY, GROUP_EXACT_SEQUENCE sequence types, can have record locks under" +
                            "\n     very high volumes.  There is not work-around for this condition." +
                            "\n 4)  The EmsClientGroup, EmsClientGroupStats, EmsGroupSequence, and EmsLatestValueAlternate" +
                            "\n     Spaces have the potential to get very large.  External monitoring should be put in" +
                            "\n     place to provided warning if a Space is getting to big.  Also, unneeded tuples" +
                            "\n     (records) should be taken (deleted)." +
                            "\n 5)  EmsGroupSequenceCache Space contains out of sequence messages for the " +
                            "\n     GROUP_EXACT_SEQUENCE sequence types.  Ideally there should not be any tuples in this" +
                            "\n     Space.  External monitoring should be put in place to remove old tuples and/or reset" +
                            "\n     the sequence number if there is a permanently missing message." +
                            "");
                    System.exit(0);
                }
                else if (args[i].equalsIgnoreCase("-license")) {
                    System.out.println(
                            "\n JmsxGroupForEMS is free and not supported by TIBCO, its employees, or affiliates." +
                            "\n Use of JmsxGroupForEMS is governed under the Apache 2.0 licence, accept for the" +
                            "\n the embedded EMS and ActiveSpaces libraries which are governed by their respective" +
                            "\n TIBCO licenses and are only usable to individuals or organizations with valid" +
                            "\n license agreements. Please see the aforementioned license agreements for further" +
                            "\n details.  By using this application you agree to all the aforementioned licensing" +
                            "\n terms and are in compliance with these license agreements.");
                    System.exit(0);
                }
                else if (args[i].equalsIgnoreCase("-help") || args[i].equalsIgnoreCase("-?")) {
                    usage(); System.exit(0);
                }
                else {
                    argError("Unrecognized argument '" + args[i] + "'");
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void argError(String strError) {
        System.err.println();
        System.err.println(" -------------------------------------------");
        System.err.println(" " + APP_NAME + " " + APP_VERSION);
        System.err.println();
        System.err.println(" " + strError + " specified (use -? for options)");
        System.err.println(" -------------------------------------------");
        System.err.println();
        System.exit(1);
    }

    private void usage() {
        System.out.println(" Usage of " + APP_NAME + ":  (The <> brackets mean a mandatory entry.  The [] brackets mean an optional entry.)");

        System.out.println();
        System.out.println(" java -jar JmsxGroupForEMS.jar [options]");
        System.out.println();
        System.out.println(" Requires -configFile        :");
        System.out.println("   -configFile <file-name>   : Start " + APP_NAME + " using the <file-name> configuration file.");
        System.out.println("   -unsubscribe              : Remove " + APP_NAME + "'s durable subscriptions for the EMS server and exit.");
        System.out.println("   -newAsDatastore           : Create a new ActiveSpace shared-nothing datastore and start " + APP_NAME + ".\n" +
                           "                             : This deletes all saved data.");
        System.out.println(" Does not require -configFile:");
        System.out.println("   -createConfig <file-name> : Create an example " + APP_NAME + " configuration file.");
        System.out.println("   -author                   : Contact information");
        System.out.println("   -version                  : Displays version information");
        System.out.println("   -notes                    : Notes, warnings, and limitations.  Please read.");
        System.out.println("   -license                  : " + APP_NAME + " usage license (by using this software you agree to the license)");
        System.out.println("   -help -?                  : Usage information");
    }


    private MemberDef getAsMemberDef() {
        MemberDef memberDef = MemberDef.create();
        memberDef.setDiscovery(jmsxGroupForEmsParams.asDiscoveryUrl);
        memberDef.setConnectTimeout(jmsxGroupForEmsParams.connectionTimeout);
        memberDef.setListen(jmsxGroupForEmsParams.asListenUrl);
        memberDef.setMemberName(jmsxGroupForEmsParams.clientId);
        memberDef.setMemberTimeout(jmsxGroupForEmsParams.connectionTimeout);
        memberDef.setTransportThreadCount(jmsxGroupForEmsParams.destinations.size());

        if (jmsxGroupForEmsParams.asDatastoreDir != null) {
            if (FileUtils.checkDirectoryExists(jmsxGroupForEmsParams.asDatastoreDir)) {
                memberDef.setDataStore(jmsxGroupForEmsParams.asDatastoreDir);
            }
            else {
                logger.error(new ASException(ASStatus.INVALID_ARG, "Cannot create data store directory: " + jmsxGroupForEmsParams.asDatastoreDir));
                System.exit(1);
            }
        }

        Tuple context = Tuple.create();
        context.put("platform", "java");
        context.put("joinTime", DateTime.create());
        memberDef.setContext(context);

        return memberDef;
    }

    private void setupSpaces(Metaspace metaspace) {
        SpaceDef sdClientSpace;
        SpaceDef sdClientStatsSpace;
        SpaceDef sdGroupSpace;
        SpaceDef sdGroupStatsSpace;
        SpaceDef sdSequenceSpace;
        SpaceDef sdSequenceCacheSpace;
        SpaceDef sdLatestValueAlternateSpace;

        // The Spaces are purposely separated in to functional units to minimize locking issues.
        try {
            // Check to see is metaspace has EmsClient
            sdClientSpace = metaspace.getSpaceDef(AS_CLIENT_SPACE);
            if (sdClientSpace == null) {
                // Create the EmsClient space if it doesn't exist.
                sdClientSpace = SpaceDef.create(AS_CLIENT_SPACE);
                sdClientSpace.setCachePolicy(SpaceDef.CachePolicy.READ_WRITE_THROUGH);
                sdClientSpace.setPersistenceType(SpaceDef.PersistenceType.SHARE_NOTHING);
                sdClientSpace.setDistributionPolicy(SpaceDef.DistributionPolicy.DISTRIBUTED);
                sdClientSpace.setPersistencePolicy(SpaceDef.PersistencePolicy.ASYNC);
                sdClientSpace.setEvictionPolicy(SpaceDef.EvictionPolicy.LRU);
                if (jmsxGroupForEmsParams.asMaxMemSpaceCount > 0) sdClientSpace.setCapacity(jmsxGroupForEmsParams.asMaxMemSpaceCount);
                sdClientSpace.setReadTimeout(READ_TIMEOUT);
                sdClientSpace.setMinSeederCount(MIN_SEEDERS);
                sdClientSpace.setLockScope(SpaceDef.LockScope.THREAD);

                sdClientSpace.putFieldDef(FieldDef.create("ClientId", FieldDef.FieldType.STRING));
                sdClientSpace.putFieldDef(FieldDef.create("ConnectionId", FieldDef.FieldType.STRING).setNullable(true));
                sdClientSpace.putFieldDef(FieldDef.create("GroupCount", FieldDef.FieldType.INTEGER).setNullable(true));
                sdClientSpace.putFieldDef(FieldDef.create("Created", FieldDef.FieldType.DATETIME).setNullable(true));
                sdClientSpace.setKey("ClientId");
                sdClientSpace.addIndexDef(IndexDef.create("idxGroupCount").setFieldNames("GroupCount"));

                metaspace.defineSpace(sdClientSpace);
            }

            // Check to see is metaspace has EmsClientStats
            sdClientStatsSpace = metaspace.getSpaceDef(AS_CLIENT_STATS_SPACE);
            if (sdClientStatsSpace == null) {
                // Create the EmsClientStats space if it doesn't exist.
                sdClientStatsSpace = SpaceDef.create(AS_CLIENT_STATS_SPACE);
                sdClientStatsSpace.setCachePolicy(SpaceDef.CachePolicy.READ_WRITE_THROUGH);
                sdClientStatsSpace.setPersistenceType(SpaceDef.PersistenceType.SHARE_NOTHING);
                sdClientStatsSpace.setDistributionPolicy(SpaceDef.DistributionPolicy.DISTRIBUTED);
                sdClientStatsSpace.setPersistencePolicy(SpaceDef.PersistencePolicy.ASYNC);
                sdClientStatsSpace.setEvictionPolicy(SpaceDef.EvictionPolicy.LRU);
                if (jmsxGroupForEmsParams.asMaxMemSpaceCount > 0) sdClientStatsSpace.setCapacity(jmsxGroupForEmsParams.asMaxMemSpaceCount);
                sdClientStatsSpace.setReadTimeout(READ_TIMEOUT);
                sdClientStatsSpace.setMinSeederCount(MIN_SEEDERS);
                sdClientStatsSpace.setLockScope(SpaceDef.LockScope.THREAD);

                sdClientStatsSpace.putFieldDef(FieldDef.create("ClientId", FieldDef.FieldType.STRING));
                sdClientStatsSpace.putFieldDef(FieldDef.create("MessageCount", FieldDef.FieldType.LONG).setNullable(true));
                sdClientStatsSpace.putFieldDef(FieldDef.create("LastUpdate", FieldDef.FieldType.DATETIME).setNullable(true));
                sdClientStatsSpace.putFieldDef(FieldDef.create("Created", FieldDef.FieldType.DATETIME).setNullable(true));
                sdClientStatsSpace.setKey("ClientId");

                metaspace.defineSpace(sdClientStatsSpace);
            }

            // Check to see is metaspace has EmsClientGroup
            sdGroupSpace = metaspace.getSpaceDef(AS_GROUP_SPACE);
            if (sdGroupSpace == null) {
                // Create the EmsClientGroup space if it doesn't exist.
                sdGroupSpace = SpaceDef.create(AS_GROUP_SPACE);
                sdGroupSpace.setCachePolicy(SpaceDef.CachePolicy.READ_WRITE_THROUGH);
                sdGroupSpace.setPersistenceType(SpaceDef.PersistenceType.SHARE_NOTHING);
                sdGroupSpace.setDistributionPolicy(SpaceDef.DistributionPolicy.DISTRIBUTED);
                sdGroupSpace.setPersistencePolicy(SpaceDef.PersistencePolicy.ASYNC);
                sdGroupSpace.setEvictionPolicy(SpaceDef.EvictionPolicy.LRU);
                if (jmsxGroupForEmsParams.asMaxMemSpaceCount > 0) sdGroupSpace.setCapacity(jmsxGroupForEmsParams.asMaxMemSpaceCount);
                sdGroupSpace.setReadTimeout(READ_TIMEOUT);
                sdGroupSpace.setMinSeederCount(MIN_SEEDERS);
                sdGroupSpace.setLockScope(SpaceDef.LockScope.THREAD);

                sdGroupSpace.putFieldDef(FieldDef.create("GroupId", FieldDef.FieldType.STRING));
                sdGroupSpace.putFieldDef(FieldDef.create("ClientId", FieldDef.FieldType.STRING).setNullable(true));
                sdGroupSpace.putFieldDef(FieldDef.create("ConnectionId", FieldDef.FieldType.STRING).setNullable(true));
                sdGroupSpace.putFieldDef(FieldDef.create("Created", FieldDef.FieldType.DATETIME).setNullable(true));
                sdGroupSpace.setKey("GroupId");
                sdGroupSpace.addIndexDef(IndexDef.create("idxClientId").setFieldNames("ClientId"));

                metaspace.defineSpace(sdGroupSpace);
            }

            // Check to see is metaspace has EmsClientGroupStats
            sdGroupStatsSpace = metaspace.getSpaceDef(AS_GROUP_STATS_SPACE);
            if (sdGroupStatsSpace == null) {
                // Create the EmsClientGroupStats space if it doesn't exist.
                sdGroupStatsSpace = SpaceDef.create(AS_GROUP_STATS_SPACE);
                sdGroupStatsSpace.setCachePolicy(SpaceDef.CachePolicy.READ_WRITE_THROUGH);
                sdGroupStatsSpace.setPersistenceType(SpaceDef.PersistenceType.SHARE_NOTHING);
                sdGroupStatsSpace.setDistributionPolicy(SpaceDef.DistributionPolicy.DISTRIBUTED);
                sdGroupStatsSpace.setPersistencePolicy(SpaceDef.PersistencePolicy.ASYNC);
                sdGroupStatsSpace.setEvictionPolicy(SpaceDef.EvictionPolicy.LRU);
                if (jmsxGroupForEmsParams.asMaxMemSpaceCount > 0) sdGroupStatsSpace.setCapacity(jmsxGroupForEmsParams.asMaxMemSpaceCount);
                sdGroupStatsSpace.setReadTimeout(READ_TIMEOUT);
                sdGroupStatsSpace.setMinSeederCount(MIN_SEEDERS);
                sdGroupStatsSpace.setLockScope(SpaceDef.LockScope.THREAD);

                sdGroupStatsSpace.putFieldDef(FieldDef.create("GroupId", FieldDef.FieldType.STRING));
                sdGroupStatsSpace.putFieldDef(FieldDef.create("MessageCount", FieldDef.FieldType.LONG).setNullable(true));
                sdGroupStatsSpace.putFieldDef(FieldDef.create("LastUpdate", FieldDef.FieldType.DATETIME).setNullable(true));
                sdGroupStatsSpace.putFieldDef(FieldDef.create("Created", FieldDef.FieldType.DATETIME).setNullable(true));
                sdGroupStatsSpace.setKey("GroupId");
                sdGroupStatsSpace.addIndexDef(IndexDef.create("idxLastUpdate").setFieldNames("LastUpdate"));

                metaspace.defineSpace(sdGroupStatsSpace);
            }

            // Check to see is metaspace has EmsGroupSequence
            sdSequenceSpace = metaspace.getSpaceDef(AS_SEQUENCE_SPACE);
            if (sdSequenceSpace == null) {
                // Create the EmsGroupSequence space if it doesn't exist.
                sdSequenceSpace = SpaceDef.create(AS_SEQUENCE_SPACE);
                sdSequenceSpace.setCachePolicy(SpaceDef.CachePolicy.READ_WRITE_THROUGH);
                sdSequenceSpace.setPersistenceType(SpaceDef.PersistenceType.SHARE_NOTHING);
                sdSequenceSpace.setDistributionPolicy(SpaceDef.DistributionPolicy.DISTRIBUTED);
                sdSequenceSpace.setPersistencePolicy(SpaceDef.PersistencePolicy.ASYNC);
                sdSequenceSpace.setEvictionPolicy(SpaceDef.EvictionPolicy.LRU);
                if (jmsxGroupForEmsParams.asMaxMemSpaceCount > 0) sdSequenceSpace.setCapacity(jmsxGroupForEmsParams.asMaxMemSpaceCount);
                sdSequenceSpace.setReadTimeout(READ_TIMEOUT);
                sdSequenceSpace.setMinSeederCount(MIN_SEEDERS);
                sdSequenceSpace.setLockScope(SpaceDef.LockScope.THREAD);

                sdSequenceSpace.putFieldDef(FieldDef.create("GroupId", FieldDef.FieldType.STRING));
                sdSequenceSpace.putFieldDef(FieldDef.create("LastSeqNumber", FieldDef.FieldType.LONG).setNullable(true));
                sdSequenceSpace.putFieldDef(FieldDef.create("LastMsgDateTime", FieldDef.FieldType.DATETIME).setNullable(true));
                sdSequenceSpace.putFieldDef(FieldDef.create("Created", FieldDef.FieldType.DATETIME).setNullable(true));
                sdSequenceSpace.setKey("GroupId");
                sdSequenceSpace.addIndexDef(IndexDef.create("idxLastMsgDateTime").setFieldNames("LastMsgDateTime"));

                metaspace.defineSpace(sdSequenceSpace);
            }

            // Check to see is metaspace has EmsGroupSequenceCache
            sdSequenceCacheSpace = metaspace.getSpaceDef(AS_SEQUENCE_CACHE_SPACE);
            if (sdSequenceCacheSpace == null) {
                // Create the EmsGroupSequence space if it doesn't exist.
                sdSequenceCacheSpace = SpaceDef.create(AS_SEQUENCE_CACHE_SPACE);
                sdSequenceCacheSpace.setCachePolicy(SpaceDef.CachePolicy.READ_WRITE_THROUGH);
                sdSequenceCacheSpace.setPersistenceType(SpaceDef.PersistenceType.SHARE_NOTHING);
                sdSequenceCacheSpace.setDistributionPolicy(SpaceDef.DistributionPolicy.DISTRIBUTED);
                sdSequenceCacheSpace.setPersistencePolicy(SpaceDef.PersistencePolicy.ASYNC);
                sdSequenceCacheSpace.setEvictionPolicy(SpaceDef.EvictionPolicy.LRU);
                if (jmsxGroupForEmsParams.asMaxMemSpaceCount > 0) sdSequenceCacheSpace.setCapacity(jmsxGroupForEmsParams.asMaxMemSpaceCount);
                sdSequenceCacheSpace.setReadTimeout(READ_TIMEOUT);
                sdSequenceCacheSpace.setMinSeederCount(MIN_SEEDERS);
                sdSequenceCacheSpace.setLockScope(SpaceDef.LockScope.THREAD);

                sdSequenceCacheSpace.putFieldDef(FieldDef.create("GroupId", FieldDef.FieldType.STRING));
                sdSequenceCacheSpace.putFieldDef(FieldDef.create("SeqNumber", FieldDef.FieldType.LONG));
                sdSequenceCacheSpace.putFieldDef(FieldDef.create("JmsMessage", FieldDef.FieldType.BLOB).setNullable(true));
                sdSequenceCacheSpace.putFieldDef(FieldDef.create("Created", FieldDef.FieldType.DATETIME).setNullable(true));
                sdSequenceCacheSpace.setKey("GroupId", "SeqNumber");
                sdSequenceCacheSpace.addIndexDef(IndexDef.create("idxGroupId").setFieldNames("GroupId"));

                metaspace.defineSpace(sdSequenceCacheSpace);
            }

            // Check to see is metaspace has EmsLatestValueAlternate
            sdLatestValueAlternateSpace = metaspace.getSpaceDef(AS_LATEST_VALUE_ALTERNATE_SPACE);
            if (sdLatestValueAlternateSpace == null) {
                // Create the EmsLatestValueAlternate space if it doesn't exist.
                sdLatestValueAlternateSpace = SpaceDef.create(AS_LATEST_VALUE_ALTERNATE_SPACE);
                sdLatestValueAlternateSpace.setCachePolicy(SpaceDef.CachePolicy.READ_WRITE_THROUGH);
                sdLatestValueAlternateSpace.setPersistenceType(SpaceDef.PersistenceType.SHARE_NOTHING);
                sdLatestValueAlternateSpace.setDistributionPolicy(SpaceDef.DistributionPolicy.DISTRIBUTED);
                sdLatestValueAlternateSpace.setPersistencePolicy(SpaceDef.PersistencePolicy.ASYNC);
                sdLatestValueAlternateSpace.setEvictionPolicy(SpaceDef.EvictionPolicy.LRU);
                if (jmsxGroupForEmsParams.asMaxMemSpaceCount > 0) sdLatestValueAlternateSpace.setCapacity(jmsxGroupForEmsParams.asMaxMemSpaceCount);
                sdLatestValueAlternateSpace.setReadTimeout(READ_TIMEOUT);
                sdLatestValueAlternateSpace.setMinSeederCount(MIN_SEEDERS);
                sdLatestValueAlternateSpace.setLockScope(SpaceDef.LockScope.THREAD);

                sdLatestValueAlternateSpace.putFieldDef(FieldDef.create("JmsOutDest", FieldDef.FieldType.STRING));
                sdLatestValueAlternateSpace.putFieldDef(FieldDef.create("AlternateIdProperty", FieldDef.FieldType.STRING));
                sdLatestValueAlternateSpace.putFieldDef(FieldDef.create("AlternateIdValue", FieldDef.FieldType.STRING).setNullable(true));
                sdLatestValueAlternateSpace.putFieldDef(FieldDef.create("LastMsgDateTime", FieldDef.FieldType.DATETIME).setNullable(true));
                sdLatestValueAlternateSpace.putFieldDef(FieldDef.create("Created", FieldDef.FieldType.DATETIME).setNullable(true));
                sdLatestValueAlternateSpace.setKey("JmsOutDest", "AlternateIdProperty", "AlternateIdValue");
                sdLatestValueAlternateSpace.addIndexDef(IndexDef.create("idxLastMsgDateTime").setFieldNames("LastMsgDateTime"));

                metaspace.defineSpace(sdLatestValueAlternateSpace);
            }
        }
        catch (ASException|NullPointerException e) {
            logger.error(e.getMessage(), e);
            System.exit(0);
        }
    }

    private void removeAsDatastore() {
        String strDir = jmsxGroupForEmsParams.asDatastoreDir + System.getProperty("file.separator") + AS_METASPACE;
        File directory = new File(strDir);

        //make sure directory exists
        if (directory.exists()) {
            try{
                FileUtils.deleteFile(directory);
                logger.info("Deleted old AS Datastore directory '" + strDir + "'.");
            }
            catch(IOException e) {
                logger.error(e.getMessage(), e);
                e.printStackTrace();
                System.exit(0);
            }
        }
        else {
            logger.info("Attempted to delete old AS datastore directory '" + strDir +
            "' but it does not exist.");
        }
    }

    private void unsubscribe() {
        TibjmsConnectionFactory emsFactory;
        Connection emsConnection = null;
        Session emsSession;
        String strSubscriptionName = null;

        emsFactory = new TibjmsConnectionFactory(jmsxGroupForEmsParams.emsConnectionUrl, jmsxGroupForEmsParams.clientId);
        emsFactory.setReconnAttemptCount(RECONNECTION_COUNT);
        emsFactory.setReconnAttemptDelay(RECONNECTION_DELAY);
        emsFactory.setReconnAttemptTimeout(jmsxGroupForEmsParams.connectionTimeout);

        try {
            emsConnection = emsFactory.createConnection(jmsxGroupForEmsParams.emsUser, jmsxGroupForEmsParams.emsPassword);
            emsSession = emsConnection.createSession();

            System.out.println(" " + APP_NAME + " started.\n");

            for (DestinationParams dp : jmsxGroupForEmsParams.destinations) {
                try {
                    strSubscriptionName = emsConnection.getClientID() + "_" + dp.outDest;
                    emsSession.unsubscribe(strSubscriptionName);
                    System.out.println(" Successfully unsubscribed " + strSubscriptionName);
                    // Unsubscribe any inDest topics also.
                    if (dp.inDestType == DestinationParams.EmsDestType.T) {
                        strSubscriptionName = emsConnection.getClientID() + "_" + dp.inDest;
                        emsSession.unsubscribe(strSubscriptionName);
                        System.out.println(" Successfully unsubscribed " + strSubscriptionName);
                    }
                }
                catch (InvalidDestinationException idE) {
                    System.out.println(" Subscription '" + strSubscriptionName + "' does not exist.");
                }
            }
        }
        catch (JMSException jmsE) {
            jmsE.printStackTrace();
        }
        finally {
            if (emsConnection != null) try {emsConnection.close();} catch (JMSException e) {e.printStackTrace();}
        }

        System.exit(0);  // No need to continue.
    }

    public void run() {
        logger.info("------------ " + APP_NAME + " starting... ------------");
        if (blnDeleteDatastore) removeAsDatastore();
        TibjmsConnectionFactory emsFactory;
        Connection emsConnection;
        int intDestinationCount = jmsxGroupForEmsParams.destinations.size();
        Thread[] tClientUpdate = new Thread[intDestinationCount];

        try {
            // Create or join Metaspace
            Metaspace asMetaspace = Metaspace.connect(AS_METASPACE, getAsMemberDef());

            shutdownThread.addAsMetaspace(asMetaspace);
            setupSpaces(asMetaspace);
            // Get spaces
            Space asClient = asMetaspace.getSpace(AS_CLIENT_SPACE, jmsxGroupForEmsParams.asDistributionRole);
            Space asClientGroup = asMetaspace.getSpace(AS_GROUP_SPACE, jmsxGroupForEmsParams.asDistributionRole);
            Space asClientStats = asMetaspace.getSpace(AS_CLIENT_STATS_SPACE, jmsxGroupForEmsParams.asDistributionRole);
            Space asClientGroupStats = asMetaspace.getSpace(AS_GROUP_STATS_SPACE, jmsxGroupForEmsParams.asDistributionRole);
            Space asGroupSequence = asMetaspace.getSpace(AS_SEQUENCE_SPACE, jmsxGroupForEmsParams.asDistributionRole);
            Space asGroupSequenceData = asMetaspace.getSpace(AS_SEQUENCE_CACHE_SPACE, jmsxGroupForEmsParams.asDistributionRole);
            Space asLatestValueAlternateSpace = asMetaspace.getSpace(AS_LATEST_VALUE_ALTERNATE_SPACE, jmsxGroupForEmsParams.asDistributionRole);

            asMetaspace.recover(RecoveryOptions.create().setRecoveryPolicy(RecoveryOptions.RecoveryPolicy.NO_DATA_LOSS));

            asClient.waitForReady();
            asClientGroup.waitForReady();
            asGroupSequence.waitForReady();

            logger.info("ActiveSpaces: " + asMetaspace.getMemberDef().toString());

            // Create an EMS connection.
            emsFactory = new TibjmsConnectionFactory(jmsxGroupForEmsParams.emsConnectionUrl, jmsxGroupForEmsParams.clientId);
            emsFactory.setReconnAttemptCount(RECONNECTION_COUNT);
            emsFactory.setReconnAttemptDelay(RECONNECTION_DELAY);
            emsFactory.setReconnAttemptTimeout(jmsxGroupForEmsParams.connectionTimeout);
            emsConnection = emsFactory.createConnection(jmsxGroupForEmsParams.emsUser, jmsxGroupForEmsParams.emsPassword);
            shutdownThread.addJmsConnection(emsConnection);
            logger.info("EMS: " + emsConnection.toString());

            // Start the Client connection listening threads.
            for (int i = 0; i < intDestinationCount; i++) {
                DestinationParams destinationParams = jmsxGroupForEmsParams.destinations.get(i);

                if (destinationParams.sequenceType == null) destinationParams.sequenceType = DestinationParams.SeqType.GROUP_ONLY; // Default to no sequence with Group ID.

                ThreadParams threadParams = new ThreadParams(logger, jmsxGroupForEmsParams.connectionTimeout, jmsxGroupForEmsParams.recordMsgStats,
                        asClient, asClientGroup, asClientStats, asClientGroupStats, asGroupSequence, asGroupSequenceData, asLatestValueAlternateSpace,
                        jmsxGroupForEmsParams.emsConnectionUrl, jmsxGroupForEmsParams.emsUser, jmsxGroupForEmsParams.emsPassword, emsConnection,
                        jmsxGroupForEmsParams.emsClientIdProperty, jmsxGroupForEmsParams.routingThreads, null, destinationParams);

                tClientUpdate[i] = new Thread(new ClientUpdate(threadParams));
                tClientUpdate[i].start();
            }

            logger.info(APP_NAME + " started.");
            System.out.println(" " + APP_NAME + " started.");

            // Wait for the Client listening threads to stop.
            for (Thread tCA : tClientUpdate) {
                tCA.join();
            }
        }
        catch (ASException|JMSException e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
            System.err.println("\n ERROR.  Check log for details.");
            System.exit(1); // Do not continue if ActiveSpace creation error.
        }
        catch (InterruptedException iE) {
            logger.info("Thread interupted.");
        }
    }

}
