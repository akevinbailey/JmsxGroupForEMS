DESCRIPTION:

TIBCO Enterprise Message Service (EMS) does not natively support JmsxGroup functions.  JmsxGroupForEMS uses TIBCO ActiveSpaces to add Message Groups (JmsxGroup) capabilities to EMS. Basic JmsxGroup ensures that all messages for the same JMSXGroupId will be sent to the same JMS consumer - while that consumer stays alive. As soon as the consumer dies, another will be chosen.

JmsxGroupForEMS also supports latest value only, were only messages with newer date-times than previous messages are sent to the JmsxGroup, and it supports JMSXGroupSeq, where the messages in a JmsxGroup are sent to the consumers in the numerical order of the JMSXGroupSeq property.

NOTES:

1)  Initially, JmsxGroupForEMS should be started immediately after EMS and before any consumers connect to EMS.  After JmsxGroupForEMS first connects to EMS, durable subscribers are created on EMS's connection advisory notifications and will therefor process any missed connections, disconnections or client messages.

2)  JmsxGroupForEMS uses transaction for all EMS and ActiveSpaces interactions.  This results in slightly lower performance, but will guarantee consistency and zero data loss.

3)  By default the ActiveSpaces created by JmsxGroupForEMS have shared nothing persistence.  This allows, after the initial startup, consistency to be maintained even if EMS or JmsxGroupForEMS were to go down.  Restarting either EMS and/or JmsxGroupForEMS is all that is necessary.  No data loss should occur.

4)  Assigning or reassigning a JMSXGroupID to a new consumer is done by round-robbin.  If the 'routingThreads' configuration setting is greater than 1, then the multiple threads could assign new or reassigned JMSXGroupIDs to the same consumer.  This is only an initial occurrence and over time the JMSXGroupIDs will balance across the consumers.

5)  JmsxGroupForEMS is optimized for the fast processing, and the Spaces (tables) schemas are optimized to minimize record locking.

WARNINGS:

1)  Setting 'recordMsgStats' to 'true' can cause record locks on the EmsClientStats and EmsClientGroupStats Spaces under very high volumes.  If this occurs, set 'recordMsgStats' to 'false'.

2)  For LATEST_VALUE_ONLY sequence type, messages with the same JMSXGroupID or 'latestValueAlternateIdProperty' must have different timestamps.  The application will treat messages with the same JMSTimestamp or 'latestValueDateTimeProperty' as duplicates and will discard the later arriving messages with the same timestamp.

3)  LATEST_VALUE_ONLY, GROUP_EXACT_SEQUENCE sequence types, can have record locks under very high volumes.  There is not work-around for this condition.

4)  The EmsClientGroup, EmsClientGroupStats, EmsGroupSequence, and EmsLatestValueAlternate Spaces have the potential to get very large.  External monitoring should be put in place to provided warning if a Space is getting to big.  Also, unneeded tuples (records) should be taken (deleted).

5)  EmsGroupSequenceCache Space contains out of sequence messages for the GROUP_EXACT_SEQUENCE sequence types.  Ideally there should not be any tuples in this Space.  External monitoring should be put in place to remove old tuples and/or reset the sequence number if there is a permanently missing message.