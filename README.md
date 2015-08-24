TIBCO Enterprise Message Service (EMS) does not natively support JmsxGroup functions.  JmsxGroupForEMS uses TIBCO ActiveSpaces to add Message Groups (JmsxGroup) capabilities to EMS. Basic JmsxGroup ensures that all messages for the same JMSXGroupId will be sent to the same JMS consumer - while that consumer stays alive. As soon as the consumer dies, another will be chosen.  JmsxGroupForEMS also supports latest value only, were only messages with newer date-times than previous messages are sent to the JmsxGroup, and it supports JMSXGroupSeq, where the messages in a JmsxGroup are sent to the consumers in the numerical order of the JMSXGroupSeq property.