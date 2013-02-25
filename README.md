storm-word-count
================

Simple word-count-application using Twitter Storm. 

This application count the occurence of word in Twitter's sample stream. It currently supports two types of counting
 1. Normal counting, with no decay or weight function. Use "nrml" as the argument.
 2. Forward-exponential-decay, with exponential decay function implemented using forward decay (refers to Cormode's paper). Use "fedcount decay_alpha output_frequency" as the arguments. "decay_alpha" and "output_frequency" are expected to be integer value. 

To execute it, follow this step
 1. Setup the "storm-word-count/src/main/resources/twitter4j.properties" with your Twitter's Consumer secret, access token, access token secret and consumer key.
 2. If you want to execute the topology in local mode, execute using Eclipse's or your preferred IDE "execute" commands.
 3. If you want to execute the topology in distributed mode (i.e in Storm cluster), you need to create the executable jar file by executing this following maven command: "mvn package" from project root directory. Find the resulting jar in project_root/target folder. Submit the jar which contains "jar-with-dependencies" words in the jar name. 

