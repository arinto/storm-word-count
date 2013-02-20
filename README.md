storm-word-count
================

Simple word-count-application using Twitter Storm. 

This application count the occurence of word in Twitter's sample stream. It currently supports two types of counting
1. Normal counting, with no decay or weight function
2. Forward-exponential-decay, with exponential decay function implemented using forward decay (refers to Cormode's paper).

To execute it, you need to setup the storm-word-count/src/main/resources/twitter4j.properties with your Twitter's Consumer secret, access token, access token secret and consumer key.
