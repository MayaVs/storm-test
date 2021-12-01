# Storm Coding Exercise #

## Overview ##

The aim of this exercise is to learn about the Storm framework and then demonstrate this knowledge.

Information on Storm can be found [here](http://storm-project.net/ "here").  A good place to start would be the [tutorial](http://storm.incubator.apache.org/documentation/Tutorial.html "tutorial").

This project contains a simple Storm topology that reads the contents of a local file, splits the text into single lines and then into individual words.  For each word in the file a line is written to an output file stating the word and the number of times that word has been previously seen.

## Tasks ##

1. Perform a code review of the application, noting any problems and describing any changes you would suggest to the developer. 
2. Describe some new features which could be made to extend this application.
3. Implement 1 or more of the new features you've listed.

## Notes ##

- We are not looking for a complete rewrite of the application for the code review. Focus on best practices and explain any potential defects in the code.
- Please do not fork this repository or submit answers as a Pull Request.  Test Responses should be submitted via email to your recruiter.

## My changes ##

1. BookTopology.java 
- Put the topology builder and config settings in separate methods out of run()
- Increased the sleep time to make sure there will be enough time to complete the tasks before killing the topology
- Set the spout parallelism to 1 to read the file only once 
- Changed the grouping for wordCount bolt from shuffleGrouping to fieldsGrouping to count each word in the same task

2. WordSplitBolt.java
- Changed the split regex to "\\W+"

3. FilePrinterBolt.java
- Initialise the FileWriter only once in prepare()
- Close the FileWriter only once in cleanup() when the bolt is about to shut down


## Additional features ##

1. printLinesWithFilterWordsToFile bolt
- Used to find lines containing crime words and print them in a file 
- Added a list with crime words in topology config
- Works on tuples with lines coming from line spout

2. matchWords bolt
- Used to match words against a list of selected words
- Works on tuples with word and count coming from wordCount
- No point of counting the most common words as they are prepositions and conjunctions, so decided to create a list with interesting/popular words and match them 
- Added a list with selected words in topology builder
- Works on tuples with all words and their counts coming from wordCount bolt
- Emitting the incoming tuples only if they match any of the selected words. 

3. printMatchedWordCount bolt
- Print all the tuples coming from MatchWords bolt
- Reusing PrinterBolt

4. totalPrintWordCount bolt
- Used to print some information for all processed tuples 
- Set only 1 parallelism to find information for all the tuples.
- Works on tuples coming from wordCount bolt, so no need to count again
- Added a list with not important words in topology config (prepositions and conjunctions)
- Print the most used word and count that is not in notImportantWords list 
- Print the total count of all the words that were in the file
- Print the number of different words in the file
- Print all the words and count in alphabetical order
- The printing in called from cleanup() when all the information is collected
- 

## Future Enhancements ##

1. Implement windowing (use timer to print the total information for every X ms in execute()) to offer information for realtime analytics
2. Add testing for the bolts.
3. Store processed data in database 

