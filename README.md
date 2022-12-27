# spark-streaming-tutorial

## Theory

![spark-stream-processing-model.png](img/spark-stream-processing-model.png)

---

## Development Environment

- OS: Windows 11
- Java: 1.11
- Scala: 2.12.17
- Spark: 3.3.1
- Hadoop: 3.3.1
- IDE: Intellij IDEA Community

---

## Word Count

Use netcat to listen to a port, and wait for input from command line. After inputting some words in command line, they will be counted in real time, and the result will be printed out in console in Intellij IDEA.

Steps: 

1. Download nmap from https://nmap.org/download#windows, and install. 
2. Open terminal, and type `ncat -lk 9999`. (9999 is a random port number here.)
3. Intellij IDEA, execute class `StreamingWordCount.scala`.
4. In terminal, type random words. 
5. Result should be seen in console of Intellij IDEA.

---

## Troubleshooting

Problem 1:

When executing Spark streaming job, encountered the error: `Exception in thread "main" java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Ljava/lang/String;I)Z`.

Solution: 

Apart from the file "winutils.exe", also need to put the file "hadoop.dll" under folder "hadoop-3.3.1\bin".
