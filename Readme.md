# Movie Script Analysis using Hadoop MapReduce

## Project Overview
The purpose of this project is to implement and execute a **Hadoop MapReduce program** for analyzing movie scripts. The program processes a dataset of **movie script dialogues** and extracts insights while using **Hadoop Counters** to track key statistics. 

### **Problem Statement**
Each line in the movie script dataset follows the format:
```
Character: Dialogue
```
The MapReduce job performs the following tasks:
1. **Find the most frequently spoken words by characters.**
2. **Calculate the total dialogue length per character.**
3. **Extract unique words used by each character.**
4. **Track key statistics using Hadoop Counters.**

## Approach and Implementation
The implementation follows a modular approach using multiple **MapReduce jobs** to achieve the desired results.

### **Task 1: Most Frequently Spoken Words by Characters**
- **Mapper:** Tokenizes dialogue into words and emits `<Character: Word, 1>`
- **Reducer:** Aggregates word counts per character.

#### **CharacterWordMapper.java**
```java
public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().trim().split(":", 2);
        if (parts.length < 2) return;
        String character = parts[0].trim();
        StringTokenizer tokenizer = new StringTokenizer(parts[1].trim());
        while (tokenizer.hasMoreTokens()) {
            characterWord.set(character + ": " + tokenizer.nextToken());
            context.write(characterWord, one);
        }
    }
}
```

### **Task 2: Total Dialogue Length Per Character**
- **Mapper:** Extracts the length of dialogue per character and emits `<Character, Length>`
- **Reducer:** Sums up the total dialogue length per character.

### **Task 3: Unique Words Used by Each Character**
- **Mapper:** Extracts unique words per character and emits `<Character, Word>`
- **Reducer:** Collects and stores unique words per character.

### **Task 4: Hadoop Counters for Tracking Statistics**
- **Mapper:** Tracks key statistics such as **total lines, total words, total characters, unique words, and number of characters speaking.**
- **Reducer:** Aggregates and outputs these statistics in a structured format.

## **Execution Steps**

### **1. Start the Hadoop Cluster**
```bash
docker compose up -d
```

### **2. Build the Project using Maven**
```bash
mvn clean install
```

### **3. Move JAR File to Shared Folder**
```bash
mv target/*.jar shared-folder/input/jar/
```

### **4. Copy JAR File to Hadoop Container**
```bash
docker cp input/jar/hands-on2-movie-script-analysis-1.0-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### **5. Copy Input Dataset to Hadoop Container**
```bash
docker cp input/data/movie_dialogues.txt resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### **6. Access the Hadoop ResourceManager Container**
```bash
docker exec -it resourcemanager /bin/bash
```

### **7. Set Up HDFS and Upload Dataset**
```bash
hadoop fs -mkdir -p /input/dataset
hadoop fs -put /opt/hadoop-3.2.1/share/hadoop/mapreduce/movie_dialogues.txt /input/dataset
```

### **8. Run the MapReduce Job**
```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/hands-on2-movie-script-analysis-1.0-SNAPSHOT.jar com.movie.script.analysis.MovieScriptAnalysis /input/dataset/movie_dialogues.txt /output
```

### **9. View the Output**
#### **Task 1: Most Frequently Spoken Words**
```bash
hadoop fs -cat /output/task1/*
```
#### **Task 2: Total Dialogue Length per Character**
```bash
hadoop fs -cat /output/task2/*
```
#### **Task 3: Unique Words per Character**
```bash
hadoop fs -cat /output/task3/*
```
#### **Task 4: Hadoop Counters**
```bash
hadoop fs -cat /output/task4/*
```

## **Challenges Faced & Solutions**
1. **Issue:** Hadoop Counters were not properly formatted as an output file.
   - **Solution:** Implemented `CounterMapper.java` and `CounterReducer.java` to emit counters in a structured format.

2. **Issue:** Missing `args[1]` and `args[2]` handling in `MovieScriptAnalysis.java`.
   - **Solution:** Ensured correct usage of `new Path(args[1])` for input and `new Path(args[2] + "/taskX")` for output.

3. **Issue:** Incorrect file permissions when transferring files to Hadoop.
   - **Solution:** Used `docker cp` and verified HDFS file existence with `hadoop fs -ls /input/dataset`.

## **Sample Input and Expected Output**

### **Input (`movie_dialogues.txt`)**
```text
TYRION: A mind needs books as a sword needs a whetstone, if it is to keep its edge.
JON: The man who passes the sentence should swing the sword.
CERSEI: When you play the game of thrones, you win or you die. There is no middle ground.
```

### **Expected Output**
#### **Task 1: Most Frequent Words**
```
the 3
is 2
sword 2
a 1
```
#### **Task 2: Dialogue Length per Character**
```
TYRION 56
JON 49
CERSEI 72
```
#### **Task 3: Unique Words per Character**
```
TYRION [mind, needs, books, sword, whetstone, edge]
JON [man, passes, sentence, should, swing, sword]
CERSEI [play, game, thrones, win, die, middle, ground]
```
#### **Task 4: Hadoop Counters Output**
```
Total Lines Processed       3
Total Words Processed       21
Total Characters Processed  177
Total Unique Words Identified 15
Number of Characters Speaking 3
```

