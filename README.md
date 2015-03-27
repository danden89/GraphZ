# GraphZ
## Installation
### Dependencies
GraphZ depends on following packages and tools:<br>
>>g++ >= 4.7<br>
>>boost >= 1.55<br>
>>tc_malloc >= 2.0<br>

### Compilation
After satisfing upper denpendencies, you can type following for making the "preprocess" executable file:
>>make preprocess<br>
Get binaries for graph algorithms:<br>
		make
		
## Get Started
### Preprocess
First, we need to do preprocess, and convert graphs to GraphZ's Degree-ordered storage fromat. For example:
>>./preprocess ~/Downloads/com-lj.txt 200000<br>

Here 200000 is the number of vertices for each partition. At the end of preprocessing, you'll gonna see a directory "~/Downloads/com-lj.txt.dir".

### Run algorithms
After "make", you can see executable graph algorithms binary files under GRAPHZ_HOME/bin. For example, to run BFS on the upper graph, you can type:
>>bin/bfs_new  ~/Downloads/com-lj.txt
