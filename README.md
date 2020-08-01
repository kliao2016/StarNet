Programming Assignment 2 Preliminary Implementation by Kevin Liao (kliao2020@gatech.edu) and Brian Glowniak (bglowniak@gatech.edu): Due November 11, 2018

	- Files submitted:
		- star-node.py: The source code for a node in our star network
		- Kevin_Log.txt: A log showing the details of RTT, peer discovery, and message sending at the times each process was initiated
		- sample.txt: A text file showing what a command to run the program looks like and what may be outputted to the console when running star-node.py
        - README.pdf: Our design report that explains the details of our star network

	- Instructions for compiling and running program:
		- To run star-node.py, use the following command (Make sure to use Python 3): python3 <name> <local-port> <PoC-address> <PoC-port> <N>
			- name = name of node (can be anything)
			- local-port = the local port number of the node
			- PoC-address = the address of the node's PoC (Use 0 for no PoC)
			- PoC-port = the port of the node's PoC (Use 0 for no PoC)
			- N = the expected number of nodes in the network
        - disconnect: Stop running a node
		- When sending a message, type: send <message> where message has double quotes if it's a normal ASCII message and no quotes if it's a file
		- Type: show-status to show the status of the node
		- Type: show-log to show a log of what processes the node has gone through

	- Known limitations and bugs:
		- When sending files and images, the bytes are sent, but only the name of the file sent is put in the log and printed to the console. There is no support for showing the contents of a file or images sent.
        - Sometimes, it can take a few seconds before a node is recognized as offline by other nodes and their rtt vectors.
        - Occasionally, it can also take a long time for nodes to disconnect because all running threads must stop first.
        - It is possible for a node to freeze after running the disconnect command, but it doesn't affect the processes of other nodes.
