import threading
import sys
import socket
import queue
import time
import os

class StarNode:
    def __init__(self, name, port, numNodes, pocAddress = 0, pocPort = 0):
        # Check and set name
        if len(name) < 1 or len(name) > 16:
            raise ValueError("Error: Name must be between 1 and 16 characters.")

        if ";" in name or "|" in name:
            raise ValueError("Error: Name cannot contain ; or |")
        self.name = name

        # Check and set local port
        if not self.verify_port(port):
            raise ValueError("Error: Local port must be a number between 0 and 65535.")

        self.localPort = int(port)

        # Check and set PoC values
        if pocAddress == "0" and pocPort == "0":
            self.poc = (0,0)
        else:
            try:
                addr = socket.gethostbyname(pocAddress)
            except socket.error:
                raise ValueError("Error: Invalid PoC address entered.")
            if not self.verify_port(pocPort):
                raise ValueError("Error: PoC port must be a number between 0 and 65535.")

            self.poc = (addr, int(pocPort)) # (PoC IP, PoC Port)

        # Check and set N
        try:
            self.numNodes = int(numNodes)
        except:
            raise ValueError("Error: N must be a valid number")

        self.exit_flag = False # used to exit all threads in case of error or disconnect

        # Set up socket for this node and get IP
        self.node_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.node_socket.settimeout(3)
        self.node_socket.bind(('', self.localPort))
        self.node_socket_lock = threading.Lock()
        self.hostname = socket.gethostname()
        self.IP = socket.gethostbyname(self.hostname)

        # SET UP LOGGING
        self.log = open(self.name + "_Log.txt", "w+")
        self.log_lock = threading.Lock()
        self.log_message("Log for Star Node " + self.name)

        # SET UP DATA STRUCTURES
        self.peer_table = {
            self.name: (self.IP, self.localPort, True)
        }
        self.peer_table_lock = threading.Lock()
        self.peer_discovery_exit = False
        self.pd_queue = queue.Queue(self.numNodes)
        self.rtt_vector_lock = threading.Lock()
        self.rtt_vector = [] # no lock needed b/c it's only used in one thread
        self.rtt_queue = queue.Queue()
        self.sum_queue = queue.Queue()
        self.seq_num_dict = {}
        self.msg_ack_queue = queue.Queue()
        self.sum_ack_queue = queue.Queue()
        self.keep_alive_queue = queue.Queue()
        self.msg_packet_num = 0
        self.msg_set = set()

        self.hub = ("", -1) # (Name, RTT Sum) -> -1 denotes it hasn't been set
        self.hub_lock = threading.Lock()

        # SET UP PACKET RECEIVER THREAD
        threading.Thread(target=self.main_packet_receiver, name="receiver").start()

        # BEGIN PEER DISCOVERY
        if len(self.peer_table) != self.numNodes:
            self.peer_discovery() # returns only when this node finds N nodes

        if not self.exit_flag: # if peer discovery did not error
            # BEGIN RTT PROCESS + HUB SELECTION
            self.rtt_iteration = threading.Event()
            threading.Thread(target=self.rtt_hub_process, name="rtt process", args=[self.rtt_iteration]).start()
            self.rtt_iteration.wait() # wait until we have picked our first hub

    def verify_port(self, port):
        try:
            portNum = int(port)
        except:
            return False
        if portNum < 0 or portNum > 65535:
            return False
        return True

    def reset_sum_seq_num(self, name):
        self.seq_num_dict[name] = -1

    def set_node_is_active(self, node, isActive, disconnect = False):
        with self.peer_table_lock:
            entry = self.peer_table[node]
            new_entry = (entry[0], entry[1], isActive)
            self.peer_table[node] = new_entry

        if disconnect:
            self.reset_sum_seq_num(node)
            self.log_message("Node " + node + " has disconnected.")
        elif not isActive:
            self.reset_sum_seq_num(node)
            self.log_message("Node " + node + " has gone offline.")
        else:
            self.log_message("Node " + node + " has come back online.")

    def peer_discovery(self):
        poc_response = threading.Event()
        discovery_complete = threading.Event()
        poc_found = threading.Event()

        threading.Thread(target=self.receive_peer_discovery, args=[poc_response, discovery_complete]).start()

        if self.poc != (0,0):
            ping_thread = threading.Thread(target=self.ping_poc, args=[poc_response, poc_found])
            ping_thread.start()
            ping_thread.join()

            if self.exit_flag:
                if poc_found.isSet():
                    message = "Error: Peer Discovery timed out. Unable to discover all nodes."
                else:
                    message = "Error: Peer Discovery timed out. Unable to find PoC."
                self.exit()
                raise ValueError(message)
        elif not discovery_complete.wait(120): # if no pinging, run for 2 minutes
            self.exit()
            raise ValueError("Error: Peer Discovery timed out. Unable to discover all nodes.")

        time.sleep(3)

    def ping_poc(self, response, poc_found):
        poc_packets_sent = 0
        while poc_packets_sent < 40 and not self.exit_flag and not self.peer_discovery_exit:
            packet = "0000" + self.serialize_peer_table()
            with self.node_socket_lock:
                self.node_socket.sendto(packet.encode(), self.poc)
            if response.wait(1): # wait for response
                response.clear()
                poc_found.set()
                if len(self.peer_table) == self.numNodes: # we have sent our PoC all info, so we can stop pinging
                    break
                time.sleep(3)

            poc_packets_sent += 1

        if poc_packets_sent == 40 and len(self.peer_table) != self.numNodes: # we have sent the max number of PoC packets
            self.exit_flag = True

    def receive_peer_discovery(self, response, complete):
        while not self.exit_flag and not self.peer_discovery_exit:
            try:
                received = self.pd_queue.get(True, 1) # will block until queue is not empty
            except queue.Empty:
                continue

            packet = received[0]
            src = received[1]

            recv_peer_table = self.deserialize_peer_table_string(packet[4:]) # get the received peer table
            change = self.merge_peer_table(recv_peer_table)

            if src == self.poc:
                response.set() # signal to ping thread that a response was received
            else:
                # respond to where we received the ping from
                new_packet = "0000" + self.serialize_peer_table()
                with self.node_socket_lock:
                    self.node_socket.sendto(new_packet.encode(), src)

            # signal that peer discovery is complete for this node without exiting the thread, as we may still receive pings from nodes that are still discovering
            if len(self.peer_table) == self.numNodes:
                complete.set()

        if self.peer_discovery_exit: # ensures that pd doesn't block in some edge cases
            complete.set()

    def rtt_hub_process(self, signal):
        sum_seq_num = 0
        while not self.exit_flag:
            # compute RTT vector
            self.keep_alive_probe()
            signal.clear()
            with self.rtt_vector_lock:
                self.rtt_vector.clear()
            for key, value in self.peer_table.items():
                if value[2] and key != self.name:
                    rtt_packet = "0001" + self.name
                    current_node = (value[0], value[1])
                    rttAttempts = 0
                    while rttAttempts < 5:
                        if rttAttempts == 0:
                            self.log_message("Sending RTT Request to " + key + ".")
                        else:
                            self.log_message("Resending RTT request to " + key + ".")
                        with self.node_socket_lock:
                            send_time = time.time()
                            self.node_socket.sendto(rtt_packet.encode(), current_node)
                        try:
                            res_packet = self.rtt_queue.get(True, 3)
                            break
                        except queue.Empty: # either the request or the response was lost, or the node is offline
                            rttAttempts += 1

                    if rttAttempts == 5: # if 5 attempts failed, we assume the node is offline
                        self.set_node_is_active(key, False)
                    else:
                        with self.rtt_vector_lock:
                            self.rtt_vector.append((key, (time.time() - send_time)))

            # compute RTT Sum
            with self.rtt_vector_lock:
                rtt_sum = sum([x[1] for x in self.rtt_vector])

            self.log_message("Computed new RTT sum of " + str(rtt_sum))

            # spawn thread to send RTT sum packets and handle ACKs
            sum_thread = threading.Thread(target=self.send_sum_packets, name="sum thread", args=[sum_seq_num, rtt_sum])
            sum_thread.start()

            # receive and process RTT sum packets
            sum_packets_received = 0
            min_node = (self.name, rtt_sum)
            with self.peer_table_lock:
                num_active = sum(1 for k,v in self.peer_table.items() if v[2] and not k == self.name)
            while sum_packets_received < num_active:
                try:
                    message = self.sum_queue.get()
                except queue.Empty:
                    break
                    #if a node has somehow gone offline without sending all packets

                sum_packet = message[0]
                src = message[1]

                if sum_packet[0:4] != "0003":
                    continue # this shouldn't happen

                fields = sum_packet[4:].split(";")
                recv_name = fields[0]
                recv_sum = fields[2]

                self.log_message("Received RTT Sum packet from " + recv_name + ". Value is " + recv_sum)
                current_sum = float(recv_sum)
                if current_sum < min_node[1] or (current_sum == min_node[1] and min_node[0] > recv_name):
                    min_node = (recv_name, current_sum)

                sum_packets_received += 1

            # choose hub after processing
            with self.hub_lock:
                if self.hub[1] == -1 or not self.peer_table[self.hub[0]][2]: # no hub chosen yet or hub offline
                    self.hub = min_node
                    self.log_message("Hub selected as " + self.hub[0] + " with RTT Sum of " + str(self.hub[1]) + ".")
                elif min_node[0] == self.hub[0]: # same hub, just different RTT
                    self.hub = (self.hub[0], min_node[1])
                    self.log_message("Hub remains " + self.hub[0] + " with updated RTT Sum of " + str(min_node[1]) + ".")
                else:
                    # if min_node is within 25% of previous RTT, don't change to avoid instability
                    difference = abs(min_node[1] - self.hub[1])
                    if self.hub[1] == 0.0: # avoid divide by zero
                        temp = 0.01
                    else:
                        temp = self.hub[1]
                    if (difference / temp) > 0.25: # MAY NEED TO ADJUST THIS NUMBER
                        self.hub = min_node
                        self.log_message("Hub selected as " + self.hub[0] + " with RTT Sum of " + str(self.hub[1])+ ".")

            signal.set()
            if sum_seq_num < 9:
               sum_seq_num += 1
            else:
                sum_seq_num = 0
            time.sleep(5)

    def send_sum_packets(self, seq, rtt_sum):
        for key, value in self.peer_table.items():
            if value[2] and key != self.name:
                sum_packet = "0003" + self.name + ";" + str(seq) + ";" + str(rtt_sum)
                attempts = 0
                while attempts < 5:
                    with self.node_socket_lock:
                        self.node_socket.sendto(sum_packet.encode(), (value[0], value[1]))
                    try:
                        ack = self.sum_ack_queue.get(True, 3)
                        break
                    except queue.Empty:
                        attempts += 1

                if attempts == 5:
                    self.set_node_is_active(key, False)

    def keep_alive_probe(self):
        for key, value in self.peer_table.items():
            if key != self.name and not value[2]: # node is inactive
                probe = "0007" + self.serialize_peer_table()
                current_node = (value[0], value[1])
                attempts = 0
                while attempts < 5:
                    with self.node_socket_lock:
                        self.node_socket.sendto(probe.encode(), current_node)
                    self.log_message("Sending Keep Alive Probe to " + key + ".")
                    try:
                        response = self.keep_alive_queue.get(True, 3)
                        self.set_node_is_active(key, True)
                        break
                    except queue.Empty: # either the probe or the response was lost, or the node is still offline
                        attempts += 1

    def main_packet_receiver(self):
        while not self.exit_flag:
            try:
                data, src = self.node_socket.recvfrom(64000) # max message size is 64KB
                if data:
                    packet = data.decode()
                    packet_type = packet[0:4]
                    if packet_type == "0000": # Peer Discovery
                        self.pd_queue.put((packet, src))
                    elif packet_type == "0001": # RTT Request
                        rtt_res_packet = "0002" + self.name
                        with self.node_socket_lock:
                            self.node_socket.sendto(rtt_res_packet.encode(), src)
                        self.log_message("Received RTT Request from " + packet[4:] + ". Sending RTT Response back.")
                    elif packet_type == "0002": # RTT Response
                        self.rtt_queue.put((packet, src))
                        self.log_message("Received RTT Response from " + packet[4:])
                    elif packet_type == "0003": # RTT Sum for Hub Calculation
                        threading.Thread(target=self.process_sum, args=[packet, src]).start()
                    elif packet_type == "0004": # Message Packet
                        self.receive_message(packet, src)
                    elif packet_type == "0005": # ACK Packet
                        if packet[4:] == "0003": # Msg broadcast
                            self.msg_ack_queue.put((packet, src))
                        elif packet[4:] == "0002": # Sum packet
                            self.sum_ack_queue.put((packet, src))
                    elif packet_type == "0006": #disconnect packet
                        node = packet[4:]
                        self.set_node_is_active(node, False, True)
                    elif packet_type == "0007": # Keep-Alive Probe
                        if len(self.peer_table) != self.numNodes:
                            table = self.deserialize_peer_table_string(packet[4:])
                            change = self.merge_peer_table(table)
                            self.peer_discovery_exit = True
                        keep_alive_res = "0008" + self.name
                        if not self.exit_flag:
                            with self.node_socket_lock:
                                self.node_socket.sendto(keep_alive_res.encode(), src)
                    elif packet_type == "0008": # Keep-Alive Response
                        self.keep_alive_queue.put((packet, src))
            except socket.error as error:
                pass

    def process_sum(self, packet, src):
        fields = packet[4:].split(";")
        name = fields[0]
        recv_seq_num = int(fields[1])

        if not name in self.seq_num_dict or self.seq_num_dict[name] == -1:
            self.seq_num_dict[name] = recv_seq_num

        if self.seq_num_dict[name] == recv_seq_num:
            self.sum_queue.put((packet, src))
            if recv_seq_num < 9:
                self.seq_num_dict[name] += 1
            else:
                self.seq_num_dict[name] = 0

        self.send_ack(src, False)

    def send_to_hub(self, message):
        with self.hub_lock:
            if self.hub[1] != -1:
                msg_packet = ""
                if message[0] != '"':
                    if os.path.exists(message):
                        with open(message, "rb") as file:
                            msg_bytes = file.read()
                            msg_packet = "0004" + self.pad_packet_num(self.msg_packet_num) + self.name + ";" + str(msg_bytes) + "|" + message + "|" + self.hub[0]
                    else:
                        print("\nFile doesn't exist. If you are sending a message, please use quotes.")
                        return
                else:
                    msg_packet = "0004" + self.pad_packet_num(self.msg_packet_num) + self.name + ";" + message + "|0|" + self.hub[0]
                if self.hub[0] != self.name:
                    if len(msg_packet) < 64000:
                        # Get address and port of current hub
                        with self.peer_table_lock:
                            cur_hub = (self.peer_table[self.hub[0]][0], self.peer_table[self.hub[0]][1])
                            send_attempts = 0
                            while send_attempts < 5:
                                with self.node_socket_lock:
                                    self.node_socket.sendto(msg_packet.encode(), cur_hub)
                                if send_attempts > 0:
                                    self.log_message("Resending message to " + self.hub[0] + " (Current Hub).")
                                else:
                                    self.log_message("Sending message to " + self.hub[0] + " (Current Hub).")

                                try:
                                    ack_packet = self.msg_ack_queue.get(True, 3)
                                    self.msg_packet_num += 1
                                    return
                                except queue.Empty: # packet or ACK was lost, or hub is offline
                                    send_attempts += 1

                            if send_attempts == 5:
                                # At this point, we know the destination node is offline, so we update peer table
                                self.set_node_is_active(self.hub[0], False)
                                print("Hub " + self.hub[0] + " is offline")
                    else:
                        print("\nMessage too large. Please send messages smaller than 64 KB.")
                        return
                else:
                    src = (self.IP, self.localPort)
                    threading.Thread(target=self.forward_message, args=[msg_packet, src]).start()
                    return

        # Wait for rtt thread to recalculate hub
        print("Recalculating Hub...")
        time.sleep(5)
        self.send_to_hub(message)

    def receive_message(self, msg_packet, src):
        self.send_ack(src, True)
        msg_num = int(msg_packet[4:8])
        packet_check = (src, msg_num)

        if packet_check not in self.msg_set:
            self.msg_set.add(packet_check)
            payload = msg_packet[8:].split(';') # All messsages guaranteed to have semicolon
            message = payload[1].split('|')
            if message[0][0] == '"': # Check for ascii message
                self.log_message("Received message from " + payload[0] + ": " + message[0])
                print("\nMessage from " + payload[0] + ": " + message[0]) # Display message
            else:
                self.log_message("Received file from " + payload[0] + ": " + message[1])
                print("\nFile received from " + payload[0] + ": " + message[1])

            if message[2] == self.name: # This node is the hub
                # Use separate thread to clear main thread for receiving acks
                threading.Thread(target=self.forward_message, args=[msg_packet, src]).start()
                return

    def forward_message(self, message, src):
        for key, value in self.peer_table.items():
            dest_node = (value[0], value[1])
            if value[2] and dest_node != src and key != self.name: # Don't forward message to source
                send_attempts = 0
                while send_attempts < 5:
                    with self.node_socket_lock:
                        new_message = self.change_packet_num(message)
                        self.node_socket.sendto(new_message.encode(), dest_node)
                    if send_attempts > 0:
                        self.log_message("Retransmitting message to " + key + ".")
                    else:
                        self.log_message("Forwarding message to " + key + ".")

                    try:
                        ack_packet = self.msg_ack_queue.get(True, 3)
                        self.msg_packet_num += 1
                        break
                    except queue.Empty: # either the request or the response was lost
                        send_attempts += 1

                if send_attempts == 5:
                    # At this point, we know the destination node is offline, so we update peer table
                    self.set_node_is_active(key, False)
                    return

    def send_ack(self, src, is_msg):
        ack_packet = "00050003" if is_msg else "00050002"
        with self.node_socket_lock:
            self.node_socket.sendto(ack_packet.encode(), src)

    def reverse_peer_lookup(self, src):
        with self.peer_table_lock:
            for key, value in self.peer_table.items():
                if (value[0], value[1]) == src:
                    return str(key)

    def pad_packet_num(self, num):
        return str(num).zfill(4)

    def change_packet_num(self, packet):
        packet_list = list(packet)
        packet_list[4:8] = list(self.pad_packet_num(self.msg_packet_num))
        return "".join(packet_list)

    def print_status(self):
        if self.rtt_iteration.wait():
            print("Star Node " + self.name)
            print("RTT Measurements:")
            with self.rtt_vector_lock:
                for entry in self.rtt_vector:
                    print("Node " + entry[0] + " | RTT: " + str(entry[1]))

            with self.hub_lock:
                print("Current Hub: " + self.hub[0])

    def print_log(self):
        with self.log_lock:
            self.log.seek(0)
            print(self.log.read())

    def log_message(self, message):
        now = time.strftime("%H:%M:%S", time.localtime(time.time()))
        info = now + " - " + message + "\n"
        with self.log_lock:
            self.log.write(info)
            self.log.flush()

    def serialize_peer_table(self):
        with self.peer_table_lock:
            result = ""
            for key, value in self.peer_table.items():
                result += key + ";" + value[0] + ";" + str(value[1]) + ";"
                if value[2]:
                    result += "1|"
                else:
                    result += "0|"
            return result[:-1]

    def deserialize_peer_table_string(self, pStr):
        table = {}
        for entry in pStr.split("|"):
            values = entry.split(";")
            name = values[0]
            ip = values[1]
            port = int(values[2])
            isActive = True if values[3] == "1" else False
            table[name] = (ip, port, isActive)
        return table

    def merge_peer_table(self, table):
        update = False
        for key in table:
            with self.peer_table_lock:
                if not key in self.peer_table:
                    self.peer_table[key] = table[key]
                    update = True
                    self.log_message("Star Node " + key + " discovered.")
        return update

    def disconnect(self):
        for key, value in self.peer_table.items():
            if value[2] and key != self.name:
                node = (value[0], value[1])
                disconnect_packet = "0006" + self.name
                with self.node_socket_lock:
                    self.node_socket.sendto(disconnect_packet.encode(), node)

        self.exit()

    def exit(self):
        self.exit_flag = True
        for thread in threading.enumerate():
            if thread != threading.current_thread():
                thread.join()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Error: Incorrect number of arguments. Expected: star-node <name> <local-port> <PoC-address> <PoC-port> <N>.\nEnter 0 for both <PoC-address> and <PoC-port> to default to no PoC.\nExiting node...")
        exit(1)

    name = sys.argv[1]
    port = sys.argv[2]
    pocAddress = sys.argv[3]
    pocPort = sys.argv[4]
    numNodes = sys.argv[5]

    try:
        print("Setting up Star Node...")
        node = StarNode(name, port, numNodes, pocAddress, pocPort)
    except ValueError as error:
        print(error.args[0] + "\nExiting node...")
        exit(1)

    # BEGIN USER INPUT
    while True:
        try:
            command = input("Star-Node Command: ")
            if command == "disconnect":
                print("Exiting node...")
                node.disconnect()
                node.log_message("Node disconnected.")
                break
            elif command == "show-status":
                node.print_status()
            elif command == "show-log":
                node.print_log()
            elif command[0:4] == "send":
                node.send_to_hub(command[5:])
            elif command == "help":
                print("Star-Node Commands:")
                print("\"send <message/filename>\" - broadcast a message or file to the network. Max size is 64KB.")
                print("\"show-status\" - display the node's current RTT vector and selected hub.")
                print("\"show-log\" - display a timed log of all major events since node boot-up.")
                print("\"disconnect\" - close this star node.")
            else:
                print("Invalid command entered. Enter \"help\" for a list of valid commands.")
        except KeyboardInterrupt: # slight delay to allow all threads to close properly
            node.exit()
            break
