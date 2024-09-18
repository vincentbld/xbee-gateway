#=========================================================================================

__author__ = "Jarryd Bekker"
__copyright__ = "Copyleft 2015, Bushveld Labs"

__license__ = "GPL"
__version__ = ""
__maintainer__ = "Jarryd Bekker"
__email__ = "jarryd@bushveldlabs.com"
__status__ = "Development"

#=========================================================================================

# To do:

#   TODO Relook at the 'assemble' functions in the message classes. Make a separate function for the variable headers
#   TODO Need a script to handle connection interruptions. i.e. pause and/or buffer incoming data streams,
#   TODO If a PINRESP is not recieved withing the keep alive period, we're disconnected

#=========================================================================================


# Python modules
import socket
from threading import Thread
import sched, time
import sys


# Third party modules

# Custom modules

#=========================================================================================

# Message types (subset of MQTT 3.1)
MSG_CONNECT = 0x10
MSG_CONNACK = 0x20
MSG_PUBLISH = 0x30
MSG_PUBACK = 0x40       # TODO
MSG_PUBREC = 0x50       # TODO
MSG_PUBREL = 0x60       # TODO
MSG_PUBCOMP = 0x70      # TODO
MSG_SUBSCRIBE = 0x80    # TODO
MSG_SUBACK = 0x90       # TODO
MSG_UNSUBSCRIBE = 0xA0  # TODO
MSG_UNSUBACK = 0xB0     # TODO
MSG_PINGREQ = 0xC0
MSG_PINGRESP = 0xD0
MSG_DISCONNECT = 0xE0

#=========================================================================================

class CONNECT(object):

    def __init__(self, client_id, keep_alive=60):
        self.clientID = client_id
        
        self.message_type = MSG_CONNECT
        
        self.DUP = False
        self.QoS = False
        self.retain = False
        
        self.User_name_flag = False
        self.Password_flag = False
        self.Will_RETAIN = False
        self.Will_QoS_MSB = False
        self.Will_QoS_LSB = False
        self.Will_flag = False
        self.Clean_Session = True
        
        self.protocol_name = "MQIsdp"
        self.protocol_version = chr(3)
        self.keep_alive = keep_alive
        
        # Client ID length
        self.client_id_length_MSB = chr((len(self.clientID) & 0xFF00) >> 8)
        self.client_id_length_LSB = chr((len(self.clientID) & 0x00FF))
        
        # Protocol name length
        self.protocol_name_length_MSB = chr((len(self.protocol_name) & 0xFF00) >> 8)
        self.protocol_name_length_LSB = chr(len(self.protocol_name) & 0x00FF)
        
        # Keep alive
        self.keep_alive_MSB = chr((self.keep_alive & 0xFF00) >> 8)
        self.keep_alive_LSB = chr(self.keep_alive & 0x00FF)
        

    def connect_flags(self):
    
        connect_flags = 0x00
        
        if self.User_name_flag: connect_flags = connect_flags or 0x80
        if self.Password_flag: connect_flags = connect_flags or 0x40
        if self.Will_RETAIN: connect_flags = connect_flags or 0x20
        if self.Will_QoS_MSB: connect_flags = connect_flags or 0x10
        if self.Will_QoS_LSB: connect_flags = connect_flags or 0x08
        if self.Will_flag: connect_flags = connect_flags or 0x04
        if self.Clean_Session: connect_flags = connect_flags or 0x02
        
        return chr(connect_flags)

    def fixed_header(self):
    
        header = self.message_type
        
        if self.DUP == 1: header = header or 0x08
        
        if self.QoS == 1: header = header or 0x02
        elif self.QoS == 2: header = header or 0x04
        elif self.QoS == 3: header = header or 0x06

        if self.retain == 1: header = chr(header) or 0x01

        return chr(header)

    def fixed_header_remaining_length(self):
        remaining_length = format_length(len(
            self.protocol_name_length_MSB +
            self.protocol_name_length_LSB +
            self.protocol_name +
            self.protocol_version +
            self.connect_flags() +
            self.keep_alive_MSB +
            self.keep_alive_LSB +
            self.client_id_length_MSB +
            self.client_id_length_LSB +
            self.clientID))
            
        return remaining_length
            
    def assemble(self):

        message = (self.fixed_header() +
                    self.fixed_header_remaining_length() +
                    self.protocol_name_length_MSB +
                    self.protocol_name_length_LSB +
                    self.protocol_name +
                    self.protocol_version +
                    self.connect_flags() +
                    self.keep_alive_MSB +
                    self.keep_alive_LSB +
                    self.client_id_length_MSB +
                    self.client_id_length_LSB +
                    self.clientID)      
                
        return message

#-----------------------------------------------------------------------------------------

class CONNACK(object):

    def __init__(self):
        self.message_type = MSG_CONNACK
        self.remaining_length = 0
        
        self.return_codes = {0: 'Connection Accepted',
                             1: 'Connection Refused: unacceptable protocol version',
                             2: 'Connection Refused: identifier rejected',
                             3: 'Connection Refused: server unavailable',
                             4: 'Connection Refused: bad user name or password',
                             5: 'Connection Refused: not authorized'}

    def parse(self, response):
        """ Parse CONNACK message"""
        
        if len(response) == 4:
            """ Check that the response is the expected size (should be 2 fixed header bytes
            and 2 variable header bytes)"""
        
            # Convert the hex length byte to an integer
            self.remaining_length = ord(response[1])
            
            code = ord(response[3])
            
            message = self.return_codes[ord(response[3])]
            
        else:
            # Something is wrong...
            code = ''
            message = ''
            print("ERROR: Something is wrong in the CONNACK parser. Didn't receive the correct response size")

        return code, message

#-----------------------------------------------------------------------------------------

class PUBLISH(object):

    def __init__(self, topic, payload, qos):
        self.Topic = topic
        self.Payload = payload
        self.QoS = qos
        
        self.message_type = MSG_PUBLISH
        
        self.DUP = 0
        self.QoS = 0
        self.retain = 0         

        self.TopicLength_MSB = chr((len(self.Topic) & 0xFF00) >> 8)
        self.TopicLength_LSB = chr(len(self.Topic) & 0x00FF)


    def fixed_header(self):
    
        header = self.message_type
        
        if self.DUP == 1: header = header or 0x08
        
        if self.QoS == 1: header = header or 0x02
        elif self.QoS == 2: header = header or 0x04
        elif self.QoS == 3: header = header or 0x06

        if self.retain == 1: header = header or 0x01

        return chr(header)
        
    def fixed_header_remaining_length(self):
        remaining_length = format_length(len(
            self.TopicLength_MSB + 
            self.TopicLength_LSB +
            self.Topic +
            self.Payload))
            
        return remaining_length
            
    def assemble(self):

        message = (self.fixed_header() +
                    self.fixed_header_remaining_length() +
                    self.TopicLength_MSB +
                    self.TopicLength_LSB +
                    self.Topic +           
                    self.Payload)    
                
        return message

#-----------------------------------------------------------------------------------------

class PINGREQ(object):

    def __init__(self):

        self.message_type = MSG_PINGREQ

        self.DUP = False
        self.QoS = False
        self.retain = False

    def fixed_header(self):

        header = self.message_type

        if self.DUP == 1: header = header or 0x08

        if self.QoS == 1: header = header or 0x02
        elif self.QoS == 2: header = header or 0x04
        elif self.QoS == 3: header = header or 0x06

        if self.retain == 1: header = chr(header) or 0x01

        return chr(header)

    @staticmethod
    def fixed_header_remaining_length():

        remaining_length = "\x00"

        return remaining_length

    def assemble(self):

        message = (self.fixed_header() +
                    self.fixed_header_remaining_length())

        return message

#-----------------------------------------------------------------------------------------

class PINGRESP(object):

    def __init__(self):
        self.message_type = MSG_PINGRESP
        self.remaining_length = 0

    def parse(self, response):
        """ Parse PINGRESP message"""

        if len(response) == 2:
            """ Check that the response is the expected size (should be 2 fixed header bytes
            and 2 variable header bytes)"""

            # Convert the hex length byte to an integer
            self.remaining_length = ord(response[1])

            return 1

        else:
            # Something is wrong...
            print("Something went wrong")

            return 0

#-----------------------------------------------------------------------------------------

class DISCONNECT(object):

    def __init__(self):

        self.message_type = MSG_DISCONNECT

        self.DUP = 0
        self.QoS = 0
        self.retain = 0

    def fixed_header(self):

        header = self.message_type

        if self.DUP == 1: header = header or 0x08

        if self.QoS == 1: header = header or 0x02
        elif self.QoS == 2: header = header or 0x04
        elif self.QoS == 3: header = header or 0x06

        if self.retain == 1: header = header or 0x01

        return chr(header)

    def assemble(self):

        message = (self.fixed_header() + "\x00")

        return message

#=========================================================================================

def format_length(length):
    remaining_length_string = ''
    while length>0 :
        digit = length % 128
        length /= 128
        if length>0:
            digit |= 0x80
        remaining_length_string += chr(digit)
    return remaining_length_string

#=========================================================================================

class Client(Thread):

    def __init__(self, client_id):
        # Constructor.
        Thread.__init__(self)

        print(client_id + " - Initialising")

        self.client_id = client_id

        self.sock = ''
        self.connected = False

        self.keep_alive = 0
        self.last_transmission_time = time.time()


    def connect(self,address="iot.eclipse.org", port=1883, keep_alive=60 ):

        print(self.client_id + " - Connecting to "+str(address)+" on port "+ str(port))

        self.keep_alive = keep_alive

        # Create a socket connection to the server and connect to it
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Try connect to the socket
        try:
            self.sock.connect((address, port))

            # Send a CONNECT messagew
            self.sock.send(CONNECT(client_id = self.client_id, keep_alive=self.keep_alive).assemble())
            rsps = self.sock.recv(100)

            # Print the response message
            response = CONNACK().parse(response = rsps)[1]
            print(str(self.client_id)+" - "+str(response))
            # TODO this should rather execute only if the parsed message is correct
            self.last_transmission_time = time.time()

            self.connected = True

        except socket.gaierror:
            #We couldn't make a connection
            print("Got a problem here")



        return

    def disconnect(self):

        self.sock.send(DISCONNECT().assemble())
        self.connected = False

        return

    def publish(self,topic = 'testtopic/subtopic', payload = 'Hello World!', qos = 0):

        if self.connected is True:
            print(self.client_id+" - Publishing: "+topic+"["+payload+"]")
            try:
                self.sock.send(PUBLISH(topic = topic, payload = payload, qos = qos).assemble())
                self.last_transmission_time = time.time()

            except:
                print("Broken pipe...")
                raise
        else:
            print(self.client_id+" - Not connected. Publishing not possible")


    def run(self):
        if self.connected:

            # If we haven't transmitted a message within the keep alive time, send a PINGREQ
            if time.time() - self.last_transmission_time>self.keep_alive:
                print(self.client_id+" - Sending PINGREQ")

                try:
                    # Send PINGREQ message
                    self.sock.send(PINGREQ().assemble())
                    self.last_transmission_time = time.time()
                    resp = self.sock.recv(self.keep_alive)
                    self.last_transmission_time = time.time()
                except:
                    raise

                #Generate and try send a PINGREQ message here if we're connected
                if PINGRESP().parse(resp) is 1:
                    print(self.client_id+" - PINGREP received")

#=========================================================================================

class ClientManager(Thread):
    """The Client Manager allows us to run multiple MQTT clients"""

    def __init__(self):
        # Constructor.
        Thread.__init__(self)

        self.update_rate=1  # 1 second should do
        self.keep_alive_=0
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.client_directory = {} # This is an array of all our classes
        self.keep_alive_directory = {} # This is an array of all our classes
        self.sleep_directory = {} # This stores the last time that the class executed

    def create_client(self, client_id, server, port, keep_alive=60):

        # Add the new client to the clients dictionary
        self.client_directory[client_id] = Client(client_id=client_id)
        self.client_directory[client_id].setDaemon(True)
        self.client_directory[client_id].connect(address=server, port=port, keep_alive=keep_alive)

        self.keep_alive_directory[client_id] = keep_alive
        self.sleep_directory[client_id] = time.time()

        self.client_directory[client_id].start()
        print(client_id+" - Client created")

#        self.client_list.remove(client_id)

    def run(self):

        self.scheduler.enter(self.update_rate, 1, self.heartbeat, ())
        self.scheduler.run()
        # We go to an endless loop of heartbeat() running at the update_rate

    def heartbeat(self):
        self.scheduler.enter(self.update_rate, 1, self.heartbeat, ())

        for my_client in self.client_directory:
            # For each client, check if it's alive. If not, run it
            if not self.client_directory[my_client].isAlive():
                if time.time() - self.sleep_directory[my_client] >= self.keep_alive_directory[my_client]:
                    # This section should fire at the keep_alive frequency (+- 1second)

                    # Reset the sleep timer in the sleep directory
                    self.sleep_directory[my_client] = time.time()

                    # Run the heartbeat script
                    self.client_directory[my_client].run()

                #sys.stdout.write('.')


#If the server does not receive a message from the client within one and a half times the Keep Alive time period (the client is allowed "grace" of half a time period), it disconnects the client as if the client had sent a DISCONNECT message. This action does not impact any of the client's subscriptions. See DISCONNECT for more details.

#If a client does not receive a PINGRESP message within a Keep Alive time period after sending a PINGREQ, it should close the TCP/IP socket connection.

#=========================================================================================

if __name__ == "__main__":



    MQTT_SERVER = "iot.eclipse.org"
    MQTT_PORT = 1883

    import time


    try:

        CM = ClientManager()
        CM.create_client(client_id="bushveldlabs-Dev" , keep_alive=10, server="iot.eclipse.org", port=1883)

        CM.start()

        while True:
            # do some other stuff here

            # TODO add error handling when doing these publishes to make sure that the client id is actually in the list
            CM.client_directory['bushveldlabs-Dev'].publish(topic = 'testtopic/subtopic', payload = str(time.time()), qos = 0)
            time.sleep(20) # Publish every 10 seconds


    except(KeyboardInterrupt, SystemExit):
        print("Exiting")
