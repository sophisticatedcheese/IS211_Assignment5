__author__ = 'TJeremiah October 2015'


import urllib2,argparse,csv

url= 'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'

"""Some help from Brooklyn College and examples used from text and other sources"""

""" This upcoming function was taken from the required reading. Interactivepython"""

class Queue:

    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0,item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

class Server:
    def __init__(self):
        self.current_task=None
        self.time_remaining=0
        self.time_elasped=0

    def tick(self):
        if self.current_task != None:
            self.time_remaining=self.time_remaining - 1
            if self.time_remaining <=0:
                self.current_task=None

    def busy(self):
        if self.current_task != None:
            return True
        else:
            False

    def start_next(self,new_task):
        self.current_task=new_task
        self.time_remaining=new_task.get_time()

class Request:

    def __init__(self,request):
        self.timestamp = int(request[0])
        self.timeleft  = int(request[2])

    def get_time(self):
        return self.timeleft

    def get_stamp(self):
        return self.timestamp

    def wait_time(self, current_time):
        return current_time - self.timestamp

"""Again, taken from the text book"""
class Queue:
    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0,item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

"""opens file and tries to connect. If there isnt one, return false"""
def dl_data(url):

        counter = 1

        while counter > 0:
            try:
                response = urllib2.urlopen(url)
                return response

            except urllib2.URLError:
                print 'Can not connect'
                counter -= 1
                continue
        return False


def get_max(in_file):

    numb_list = []
    for i in in_file:
        numb_list.append(i[0])

    return max(numb_list)

def url_read(file_name):

        in_file = csv.reader(file_name)

        parsed_in_file = []

        for row in in_file:
            row[0] = int(row[0])
            parsed_in_file.append(row)

        max_timestamp = get_max(parsed_in_file)
        file_dict = {x: [] for x in range(1, max_timestamp + 1)}
        for row in parsed_in_file:
            file_dict[row[0]].append(row)

        return file_dict

def simulateOneServer(request_file):
    server=Server(name=0)
    server_queue=Queue()

    waiting_times=[]
    req_num=1

    for current_second, bulk_request in request_file.iteritems():

        if bulk_request:


            for req in bulk_request:
                timestamp = int(req[0])
                process_time = int(req[2])

                request = Request(req_num, timestamp, request)
                server_queue.enqueue(request)
                req_num += 1

        if (not server.busy()) and (not server_queue.is_empty()):   
            next_request = server_queue.dequeue()
            waiting_times.append(next_request.wait_time(current_second))
            server.start_next(next_request)

        server.tick()
    average_wait = float(sum(waiting_times)) / len(waiting_times)
    print "Average Wait %6.2f secs %3d tasks remaining." % (average_wait, server_queue.size())

def simulateManyServers(request_file, size):

    server_list = [Server(name=x) for x in range(0, size)]
    server_queue = Queue()

    waiting_times = []
    req_num = 1

    for current_second, bulk_request in request_file.iteritems():

        for req in bulk_request:
            timestamp = int(req[0])
            process_time = int(req[2])

            request = Request(req_num, timestamp, process_time)
            server_queue.enqueue(request)
            req_num += 1

        for server in server_list:
            if (not server.busy()) and (not server_queue.is_empty()):
                next_request = server_queue.dequeue()
                waiting_times.append(next_request.wait_time(current_second))
                server.start_next(next_request)

            server.tick()

    average_wait = float(sum(waiting_times)) / len(waiting_times)
    print "Average Wait %6.2f secs %3d tasks remaining." % (average_wait, server_queue.size())



def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="url of csv file")
    parser.add_argument("--servers", help="number of servers (integer)")
    args = parser.parse_args()

    if args.file:
        try:
            raw_server_data = dl_data(args.file)
            if not raw_server_data:
                print "Cannot connect to server... try again later"

            else:
                csv_file = url_read(raw_server_data)

                if args.servers:
                    simulateManyServers(csv_file, int(args.servers))

                else:
                    simulateOneServer(csv_file)

        except ValueError:
            print 'url is not valid... Program closing'
    else:
        print "A url must be entered next to the --file flag..."

if __name__ == "__main__":
    main()

