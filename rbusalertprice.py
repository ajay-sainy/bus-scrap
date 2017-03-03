import pyrebase
import json
import logging
logging.basicConfig()

class DataGenerator:
    not_found_error = "Not Found"        
    
    def get_red_bus_list(self,url):
        print("Fetching data from source")
        json_data = self.fetch_data(url)
        red_bus_list = []
        if json_data == self.not_found_error:
            return red_bus_list 
        for data in json_data["SRD"]:
            for r in data["RIN"]:
                for inv in r["InvList"]:
                    t_name = inv["Tvs"]
                    fare = float(inv["MinFare"])                
                    deptime = inv["DepTime"]
                    aseats = inv["NSA"]
                    rtId = inv["RtId"]
                    bus = RedBus(t_name,fare,deptime,aseats,rtId)
                    red_bus_list.append(bus)
       
        return red_bus_list

    def fetch_data(self,url):        
        from urllib.request import Request, urlopen  # Python 3        
        
        try:
            q = Request(url)        
            q.add_header('Referer', 'http://www.redbus.in/')
            response = urlopen(q).read().decode('utf8')
            return json.loads(response)
        except:
            print("Exception")
            return self.not_found_error
        
class Database:
    def fetch(self,provider):
        print("Fetching data from Database")
        return FireBase().fetch(provider)
    
    def push(self,provider,rtId,data):
        return FireBase().push(provider,rtId,data)
        
class FireBase:    
    config = {
          "apiKey": "AIzaSyACx7ouaXRJp8Wy66cfCE5TPxvMQqlKv5c",
          "authDomain": "busscrap-8746a.firebaseapp.com",
          "databaseURL": "https://busscrap-8746a.firebaseio.com",
          "storageBucket": "busscrap-8746a.appspot.com",
        }    
    database = None        
    user = None
    
    def __init__(self):
        firebase = pyrebase.initialize_app(self.config)        
        self.database = firebase.database()
        # Get a reference to the auth service
        auth = firebase.auth()        
        #token = auth.create_custom_token("kkjjhbhgb")                
        #self.user = auth.sign_in_with_custom_token(token)
        self.user = auth.sign_in_with_email_and_password('sainyajay@gmail.com', 'firebase@9229')
        
    def fetch(self,provider):
        rbus_list = [];
        try:
            for sbus in self.database.child(provider).get(self.user['idToken']).each():
                for b in sbus.val():
                    rbus_list.append(
                        RedBus(sbus.val().get(b).get("travels_name"),
                               sbus.val().get(b).get("fare"),
                               sbus.val().get(b).get("DepTime"),
                               sbus.val().get(b).get("available_seats"),
                               sbus.val().get(b).get("rtId")))
        except Exception as e:
            print("Exception "+str(e))
            return rbus_list
        return rbus_list
        
    def push(self,provider,rtId,data):
        return self.database.child(provider+'/'+str(rtId)).push(data,self.user['idToken'])

def get_changed_buses(oldbuses,newbuses):
        
    print("Comparing old and new buses")
    print("Old Bus Length - "+str(len(oldbuses)),"New Bust Length - "+str(len(newbuses)))
    '''1. if oldbuses is empty, return new buses because all are new'''
    if len(oldbuses)==0:
        insert_data("4-Mar-2017",newbuses)
        return []
    
    '''2. Create today's old bus dictionary'''
    old_bus_dict = {}
   
    for old in oldbuses:
        old_bus_dict[old.rtId] = old

    '''3. Traverse through new buses and check for below conditions
        a) if new bus exist in old dict and fare change, add it to changed_bus
        b) if new bus does not exist add it to changed_bus
    '''                 
    changed_buses = []
    for new in newbuses:
        try:
            if new.rtId in old_bus_dict:
                oldbus = old_bus_dict[new.rtId]
                if oldbus.fare != new.fare:
                    changed_buses.append(new)
            else:
                changed_buses.append(new)
        except:
            print("Exception occured")            
            
    '''4. Return changed_buses '''
    print("Total changed buses "+str(len(changed_buses)))
    return changed_buses;

def compare_and_alert(old_buses,new_buses):
    changed_buses = get_changed_buses(old_buses,new_buses)
    if(len(changed_buses)>0):
        insert_data("Updated",changed_buses)

def insert_data(date,buses):
    print("pushing {} data to database".format(str(len(buses))))
    for bus in buses:
        print(Database().push(date,bus.rtId,bus.__dict__))

class RedBus():
    travels_name = ""
    fare = 0
    provider = ""
    deptime = ""
    available_seats = 0
    rtId = ""
    
    def __init__(self,tname,fare,deptime,avseats,rtId):
        self.travels_name = tname
        self.fare = fare        
        self.provider = "redbus"
        self.deptime = deptime
        self.available_seats = avseats
        self.rtId = rtId
        
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()
       
@sched.scheduled_job('interval', minutes=30)
def main():    
    new_rbus_list = DataGenerator().get_red_bus_list("https://www.redbus.in/search/result?fromCity=130&toCity=313&doj=4-Mar-2017&src=Pune&dst=Indore");    
    
    old_rbus_list = Database().fetch("4-Mar-2017")    
   
    for o in old_rbus_list:
        print(o.available_seats)
    compare_and_alert(old_rbus_list ,new_rbus_list );
    
    #changed_buses = get_changed_buses(old_rbus_list,new_rbus_list)

sched.start()
main()