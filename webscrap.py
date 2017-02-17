import pyrebase
import json
import datetime as dt

def current_date():    
    return dt.datetime.now().strftime("%d-%b-%Y")

def current_time():
    return dt.datetime.now().strftime("%H:%M:%S")

class DataGenerator:
    not_found_error = "Not Found"    
    today_date = current_date()    
    #red_bus_url = "https://www.redbus.in/search/result?fromCity=130&toCity=313&doj="+today_date+"&src=Pune&dst=Indore"
    red_bus_url = "https://www.redbus.in/search/result?fromCity=130&toCity=313&doj=18-Feb-2017&src=Pune&dst=Indore"
        
    def get_red_bus_list(self):
        print("Fetching data from source")
        json_data = self.fetch_data(self.red_bus_url)
        
        red_bus_list = []
        if json_data == self.not_found_error:
            return red_bus_list 
                
        for data in json_data["SRD"]:
            for r in data["RIN"]:
                for inv in r["InvList"]:
                    t_name = inv["Tvs"]
                    fare = float(inv["MinFare"])                
                    deptime = inv["DepTime"]
                    bus = RedBus(t_name,fare,deptime)
                    red_bus_list.append(bus)

        return red_bus_list

    def fetch_data(self,url):    
        from urllib.request import urlopen
        try:
            response = urlopen(url).read().decode('utf8')                    
            return json.loads(response)
        except:
            return self.not_found_error

class RedBus():
    travels_name = ""
    fare = 0
    date = ""
    time = ""
    provider = ""
    deptime = ""
    
    def __init__(self,tname,fare,deptime):
        self.travels_name = tname
        self.fare = fare
        self.time = current_time()
        self.date = current_date()
        self.provider = "redus"
        self.deptime = deptime

class Database:
    def fetch(self,provider):
        print("Fetching data from Database")
        return FireBase().fetch(provider)
    
    def push(self,provider,data):
        return FireBase().push(provider,data)
        
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
                rbus_list.append(RedBus(sbus.val().get("travels_name"),sbus.val().get("fare"),sbus.val().get("DepTime")))
        except:
            return rbus_list
            
        return rbus_list
        
    def push(self,provider,data):
        return self.database.child(provider).push(data,self.user['idToken'])

def get_changed_buses(oldbuses,newbuses):
    print("Comparing old and new buses")
    print("Old Bus Length - "+str(len(oldbuses)),"New Bust Length - "+str(len(newbuses)))
    '''1. if oldbuses is empty, return new buses because all are new'''
    if len(oldbuses)==0:
        return newbuses
    
    '''2. Create today's old bus dictionary'''
    old_bus_dict = {}
    for old in oldbuses:        
        if old.date == current_date():            
            old_bus_dict[old.travels_name] = old

    '''3. Traverse through new buses and check for below conditions
        a) if new bus exist in old dict and fare change, add it to changed_bus
        b) if new bus does not exist add it to changed_bus
    '''                 
    changed_buses = []
    for new in newbuses:
        try:
            if new.travels_name in old_bus_dict:
                oldbus = old_bus_dict[new.travels_name]
                if oldbus.fare != new.fare and oldbus.deptime == new.deptime:
                    changed_buses.append(new)
            else:
                changed_buses.append(new)
        except:
            print("Exception occured")            
            
    '''4. Return changed_buses '''
    print("Total changed buses "+str(len(changed_buses)))
    return changed_buses;

from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes=1)
def main():
    print("main started")
    new_rbus_list = DataGenerator().get_red_bus_list();
    old_rbus_list = Database().fetch("redbus")    
    changed_buses = get_changed_buses(old_rbus_list,new_rbus_list)
    print("pushing {} data to database".format(str(len(changed_buses))))
    for bus in changed_buses:
        print(Database().push("redbus",bus.__dict__))

sched.start()