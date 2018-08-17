# Introduction

# Purpose: Provide an additional layer to data collection from the Aquarium database to contain the more nuanced and confusing components of interacting with the Aquarium API and the Google Sheets API. Allows for a clean, surface layer (aquarium_clean_data_pull.py) for easy editing and alteration of user-supplied inputs.
 
# Author: Melanie Quick  
# Last Edited: August 18th, 2018  
# DAMP Lab, BU

# Imports
import yaml

import time
import json
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
from dateutil import parser

from pydent import AqSession, models, ModelBase
import pprint

import pygsheets
from oauth2client.service_account import ServiceAccountCredentials


# Data Collection Class

# Contains all the methods necessary to interface with the Aquarium API (pydent), Google Sheets API, and user-specified inputs from aquarium_clean_data_pull

# Goals:  
# Flexible user inputs from inputs.yaml and aquarium_clean_data_pull  
# Better structure for future understanding of the necessary steps of data collection from Aquarium
# Remove the necessity of fundamental changes in this file, and only in aquarium_clean_data_pull

# Methods:
#     - Initial Set-Up
#         -loadInputs
#         -login
#     - Google Sheets
#         -connectSheet
#         -createSheets
#         -findFirstEmptyRow
#         -writeData
#     - Filtering
#         -collectOperations
#         -createEmptyDict
#         -findKeys
#         -findTimeDelta
#     - Data Collection
#         -collectOperations
#         -createEmptyDict
#         -findKeys
#         -findTimeDelta

class DAMPAqData:
    
    # Resets all class variables with each new DAMPAqData() instance
    def __init__(self):
        self.USERS = []
        self.PROTOCOLS = []
        self.USER_KEYS = []
        self.PROTOCOL_KEYS = []
        self.HANDS_OFF_TIME = []
        self.OUTPUTS = []
        self.COSTS = []
        self.ERRORS = []
        self.session = None
        self.spreadsheet = None
        self.op_data = {}

    # ===================================================================================================== #
    
    # Functions related to initial set-up with user-specified inputs 
    # Includes: loadInputs(input_file) and login(user,password,IP)
    
    # ===================================================================================================== # 
    
    # Parses input .yaml file for the users, protocols, hands-off time of each protocol, and desired outputs
    # Creates class lists of each for later access
    # Used in aquarium_clean_data_pull with passed in name of .yaml file
    # MUST be a .yaml file
    # MUST be in same directory
    
    def loadInputs(self,input_file):
        with open(input_file) as stream:
            constants = yaml.load(stream)
        self.USERS = constants['users']
        self.PROTOCOLS = constants['protocols']
        self.HANDS_OFF_TIME = constants['times']
        self.OUTPUTS = constants['outputs']
        self.COSTS = constants['costs']
        self.ERRORS = constants['errors']
    
    # Accesses aquarium server and logs-in with supplied credentials and local Aquarium IP address
    # Times out after 60 minutes
    # Used in aquarium_clean_data_pull with passed in username, password, and IP address
    
    def login(self,user,password,IP):
        self.session = AqSession(user, password, "http://" + IP)
        self.session.User
        self.session.set_timeout(60)
    
    # ===================================================================================================== #
    
    # Functions related to Google Sheets
    # Includes: createSheets(), connectSheet(name,creds), findFirstEmptyRow(sheet), and writeData(protocol)
    
    # ===================================================================================================== #               
    
    # Supplies necessary credentials for access and use of pygsheets
    # Opens the specified worksheet and sets the class variable 'spreadsheet'
    # Requires sharing of the spreadsheet inside Google Sheets with the email inside the credential file
    # Credential file needs to be in same directory
    # Used in aquarium_clean_data_pull with passed user inputs of spreadsheet and credential files
    
    def connectSheet(self,name,creds):
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        client = pygsheets.authorize(service_file=creds)
        self.spreadsheet = client.open(name)
        self.createSheets()
        
    # Checks if a sheet within the worksheet exists with each protocol name
    # If not, creates a sheet with the title of the protocol for later access and writing
    # Used in connectSheet(name,creds)
    
    def createSheets(self):
        for protocol in self.PROTOCOLS:
            try: 
                self.spreadsheet.worksheet_by_title(protocol)
            except:
                self.spreadsheet.add_worksheet(protocol)
                
    # Finds the first empty row in the sheet to prevent overwriting previous data
    # Used in writeData(protocol)
    
    def findFirstEmptyRow(self,sheet):
        column = sheet.get_col(1)
        row = 0
        if column:
            try: 
                while column[row]:
                    row += 1
            except:
                sheet.add_rows(1)
                return(row)
        
        return(row+1)
    
    # Converts the class dictionary to a dataframe
    # Writes the dataframe to the last empty row in the protocol's spreadsheet
    # Updates the headers (row 1) to the current desired outputs
    # Used in collectData(time) for each protocol
    
    def writeData(self,protocol):
        df = pd.DataFrame(self.op_data)
        sheet = self.spreadsheet.worksheet("title",protocol) #Selects the sheet based on the current protocol
        sheet.update_row(1,self.OUTPUTS)
        row = self.findFirstEmptyRow(sheet)
        if not df.empty:
            sheet.set_dataframe(df,(row,1),copy_head = False)
        
            
    # ===================================================================================================== #
    
    # Functions related to the filtering of Aquarium data by user-specified inputs and their helper functions
    # User-specified inputs include desired users, protocols, outputs, and length of time collected
    # Includes: collectOperations(protocol_key,time), createEmptyDict(), findKeys(), and findTimeDelta(date)
    
    # ===================================================================================================== # 
                
    # Collects all operations within a specified protocol
    # Filters by user and length of time
    # Used in collectData(time)
    
    def collectOperations(self,protocol_key,time):
        op_type = self.session.OperationType.find(protocol_key)    #Finds the protocol/operation type object from the key. We need the object to find additional information.

        ops = (op for op in op_type.operations if op.user_id in self.USER_KEYS and op.jobs and op.jobs[-1].user_id) #Filters by user, elimates jobs without a technician, collects all operations in system
        
        #ops = (op for op in op_type.operations if op.user_id in self.USER_KEYS and op.jobs and op.jobs[-1].user_id and self.findTimeDelta(op.created_at) <= time) #Filters by user, elimates jobs without a technician, collects most recent operations

        return ops
    
    # Creates an empty dictionary with labels corresponding to desired outputs
    # Used to reset the collection dictionary for every protocol and reduce data storage
    # Used in collectData(time)
    
    def createEmptyDict(self):
        self.op_data = dict([(output, []) for output in self.OUTPUTS])        
    
    # Finds the Aquarium database keys for the users and protocols for faster access
    # Used in collectData(time)
    
    def findKeys(self):
        self.USER_KEYS = [self.session.User.find_by_name(user).id for user in self.USERS]
        self.PROTOCOL_KEYS = [self.session.OperationType.where({"name" : protocol, "deployed" : True})[-1].id for protocol in self.PROTOCOLS]
     
    # Finds the difference in time between the operation's creation date and today
    # Information is used to filter the list of operations for every protocol to the most recent (as user-specified)
    # Used in collectData(time)
    
    def findTimeDelta(self,date):
        tz = timezone('UTC')
        today = tz.localize(datetime.utcnow())
        
        op_date = parser.parse(date)
        op_date = op_date.astimezone(tz)
            
        dif = today - op_date
        return dif.days
            
    # ===================================================================================================== #
    
    # Functions related to collection of data from Aquarium server
    # Includes: appendData(key,value), collectData(time), findData(op,key), and findRuntime(op)
    
    # ===================================================================================================== # 
    
    # Appends the passed in data to the current dictionary holding information on a particular protocol
    # Used in findData(op,key) for every data value collected
    
    def appendData(self,key,value):
        self.op_data[key].append(value)
        
    # Key method for the collection of data and integration of helper methods
    # Uses: findKeys() (once), createEmptyDict() (Nx, N = number of protocols), collectOperations(protocol,time) (Nx) 
    # findData(operation,output) ((M*N)x, M = number of outputs * number of operations inside one protocol),
    # and writeData(protocol) (Nx)
    # Used in aquarium_clean_data_pull with user specified period of past time to collect from the current date
        
    def collectData(self,time):
        self.findKeys() 

        for p in self.PROTOCOL_KEYS:

            self.createEmptyDict()
            operations = self.collectOperations(p,time)

            for op in operations:
                for o in self.OUTPUTS:
                    self.findData(op,o)

            self.writeData(self.session.OperationType.find(p).name)
            
    # Finds the data value as specified by the current output ("key") and operation
    # Throws error if the current output is not a "known" output (i.e., the method of locating the value is not specified below)
    # Can pass in specific values as a 3rd parameter if desired
    # TODO: Make more programmatic/cleaner
    # Used in collectData(time)
        
    def findData(self,op,key,check=None):
        value = ""
        if check is None: #key = known
            if key == "Date":
                value = op.created_at
            if key == "ID":
                value = int(op.id)
            if key == "Protocol":
                value = op.operation_type.name
            if key == "Technician":
                value = self.session.User.find(int(op.jobs[-1].user_id)).name
            if key == "Status":
                value = op.status
                try:
                    if op.data_associations and value == "error":
                        for da in op.data_associations:
                            if da.key == "job_crash":
                                value = "crashed"
                            if da.key == "aborted":
                                value = "aborted"
                            if da.key == "canceled":
                                value = "canceled"
                except:
                    pass
            if key == "Error Message":
                try:
                    if op.data_associations and self.op_data["Status"][-1] != "done":
                        data = (da for da in op.data_associations if da.key in self.ERRORS)    
                        for da in data:
                            value = da.key
                except:
                    pass
            if key == "Job Size":
                value = len(op.jobs[-1].operations)
            if key == "Runtime":
                if self.op_data["Status"][-1] == "done":
                    value = self.findRuntime(op)
            if key == "Hands-off Time":
                if self.op_data["Status"][-1] == "done":
                    value = self.HANDS_OFF_TIME[self.PROTOCOLS.index(op.operation_type.name)]
            if key == "Hands-on Time":
                if self.op_data["Status"][-1] == "done" and self.op_data["Runtime"][-1]:
                    value = self.op_data["Runtime"][-1] - self.op_data["Hands-off Time"][-1]
            if key == "Hands-on Time/Job":
                if self.op_data["Status"][-1] == "done" and self.op_data["Runtime"][-1]:
                    value = self.op_data["Hands-on Time"][-1]/self.op_data["Job Size"][-1]
            if key == "Cost/Job":
                value = self.COSTS[self.PROTOCOLS.index(op.operation_type.name)]
            if key == "Total Cost":
                value = self.op_data["Job Size"][-1] * self.op_data["Cost/Job"][-1]
            if key == "Cost/Minute (Total)":
                if self.op_data["Status"][-1] == "done" and self.op_data["Runtime"][-1]:
                    value = self.op_data["Total Cost"][-1]/self.op_data["Runtime"][-1]
            if key == "Cost/Minute (Hands-on)":
                if self.op_data["Status"][-1] == "done" and self.op_data["Runtime"][-1]:
                    value = self.op_data["Total Cost"][-1]/self.op_data["Hands-on Time"][-1]
            if key == "Concentration Keyword":
                if op.outputs[-1] and op.outputs[-1].item and op.outputs[-1].item.data_associations:
                    for da in op.outputs[-1].item.data_associations:
                        if da.key == "concentration_keyword":
                                value = da.value
            if key == "White Colonies" and self.op_data["Protocol"][-1] == "Check Plate":
                if op.outputs[-1] and op.outputs[-1].item and op.outputs[-1].item.data_associations:
                    for da in op.outputs[-1].item.data_associations:
                        if da.key == "white_colonies":
                                value = da.value
            if key == "Blue Colonies" and self.op_data["Protocol"][-1] == "Check Plate":
                if op.outputs[-1] and op.outputs[-1].item and op.outputs[-1].item.data_associations:
                    for da in op.outputs[-1].item.data_associations:
                        if da.key == "blue_colonies":
                                value = da.value
                    
            if key in self.OUTPUTS:
                self.appendData(key,value)
            else:
                self.appendData(key,"")
                print(key + " is not a known data type. Must input additional collection parameter (i.e., op.id)")
        else:
            self.appendData(key,value)
     
    # Finds the runtime of the current operation's job based on the json provided by job.state
    # Localizes all times to UTC
    # Finds runtime as the difference between the first and last step's time stamps
    # Used in findData()
    
    def findRuntime(self,op):
        tz = timezone('UTC')
        
        time_json = json.loads(op.jobs[-1].state)
        
        runtime = ""
            
        try:
            start_time = time_json[0]['time']
            end_time = time_json[-2]['time']
        except:
            return runtime
        else:
            if type(start_time) is str:
                start = tz.localize(datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S+00:00'))
            else:
                start = datetime.fromtimestamp(int(start_time), utc_tz)
            if type(end_time) is str:
                end = tz.localize(datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S+00:00'))
            else:
                end = datetime.fromtimestamp(int(end_time), utc_tz)
                
            total_time = end - start
            runtime = total_time.days*1440 + total_time.seconds/60 #In minutes
                
            return runtime