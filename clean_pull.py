# Introduction

# Purpose: Provide an easy to change and understand top layer to pull.py for the collection of Aquarium data using the pydent API. 
# Each of the user inputs can be altered to change the user, Aquarium IP address, the Google Sheet the data will be placed in,
# the Google Sheet credential file, and the input file. The input.yaml file itself can be altered with desired users, protocols, outputs,
# costs, times, and desired error messages to be collected.

# MUST: have the input.yaml, credential file, and pull.py files in the same directory
# MUST: follow yaml markup guidelines (https://learn.getgrav.org/advanced/yaml)
# MUST: share the desired Google Sheet with the email produced in the credential file
# MUST: install yaml, pygsheets, pandas, datetime, pytz, dateutil, pydent, and oauth2client.service_account (all available via pip)

# Author: Melanie Quick
# Last Edited: August 16th, 2018
# DAMP Lab, BU

import pull
from pull import DAMPAqData

# User inputs
username = "username"
password = "password"
ip = "12.34"
sheet = "Your Google Sheet"
creds = "client_secret.json"
inputs = "inputs.yaml"
time = 14 # In days. Duration of time you want to collect, previous from the current date.

# Main calls
aqdata = DAMPAqData()
aqdata.login(username,password,ip)
print("Logged In")
aqdata.loadInputs(inputs)
print("Inputs Loaded")
aqdata.connectSheet(sheet,creds)
print("Sheet Connected")
aqdata.collectData(time)
print("Data Collected")