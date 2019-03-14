import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import re
import os
from pathlib import Path
import json
import operator

##import nltk
##from nltk.corpus import stopwords
##set(stopwords.words('english'))

import difflib
import itertools


#import ssh
from paramiko import SSHClient
import paramiko


dateRegEx = "\\d{2}[/|-]\\d{2}[/|-]\\d{2}\\s{0,}\\d{2}[:]\\d{2}[:]\\d{2}"
errorRegEx = "\\d{0,}[/|-]\\d{0,}[/|-]\\d{0,}\\s{0,}\\d{0,}[:]\\d{0,}[:]\\d{0,}\\s{0,}\[\\w+\]:\\s{0,}ER\\w+\\s{0,}\\w+.*"
#errorStatusRegEx = "\\d{0,}[/|-]\\d{0,}[/|-]\\d{0,}\\s{0,}\\d{0,}[:]\\d{0,}[:]\\d{0,}\\s{0,}\[\\w+\]:\\s{0,}ER\\w+\\s{0,}\\w+:\\s{0,}\\w+:\\s{0,}\\w+ed"
errorStatusRegEx = "\\w+:\\s{0,}\\w+:\\s{0,}\\w+ed"
#successStatusRegEx = "\\w+:\\s{0,}\\w+\\s{0,}\\w+lly"

#errorStatusRegEx = "\\w+:\\s{0,}\\w+:\\s{0,}\\w+ed"

errorCuasedByRegEx = "caus\\w+\\s{0,}by.*"
dbnameRegEx = "db\\w+[:]\\w+"
tablenameRegeEx = "tab\\w+:\\w+"

regexNames = ["Activity Status","Database","Table","Error","Root Cause Analysis"]
logsearchRegEx = ["status","dbName","tableName","Error","Root Cause"] # To be read from JSON


regexList =[errorStatusRegEx,dbnameRegEx,tablenameRegeEx,errorRegEx,errorCuasedByRegEx]


def extractInfo(content):

    #actionSQL  = 
    
    lines = content.split("\n")
    words = content.split(" ")
    most = max(set(words), key=words.count)
    print#(most)
    sparkCount = content.lower().count("spark")
    hiveCount = content.lower().count("hive")

    
        
    errorjson = {}


    

    
        
    for index, exp in enumerate(regexList) :
        
        values = re.findall(exp.lower(),str(content).lower())
        val = list(set(values))
        
        if (val) :            
            if len(val) == 1:
                print#(val)
                stmsg = " ".join(val)                
                exp1 = "^"+str(logsearchRegEx[index])+":\\\s{0,}(\\\w+)"
                exp = "\""+str(logsearchRegEx[index])+":(.*)\""
                #print(stmsg.strip()+"------>"+exp+"--------->"+logsearchRegEx[index])
                if str(logsearchRegEx[index]).lower() in stmsg.lower():
                    print#(stmsg.split(":")[-1].strip().title())
                    errorjson[regexNames[index]] =stmsg.split(":")[-1].strip().title()
                
                
            else:
                
                subList = val[1:]
                threshold_ratio = 0.75
                ratios = []
                val.sort(key=len) 
                
                for value, value2 in itertools.combinations(val, 2):
                     ratio = difflib.SequenceMatcher(None, value, value2).ratio()
                     ratios.append(ratio)
                     if "Root Cause Analysis".lower() in str(regexNames[index]).lower() :
                         print#(value+">>>>"+value2+">>>>"+str(ratio))
                         if ratio >=0.40 and ratio <=0.47 :
                             print#(value+">>>>"+value2+">>>>"+str(ratio))
                             #errorjson[str(regexNames[index])] = value
                             print#("======================================")
                sortedRatios = sorted(set(ratios))
                print#(sortedRatios)

                     

                     
                if "Root Cause Analysis".lower() in str(regexNames[index]).lower() :
                    errorjson[str(regexNames[index])+" Msg1"] = val[0]
                    errorjson[str(regexNames[index])+" Msg2"] = val[1]#value
                    match = re.search("\[\\d{0,},\\s{0,}\\d{0,},\\s{0,}\\d{0,}\],\\D{2}", str(errorjson[str(regexNames[index])+" Msg1"]).lower())
                    errorjson["Action Param"] = match.group()[-2:]     
                    
                    
                    
                else:
                    dateVal =re.findall(dateRegEx.lower(),str(val).lower())
                    if dateVal :
                        errorjson["Activity Date"] = dateVal[0]                        
                    errorjson[str(regexNames[index])+" Msg1"] = val[0]
                    errorjson[str(regexNames[index])+" Msg2"] = val[1]#value

    if "Activity Status".lower() in str(errorjson.keys()).lower():
        if sparkCount > hiveCount :
            errorjson["Activity"] = "Spark"
        if sparkCount < hiveCount :
            errorjson["Activity"] = "Hive"
        if "Spark".lower() in  str(errorjson["Activity"]).lower() :
            errorjson["Action"] = "Spark Action"
        if "Hive".lower() in  str(errorjson["Activity"]).lower():
            errorjson["Action"] = "Hive SQL"
        print(json.dumps(errorjson, indent=1))

    

def readSource(sourceData,isTest):
    
    for path in sourceData:
        print("-----"+str(path))
        file = open(path, "r") 
        #print(file.read())
        extractInfo(str(file.read()))
        

    
def sourceConnector(excelDirPath,isTest):
    print(isTest)
    pathlist = Path(excelDirPath).glob('**/*')
    readSource(pathlist,isTest)



      
sourceConnector("D:/Projects/ML/LogError",False)
