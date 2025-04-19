import json
import pandas as pd
import os
from datetime import datetime, timedelta
from config import Config
from significantLow import SignificantLow
from ticker import Ticker
from fibonnaci import FibonnaciBaseLine,FibonnaciLevel
from transaction import Transaction
from info import Info

#region load_file
def load_file(fileName):
    #region File Loading
    try:
        if not os.path.exists(fileName):
            raise FileNotFoundError(f"File {fileName} not found")
            
        df = pd.read_csv(fileName)
        print(f"Successfully loaded {fileName} with {len(df)} rows")
        return df
    except pd.errors.EmptyDataError:
        print(f"Error: File {fileName} is empty")
        return None
    except pd.errors.ParserError as e:
        print(f"Error parsing {fileName}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error loading {fileName}: {e}")
        return None
    #endregion
#endregion

#region initiate
def initiate(fileName):
    
    significantLowsList = []
    fibonnaciDict = None
    ticker = Ticker()
    config = None
    transactionsList = []
    info = Info()
    
    #region Config Loading
    try:
        config = Config().loadConfig()
        #print(config)
    except Exception as e:
        print(f"Error loading config: {e}")
        return
    #endregion
    
    #region File Loading
    df = pd.read_csv(fileName, usecols=[0,2,3], header=None)  # 0 = timestamp, 2 = High Price, 3 = Low Price  
    if df is None:
        print("Error: Data file not loaded")
        return
    
    #endregion
      
    #region Data Processing
    for idx, row in df.iterrows():
        checkSigLowAge = int(config["checkSigLowAge"])
        
        if checkSigLowAge == 1:
            ''' Check Age of Sig Low'''
            if len(significantLowsList) > 0:
                significantLowsList,resetLowestPrice = removeStaleSignificantLows(significantLowsList,row.iloc[0],config)

                if significantLowsList is None:
                
                    errorTicker = errorTicker(excelRow,timeStamp,"Error clearing out expired SL")
                    printInfo (errorTicker,None)
                    return
                else:
                    if resetLowestPrice:
                        ticker.lowestPrice = None
    
        
#        userInput = input()  
#        if userInput == 'q':
#            return     
        timeStamp = row.iloc[0]
        tmpCurrentHighPrice = float(row.iloc[1])
        tmpCurrentLowPrice = float(row.iloc[2])
        excelRow = idx + 1

        #region Fib
        #'''Fibonnaci logic'''
        #if fibonnaciDict is not None:
        #    fibonnaciDict = checkFibAction(fibonnaciDict,tmpCurrentLowPrice,excelRow,timeStamp,config)
        #endregion
        
        #region We have NO Significant Low Price and this is the first row
        if ticker.lowestPrice is None:
            
            ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, "New Low")
            
            if ticker is None:
                errorTicker = errorTicker(excelRow,timeStamp,"Error")
                printInfo(errorTicker,significantLowsList)
                return
            
            else:
                printInfo(ticker,significantLowsList)
                continue
        #endregion
        
        #region When the list of significant lows is empty
        if len(significantLowsList) == 0:
            
            if ticker.lowestPrice >= tmpCurrentLowPrice:
                
                ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, "New Low")
                
                if ticker is None:
                    errorTicker = errorTicker(excelRow,timeStamp,"Error")
                    printInfo(errorTicker,significantLowsList)
                    return
                
                printInfo(ticker,significantLowsList)
                continue
            
            if ticker.lowestPrice < tmpCurrentLowPrice:
                
                tmpMarker = int(config['significantLowMarker'])
                
                difference = tmpCurrentLowPrice - ticker.lowestPrice
                
                if difference < tmpMarker:
                    '''I should update ticker even though it doesnt matter ...'''
                    ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, None)
                    
                    if ticker is None:
                        errorTicker = errorTicker(excelRow,timeStamp,"Error")
                        printInfo(errorTicker,significantLowsList)
                        return
                    
                    printInfo(ticker,significantLowsList)
                    
                    continue
                
                if difference >= tmpMarker:
                    
                    significantLow = setSignificantLow(timeStamp, ticker.lowestPrice)
                    
                    if significantLow is None:
                        errorTicker = errorTicker(excelRow, timeStamp, "Error setting Sig Low")
                        printInfo(errorTicker, None)
                        return
                    
                    significantLowsList.append(significantLow)
                    
                    fibonnaciBaseline = setFibonnaciBaseLine(tmpCurrentLowPrice,timeStamp,excelRow)
                    
                    if fibonnaciBaseline is None:
                        errorTicker = errorTicker(excelRow, timeStamp, "Error setting Fib baseline")
                        printInfo(errorTicker, None)
                        return
                    else:
                        fibonnaciDict = {}
                        fibonnaciDict['baseline'] = fibonnaciBaseline
                        #fibonnaciDict = addFibLevels(fibonnaciDict,timeStamp)
                    
                    ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, "Found Sig Low")
                    
                    if ticker is None:
                        errorTicker = errorTicker(excelRow,timeStamp,"Error")
                        printInfo(errorTicker,None)      
                        return
                    
                    printInfo(ticker,significantLowsList)
                    
                    continue
        #endregion
    
        #region When there is something in the significant low list
        if len(significantLowsList) > 0:
            
            slPrice = significantLowsList[0].price
            
            if (tmpCurrentLowPrice < slPrice):
                
                if significantLowsList[0].downBreach == False or significantLowsList[0].downBreach is None:
                    
                    sigLowDownwardsBreach = isDownwardsBreach(config,slPrice,tmpCurrentLowPrice,timeStamp,excelRow)
                    
                    if sigLowDownwardsBreach is None:
                        errorTicker = errorTicker(excelRow,timeStamp,"Error while confirming downward breach")
                        printInfo(errorTicker,None)
                        return
                    
                    if sigLowDownwardsBreach == -1:
                        
                        significantLowsList.clear()
                        
                        ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, "Flush")
                    
                        if ticker is None:
                    
                            errorTicker = errorTicker(excelRow,timeStamp,"Error")
                            printInfo(errorTicker,significantLowsList)
                            return
                
                        printInfo(ticker,significantLowsList)
                        
                        continue
                    
                    if sigLowDownwardsBreach == 0:
                        
                        ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, "No Downward Breach")
                    
                        if ticker is None:
                    
                            errorTicker = errorTicker(excelRow,timeStamp,"Error")
                            printInfo(errorTicker,significantLowsList)
                            return
                
                        printInfo(ticker,significantLowsList)
                        
                        continue
                    
                    if sigLowDownwardsBreach == 1:
                        
                        significantLowsList[0].downBreach = True
                        
                        ticker = updateTicker(slPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, "Downward Breach Confirmed")
                        
                        if ticker is None:
                    
                            errorTicker = errorTicker(excelRow,timeStamp,"Error")
                            printInfo(errorTicker,significantLowsList)
                            return
                
                        printInfo(ticker,significantLowsList)
                        
                        continue
                        
            
            if (tmpCurrentLowPrice > slPrice):
                
                if significantLowsList[0].downBreach == False or significantLowsList[0].downBreach is None:
                         
                    ticker = updateTicker(slPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, "Continue")
                
                    if ticker is None:
                    
                        errorTicker = errorTicker(excelRow,timeStamp,"Error")
                        printInfo(errorTicker,significantLowsList)
                        return
                
                    printInfo(ticker,significantLowsList)                
                    
                    continue
                
                if significantLowsList[0].downBreach == True:
                    
                    sigLowUpwardBreach = isUpwardsBreach(config,slPrice,tmpCurrentLowPrice,timeStamp,excelRow)
                    
                    if sigLowUpwardBreach is None:
                        errorTicker = errorTicker(excelRow, timeStamp, "Error looking for Upwards Breach")
                        printInfo(errorTicker,significantLowsList)
                        return
                    
                    if sigLowUpwardBreach == False:
                        
                        ticker = updateTicker(slPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"Continue looking for Upward Breach")
                        
                        if ticker is None:
                            
                            errorTicker = errorTicker(excelRow,timeStamp,"Error")
                            printInfo(errorTicker, None)
                            return
                        
                        printInfo(ticker,significantLowsList)
                        
                        continue
                    
                    if sigLowUpwardBreach:
                        '''Buy'''
                        transaction = transact(excelRow,timeStamp,tmpCurrentLowPrice,config)
                        
                        if transaction is None:
                            
                            errorTicker = errorTicker(excelRow, timeStamp, "Error Buying")
                            printInfo(errorTicker,None)
                            return
                        
                        transactionsList.append(transaction)
                        
                        significantLowsList.clear()
                        
                        '''After buying, the SL is finished. Therefore we have to look for next SL; set lowestPrice to current low price'''
                        ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"After BUY; Looking for New SL")
                        
                        if ticker is None:
                            
                            errorTicker = errorTicker(excelRow,timeStamp,"Error")                            
                            printInfo(errorTicker,None)                            
                            return
                        
                        printInfo(ticker,None)

                        continue         
        #endregion 
    #endregion loop    

#region checkFibAction(fibonnaciDict,tmpCurrentLowPrice,excelRow,timeStamp)
def checkFibAction(fibonnaciDict,tmpCurrentLowPrice,excelRow,timeStamp,config):
        
        upperBound = int(config["fibOscillations"][1])
        lowerBound = int(config["fibOscillations"][0])    
 
        for key in fibonnaciDict.keys:
            
            if key.upper() == "BASELINE":
                continue
            
            fibPrice = fibonnaciDict[key]
            
            if abs(fibPrice - tmpCurrentLowPrice) <= upperBound:
                print()

#endregion


#region scaffold the Fib dict
def addFibLevels(fibonnaciDict,timeStamp):
    keys_to_add = ["Fib1", "Fib2","Fib3"]
    fibBase = fibonnaciDict['baseline']
    for key in keys_to_add:
        fibonnaciDict[key] = setFibLevels(fibBase,key,timeStamp)
    return fibonnaciDict
#endregion

#def setFibLevels
def setFibLevels(fibBasePrice,key,timeStamp):
    
    fibLevel = FibonnaciLevel()
    
    fibLevel.timeStamp
    
    if key.upper() == "FIB1":
        fibLevel.price = round(fibBasePrice * 0.382,2)
        
    if key.upper() == "FIB2":
        fibLevel.price = round(fibBasePrice * 0.50,2)

    if key.upper() == "FIB3":
        fibLevel.price = round(fibBasePrice * 0.618,2)
    
    return fibLevel
    
#endregion              
#region error ticker
def errorTicker(excelRow,timeStamp,message):
    ticker = Ticker()
    ticker.idExcel = excelRow
    ticker.timeStamp = timeStamp
    ticker.tickerComment = message
    return ticker
#endregion

#region removeStaleSignificantLows    
def removeStaleSignificantLows(significantLowsList,timeStamp,config):
    
    try:
        resetLowestPrice = False
        
        format_str = '%Y-%m-%d %H:%M'
        
        allowedAging = int(config['significantLowExpiryInMinutes'])
        
        createTimestamp = datetime.strptime(significantLowsList[0].timeStamp,format_str)
        
        timeStamp = datetime.strptime(timeStamp,format_str)
        
        time_difference = abs(timeStamp - createTimestamp)
        
        total_seconds = time_difference.total_seconds()
        
        difference_in_minutes = total_seconds / 60
        
        if difference_in_minutes >=allowedAging:
            significantLowsList.clear()
            resetLowestPrice = True
            print('Sig Low Found @', createTimestamp, ' is', difference_in_minutes, ' mins old and is expired')
        
        
        return significantLowsList, resetLowestPrice
     
    except Exception as ex:
        
        print ("Error in removeStaleSignificantLows @",timeStamp)
        
        return None             
#endregion

#region print ticker
def printInfo(ticker,significantLowsList):
    
    info =  Info()
    info.row = ticker.idExcel
    info.timestamp = ticker.timeStamp
    info.high = ticker.highPrice
    info.low = ticker.lowPrice
    info.lowestPrice = ticker.lowestPrice
    info.comment = ticker.tickerComment
    
    if significantLowsList is not None and len(significantLowsList)>0:
        print(f'Row Index: {info.row if info.row!= None else ""}, '
              f'@: {info.timestamp if info.timestamp != None else ""}, '
              f'Hi: {info.high if info.high != None else ""}, '
              f'Lo: {info.low if info.low != None else ""}, '
              f'Lowest: {info.lowestPrice if info.lowestPrice != None else ""}, '
              f'Diff: {abs(info.low - info.lowestPrice) if info.low != None else ""}, '
              f'SigLow: {significantLowsList[0].price if significantLowsList[0].price != None else ""}, '
              f'Down: {significantLowsList[0].downBreach if significantLowsList[0].downBreach != None else "False"}, '
              f'Up: {significantLowsList[0].upBreach if significantLowsList[0].upBreach != None else "False"}, '
              f'Comment: {info.comment if info.comment != None else ""}')
    else:       
        print(f'Row Index: {info.row if info.row!= None else ""}, '
              f'@: {info.timestamp if info.timestamp != None else ""}, '
              f'Hi: {info.high if info.high != None else ""}, '
              f'Lo: {info.low if info.low != None else ""}, '
              f'Lowest: {info.lowestPrice if info.lowestPrice != None else ""}',
              f'Comment: {info.comment if info.comment != None else ""}')           
#endregion

#region transact
def transact(excelRow,timeStamp,tmpCurrentLowPrice,config):
    
    try:
        transaction = Transaction()
        transaction.idExcel = excelRow
        transaction.timeStamp = timeStamp
        transaction.boughtAt = tmpCurrentLowPrice
        transaction.perUnitProfit = int(config["perUnitProfit"])
        transaction.soldAt = None
        transaction.units = int(config["unitsToBuy"])
        return transaction  
    except Exception as ex:
        print ("Error in transact on row",excelRow,"@ ",timeStamp)
        return None           
#endregion    

#region isUpwardsBreach
def isUpwardsBreach(config,significantLowPrice,tmpCurrentLowPrice,timeStamp,excelRow):
    '''time stamp and excel row are for error messages'''
    try:
        breachLowerLevel = int(config["sigLowBreach"][0])
        breachQuantity =  tmpCurrentLowPrice - significantLowPrice
    
        '''0 = no breach 1, = yes breach'''
        flag = False
    
        if breachQuantity >= breachLowerLevel:
            flag = True
    
        return flag
    
    except Exception as ex:
        print ("Error in isUpwardsBreach @",timeStamp, " on row:",excelRow)
        return None
#endregion

#region isDownwardsBreach    
def isDownwardsBreach(config,significantLowPrice,tmpCurrentLowPrice,timeStamp,excelRow):
    
    try:
        breachLowerLevel = int(config["sigLowBreach"][0])
        breachUpperLevel = int(config["sigLowBreach"][1])
        breachQuantity = significantLowPrice - tmpCurrentLowPrice
     
        '''0 = no breach 1, = yes breach, -1 = flush'''
        flag = 0
    
        if breachQuantity >= breachLowerLevel and breachQuantity <= breachUpperLevel:
            flag = 1
    
        if breachQuantity > breachUpperLevel: 
            flag = -1
    
        return flag
    except Exception as ex:
        print ("Error in isDownwardsBreach @",timeStamp, " on row:",excelRow)
        return None
#endregion    
    
#region setFibonnaciBaseLine
def setFibonnaciBaseLine(tmpCurrentLowPrice,timeStamp,excelRow):
    try:
    
        fibBaseline = FibonnaciBaseLine()
        fibBaseline.price = tmpCurrentLowPrice
        fibBaseline.timeStamp = timeStamp
        fibBaseline.idExcel = excelRow
        return fibBaseline
    
    except Exception as ex:
        print ("Error in setFibonnaciBaseLine at Excel row:",excelRow," @ timestamp:", timeStamp)
        return None

#endregion

#region setSignificantLow
def setSignificantLow(timeStamp,currentLowPrice):
    try:
        sigLow = SignificantLow()
        sigLow.timeStamp = timeStamp
        sigLow.price = currentLowPrice
        return sigLow
    except Exception as ex:
        print("Error in setSignificantLow @ timestamp:", timeStamp)
        return None
#endregion

#region updateTicker 
def updateTicker(lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,message):
    try:
        ticker = Ticker()
        ticker.highPrice = tmpCurrentHighPrice
        ticker.lowPrice = tmpCurrentLowPrice
        ticker.timeStamp = timeStamp
        ticker.idExcel = excelRow
        ticker.lowestPrice = lowestPrice
        ticker.tickerComment = message
        return ticker 
    except Exception as e:
        print("Error updating ticker for Excel Row:",excelRow," @Timestamp:",timeStamp)
        return None
#endregion    

#region main    
if __name__ == "__main__":
    
    fileName= '2024-10-2000Rows.csv'
    #fileName = '2024-Trim_wip.csv'
    initiate(fileName)
#endregion