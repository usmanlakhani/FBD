import inspect
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
    
    significantLow = None
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
        
        #region extracting value from row
        timeStamp = datetime.strptime(row.iloc[0], "%Y-%m-%d %H:%M")
        tmpCurrentHighPrice = float(row.iloc[1])
        tmpCurrentLowPrice = float(row.iloc[2])
        excelRow = idx + 1
        #endregion
        
        #region Significant Low Age Check
        '''Any Significant Low that is older than allowed time has to be ignored and removed'''
        try:
            if (config['checkSigLowAge'] and (significantLow is not None and significantLow.state is not None)):
                if (isActive(significantLow,timeStamp)):
                    ticker.lowestPrice = None
                    significantLow = None
                    print("\t\t\tSignificant Low has aged; Ignore and find new Significant Low")
        except Exception as ex: 
                frame = inspect.trace()[-1]
                method_name = frame.function
                print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)       
        #endregion Significant Low Age Check
        
        #region Is the price right to sell ?
        try:
            transactionsList = sell(transactionsList,tmpCurrentHighPrice)
        except Exception as ex:
            frame = inspect.trace()[-1]
            method_name = frame.function
            print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)  
        #endregion
        
        
        #region Nothing is known at this point
        if ticker.lowestPrice is None:
            try:
                ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"New lowest price set @" + str(tmpCurrentLowPrice))
                printInfo(ticker, None)
            except Exception as ex:
                frame = inspect.trace()[-1]
                method_name = frame.function
                print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)
                return
            
            '''Call printinfo here'''
            continue
        #endregion
        
        #region No significant low is known off
        if significantLow is None or significantLow.state is None:
            
            if tmpCurrentLowPrice <= ticker.lowestPrice:
                try:
                    ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"New lowest price set @" + str(tmpCurrentLowPrice))
                    '''Call printinfo'''
                    printInfo(ticker, None)
                except Exception as ex:
                    frame = inspect.trace()[-1]
                    method_name = frame.function
                    print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)
                    return
                continue
            
            if tmpCurrentLowPrice > ticker.lowestPrice:
                
                howHigh = abs(tmpCurrentLowPrice - ticker.lowestPrice)
                
                #region Marker check
                if howHigh >= config["significantLowMarker"]:
                    
                    '''Significant Low Found'''
                    try:
                        ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"Significant Low set @" + str(ticker.lowestPrice))
                        '''Call printinfo'''
                        printInfo(ticker, None)
                    except Exception as ex:
                        frame = inspect.trace()[-1]
                        method_name = frame.function
                        print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)
                        return
                    
                    try:
                        significantLow = setSignificantLow(timeStamp,ticker.lowestPrice,config['significantLowExpiryInMinutes'],'Active')
                        '''No need to call printinfo'''
                        #printInfo(ticker)
                        
                    except Exception as ex:
                        frame = inspect.trace()[-1]
                        method_name = frame.function
                        print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)                        
                        return
                    
                    continue

                else:
                    try:
                        ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"Did not crack marker price")
                        '''Call printinfo'''
                        printInfo(ticker, None)
                    except Exception as ex:
                        frame = inspect.trace()[-1]
                        method_name = frame.function
                        print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)                        
                        return
                    continue
                    
                #endregion Marker check      
        #endregion No significant low is known off
        
        #region If a significant low is known
        if significantLow.state == 'Active':
            
            slPrice = significantLow.price
            
            '''if downward breach has not occured and tmpCurrentLowPrice is less than significant low price, check for downward breach'''
            if slPrice > tmpCurrentLowPrice and (significantLow.downBreach is None or significantLow.downBreach == "in-range"):
                try:
                    isDownBreachConfirmed = isDownwardsBreach(config,slPrice,tmpCurrentLowPrice)
                
                    '''if flush'''
                    if isDownBreachConfirmed == "flush":
                        ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"Significant Low flushed out")
                        significantLow.state = None
                        significantLow.downBreach = None
                        printInfo(ticker, None)
                    
                    '''if breached'''
                    if isDownBreachConfirmed == "in-range":
                        ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, "Significant Low breached downwards in-range")
                        significantLow.downBreach = "in-range"
                        significantLow.searchingForUpwardBreach = True
                        printInfo(ticker, None)
                    
                except Exception as ex:
                    frame = inspect.trace()[-1]
                    method_name = frame.function
                    print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)                        
                    return
                continue
            
            if slPrice <= tmpCurrentLowPrice and (significantLow.downBreach is None or significantLow.downBreach == "in-range") and significantLow.searchingForUpwardBreach:
                try:
                    if tmpCurrentLowPrice - slPrice < config["sigLowBreach"][0]:
                        ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"Significant Low known; No downward breach yet" if significantLow.downBreach is None else "Significant Low breached downwards in-range; Looking for Upward motion")
                        printInfo(ticker, None)
                    elif tmpCurrentLowPrice - slPrice >= config["sigLowBreach"][0]:
                        '''This elif completely removes any need to call the isUpwardsBreach ... the method is unnecessary'''
                        print("\t\t\tRow Index: " + str(excelRow) + ". A Buy opportunity")
                        
                        ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"Resetting Significant Low; Lowest Price")
                        ''' No need to do significantLow.upBreach = True really. Once a buy is done, the Sig Low is useless and needs to be RESET TO NONE'''
                        significantLow.upBreach = True
                        significantLow = setSignificantLow(None,None,None,None)
                        
                        '''Adding logic to process transactions'''
                        transactionsList.append(addTransaction(excelRow,timeStamp,tmpCurrentLowPrice,config))
                        #printNewTransaction(transactionsList[len(transactionsList)-1])
                        printInfo(ticker,transactionsList[len(transactionsList)-1])
                        
                except Exception as ex:
                    frame = inspect.trace()[-1]
                    method_name = frame.function
                    print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)
                    return 
                continue
             
        #endregion If a significant low is known  
                
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

#region setFibLevels
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

#region isActive    
def isActive(significantLow,timeStamp):
    
    if significantLow.expiresOn<timeStamp:
        return True
        
    return False
#endregion remove stale sig lows        
    
#region print ticker
def printInfo(ticker,transaction):
    
    print(f'Row Index: {ticker.idExcel if ticker.idExcel!= None else ""}, \t'
         f'@: {ticker.timeStamp if ticker.timeStamp != None else ""}, \t'
         f'Hi: {ticker.highPrice if ticker.highPrice != None else ""},\t '
         f'Lo: {ticker.lowPrice if ticker.lowPrice != None else ""}, \t'
         f'Lowest: {ticker.lowestPrice if ticker.lowestPrice != None else ""}\t',
         f'Comment: {ticker.tickerComment if ticker.tickerComment != None else ""}')
    
    if transaction is not None:

        print (f'\t\t\tBought #: {transaction.units if transaction.units != None else ""}, \t'
            f'@: {transaction.boughtAt if transaction.boughtAt != None else ""}, \t'
            f'Will sell at: {transaction.willSellAt if transaction.willSellAt != None else ""},\t'
            f'Excel Row: {transaction.idExcel}')          

#endregion

#region print new transaction
def printNewTransaction(transaction):
    print(
            f'\t\t\tComment: {"Transaction Information"}',
            f'Row Index: {transaction.idExcel if transaction.idExcel!= None else ""}, \t'
            f'@: {transaction.timeStamp if transaction.timeStamp != None else ""}, \t'
            f'Hi: {transaction.boughtAt if transaction.boughtAt != None else ""},\t '
            f'Lo: {transaction.willSellAt if transaction.willSellAt != None else ""}, \t'
            f'Lowest: {transaction.units if transaction.units != None else ""}\t')      
#endregion

#region sell
def sell(transactionsList,tmpCurrentHighPrice):
    
    list = transactionsList
    
    if len(list) > 0:
        
        for eachTransaction in list:
            
            if eachTransaction.willSellAt <= tmpCurrentHighPrice and eachTransaction.wasSold is None:
                
                print('\t\t\tSelling ' + str(int(eachTransaction.units)) + ' units @ ' + str(int(tmpCurrentHighPrice)))
                
                eachTransaction.wasSold = True
    
    return list    
        
#endregion

#region transact
def addTransaction(excelRow,timeStamp,tmpCurrentLowPrice,config):
    transaction = Transaction()
    transaction.idExcel = excelRow
    transaction.timeStamp = timeStamp
    transaction.boughtAt = tmpCurrentLowPrice
    transaction.perUnitProfit = config["perUnitProfit"]
    transaction.willSellAt = transaction.boughtAt + transaction.perUnitProfit
    transaction.units = int(config["unitsToBuy"])
    return transaction            
#endregion    

#region isUpwardsBreach
def isUpwardsBreach(config,significantLowPrice,tmpCurrentLowPrice):
    '''time stamp and excel row are for error messages'''
    breachLowerLevel = int(config["sigLowBreach"][0])
    breachQuantity =  tmpCurrentLowPrice - significantLowPrice
    
    '''0 = no breach 1, = yes breach'''
    flag = False
    
    if breachQuantity >= breachLowerLevel:
            flag = True
    
    return flag
#endregion

#region isDownwardsBreach    
def isDownwardsBreach(config,significantLowPrice,tmpCurrentLowPrice):
    
    breachLowerLevel = int(config["sigLowBreach"][0])
    breachUpperLevel = int(config["sigLowBreach"][1])
    breachQuantity = significantLowPrice - tmpCurrentLowPrice
     
    '''0 = no breach 1, = yes breach, -1 = flush'''
    flag = None
    
    if breachQuantity >= breachLowerLevel and breachQuantity <= breachUpperLevel:
        flag = "in-range"
    
    if breachQuantity > breachUpperLevel: 
        flag = "flush"
    
    return flag
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
def setSignificantLow(timeStamp,lowestPrice,significantLowExpiryInMinutes,state):
    sigLow = SignificantLow()
    #sigLow.timeStamp = datetime.strptime(timeStamp, "%Y-%m-%d %H:%M")
    sigLow.timeStamp = timeStamp
    sigLow.price = lowestPrice
    
    if state is not None:
        #sigLow.expiresOn = datetime.strptime(timeStamp, "%Y-%m-%d %H:%M") + timedelta(minutes=significantLowExpiryInMinutes)
        sigLow.expiresOn = timeStamp + timedelta(minutes=significantLowExpiryInMinutes)
    
    sigLow.state = state
    return sigLow
#endregion

#region updateTicker 
def updateTicker(lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,message):
    ticker = Ticker()
    ticker.highPrice = tmpCurrentHighPrice
    ticker.lowPrice = tmpCurrentLowPrice
    ticker.timeStamp = timeStamp
    ticker.idExcel = excelRow
    ticker.lowestPrice = lowestPrice
    ticker.tickerComment = message
    return ticker 
#endregion    

#region main    
if __name__ == "__main__":
    
    fileName= '2024-10-2000Rows.csv'
    #fileName = '2024-Trim_wip.csv'
    initiate(fileName)
#endregion