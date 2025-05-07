import inspect
import json
import pandas as pd
import os
from datetime import datetime, timedelta,time
from config import Config
from significantLow import SignificantLow
from ticker import Ticker
from fibonnaci import FibonnaciLevel
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
    
    #region local variables
    # Create a time object for 9:20 AM
    openingTime = time(9, 20)    
    closingTime = time(16, 40)
    significantLowsList = []
    ticker = Ticker()
    transactionsList = []
    #endregion
    
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
        #print(row.iloc[0])
        timeStamp = datetime.strptime(row.iloc[0], "%Y-%m-%d %H:%M:%S") 
        #timeStamp = datetime.strptime(row.iloc[0], "%Y-%m-%d %H:%M")   
        tmpCurrentHighPrice = float(row.iloc[1])
        tmpCurrentLowPrice = float(row.iloc[2])
        excelRow = idx + 1
        
         #region Can we sell?
        '''Adding logic for selling'''
        try:
            if len(transactionsList)>0:
                transactionsList = sellPosition(transactionsList,tmpCurrentLowPrice)
        except Exception as ex:
            printException(ex,excelRow,timeStamp)
            '''Why continue and not return? If the algo doesnt sell right now, its ok... the magic is in finding signals for buying'''
            continue
        #endregion
        
        #region check for working hours
        '''We are setting working hours between 920 and 1640. Any time stamps outside this range will be ignored''' 
        try:
            if (config['activateWorkingHours']):
                if isInsideWorkingHours(timeStamp,openingTime,closingTime) == False:
                    
                    '''If there are any Significant Lows that are Active, I will get rid of them'''
                    clearSignificantLowList(significantLowsList)
                    
                    '''Ticker.lowestPrice will also be set to None, since that starts the hunt for a new Significant Low'''
                    ticker = updateTicker(None,None,None,None,None,None)
                    
                    continue
                
        except Exception as ex:
            printException(ex,excelRow,timeStamp)
            return
        #endregion
        

        
        #region Nothing is known at this point
        if ticker.lowestPrice is None:
            try:
                print("*** Starting Fresh ***")
                ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"New lowest price set @" + str(tmpCurrentLowPrice))
                printInfo(ticker, None)
            except Exception as ex:
                printException(ex,excelRow,timeStamp)
                return
            continue
        #endregion
        
        #region No Sig Low is known
        if len(significantLowsList) == 0:
            
            if tmpCurrentLowPrice <= ticker.lowestPrice:
                try:
                    ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"New lowest price set " + str(tmpCurrentLowPrice))
                    printInfo(ticker, None)
                except Exception as ex:
                    printException(ex,excelRow,timeStamp)
                    return
                continue
        

            if tmpCurrentLowPrice > ticker.lowestPrice:
                
                #howHigh = abs(tmpCurrentLowPrice - ticker.lowestPrice)
                howHigh = abs(tmpCurrentHighPrice - ticker.lowestPrice)

                if howHigh >= config["significantLowMarker"]:
                    '''Significant Low Found'''
                    try:
                        ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"Significant Low set @" + str(ticker.lowestPrice))
                        printInfo(ticker,None)
                    except Exception as ex:
                        printException(ex,excelRow,timeStamp)
                        return
                    
                    '''We will create a significant low object and add it to the sig low list'''  
                    try:
                        significantLow = setSignificantLow(timeStamp,ticker.lowestPrice,config['significantLowExpiryInMinutes'],'active')
                        significantLowsList.append(significantLow)                    
                    except Exception as ex:
                        printException(ex,excelRow,timeStamp)
                        return
                                            
                    continue
                else:
                    '''If marker is not broken, this branch executes'''  
                    try:
                        ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"Did not crack marker price") 
                        printInfo (ticker,None)
                    except Exception as ex:
                        printException(ex,excelRow,timeStamp)
                        return
                    continue                                                      
        #endregion No sig low is known
        
        #region Sig Low List is not empty
        if len(significantLowsList) > 0:
            
            '''Since we have a list of Significant Lows, we must loop through'''
            for eachSignificantLow in significantLowsList:
                try:
                    
                    '''If the state of Significant Low is either FLUSH or USED, continue'''
                    if eachSignificantLow.state == 'flush' or eachSignificantLow.state == 'used':
                        continue
                    
                    ''' get the price at which this sig low became active'''
                    slPrice = eachSignificantLow.price
                    slDownBreach = eachSignificantLow.downBreach
                
                    ''' This only occurs when tmpCurrentLowPrice is lower than Sig Low'''
                    if slPrice > tmpCurrentLowPrice:
                        if slDownBreach is None:
                            '''Coming here means this Sig Low has not been tested for a down breach yet or if it was, none was found'''
                            isDownBreachConfirmed = isDownwardsBreach(config,slPrice,tmpCurrentLowPrice)
                        
                            '''If flush i.e -1, set this significant low state to flush'''
                            if isDownBreachConfirmed == -1:
                                ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,'Significant Low set for $:' + str(slPrice) + ' is flushed')
                                eachSignificantLow.state = 'flush'
                                
                                '''Earlier I was setting both these to None ... however if I am using STATE as a check, it doesnt matter for these'''
                                #eachSignificantLow.downBreach = None
                                #significantLow.searchingForUpwardBreach = None
                                printInfo(ticker,None)
                                continue
                            
                            '''If downward breach is confirmed'''
                            if isDownBreachConfirmed == 1:
                                
                                '''Added this check after AB told me that the tmpCurrentLowPrice if lower than lowest, should become the new lowest'''
                                ticker = updateTicker(ticker.lowestPrice if ticker.lowestPrice < tmpCurrentLowPrice else tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, "Significant Low breached downwards in-range")
                                eachSignificantLow.downBreach = 1
                                eachSignificantLow.searchingForUpwardBreach = True
                                printInfo(ticker,None)
                                continue
                        
                        if slDownBreach == 1:
                            '''Coming here means the Sig Low has had an in range breach once'''
                            isDownBreachConfirmed = isDownwardsBreach(config,slPrice,tmpCurrentLowPrice)
                            
                            if isDownBreachConfirmed == -1:
                                ticker = updateTicker(tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,'Significant Low set for $:' + str(slPrice) + ' is flushed')
                                eachSignificantLow.state = 'flush'
                                printInfo(ticker,None)
                                continue
                                
                            if isDownBreachConfirmed == 1:
                                ticker = updateTicker(ticker.lowestPrice if ticker.lowestPrice < tmpCurrentLowPrice else tmpCurrentLowPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow, "Significant Low breached downwards in-range")
                                eachSignificantLow.downBreach = 1
                                eachSignificantLow.searchingForUpwardBreach = True
                                printInfo(ticker,None)
                                continue

                        
                    '''If slPrice is less than tmpCurrentPrice, means upwards motion may be happening'''
                    if slPrice < tmpCurrentLowPrice:
                        
                        if eachSignificantLow.downBreach is None:
                            '''This condition happens when right after Sig Low is found, the price keeps going up'''
                            ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice, tmpCurrentHighPrice, timeStamp,excelRow,'Nothing interesting')
                            printInfo(ticker, None)
                            continue
                            
                        if eachSignificantLow.searchingForUpwardBreach:
                            '''This condition gets triggered when Sig Low has had a down breach and now is completing the W'''
                            minUpwardBreach = config['sigLowBreach'][0]
                            if tmpCurrentLowPrice - slPrice < minUpwardBreach:
                                ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"Significant Low known; No downward breach yet" if significantLow.downBreach is None else "Significant Low breached downwards in-range; Looking for Upward motion")
                                printInfo(ticker, None)
                                continue
                            else:
                                '''We have a Buy opp here'''
                                print("\t\t\tRow Index: " + str(excelRow) + ". A Buy Opportunity")
                                ticker = updateTicker(ticker.lowestPrice,tmpCurrentLowPrice,tmpCurrentHighPrice,timeStamp,excelRow,"Significant Low has been used; Store it for later")
                                eachSignificantLow.upBreach = True
                                eachSignificantLow.state = 'used'
                                
                                '''Adding logic to process transactions'''
                                transactionsList= addTransaction(excelRow,timeStamp,tmpCurrentLowPrice,config)
                                printInfo(ticker,transactionsList)
                                continue
                                
                except Exception as ex:
                    printException(ex,excelRow,timeStamp)                       
                    return
                    
        #endregion  Sig Low List is not empty      

        #region temporary hack : before I loop back, I must clear out all sig low objects that are NOT active; else the len(significantLowList) == 0 will only execute once
        significantLowsList = refreshList(significantLowsList)
        #endregion temporary hack
        
    #endregion data processing

#region Sell Position
def sellPosition(transactionsList,tmpCurrentLowPrice):
    
    for eachTransaction in transactionsList:
        
        if eachTransaction.majorLotSold is None:
            '''This is the first time we are trying to sell this position; need to consider the 80 - 20 split'''
            if int(tmpCurrentLowPrice) >= int(eachTransaction.majorLotSellPrice):
                eachTransaction.majorLotSold = True
                print("Selling Alert ==> Units Sold | " + str(eachTransaction.majorLotUnits) + " | Sold At: " + str(eachTransaction.majorLotSellPrice))
        elif (eachTransaction.majorLotSold and eachTransaction.stopLossLotSold is None):
            if int(tmpCurrentLowPrice) <= int(eachTransaction.stopLossLotSellPrice):
                eachTransaction.stopLossLotSold = True
                eachTransaction.state = "sold"
                print("Selling Alert ==> Units Sold | " + str(eachTransaction.stopLossUnits) + " | Sold At: " + str(eachTransaction.stopLossLotSellPrice))      
    
    transactionsList = refreshTransactionsList(transactionsList)
    
    return transactionsList
    
#endregion

#region Refresh Transactions List
def refreshTransactionsList(transactionsList):
    
    tmpList = []
    
    for eachTransaction in transactionsList:
        
        if eachTransaction.state == 'active':
            tmpList.append(eachTransaction)
        else:
            continue

    return tmpList
#endregion


#region refresh list - remove all sig lows NOT active
def refreshList(significantLowsList):
    
    sigLowListTemp = []
    
    for eachSigLow in significantLowsList:
        
        if eachSigLow.state == 'flush' or eachSigLow.state == 'used':
            continue
        else:
            sigLowListTemp.append(eachSigLow)
        
    return sigLowListTemp

#endregion refresh list

#region Add Transaction v2
def addTransaction(excelRow,timeStamp,tmpCurrentLowPrice,config):
    
    sellStrategy = config['sellStrategy']
    perUnitProfit = config['perUnitProfit']
    totalUnitsBought = int(config['unitsToBuy'])
    tmpList = []
    
    transaction = Transaction()
    
    transaction.idExcel = excelRow
    transaction.timeStamp = timeStamp
    transaction.unitsBought = totalUnitsBought
    transaction.boughtAt = tmpCurrentLowPrice
    
    transaction.majorLotSellPrice = tmpCurrentLowPrice + int(perUnitProfit[0])
    transaction.majorLotUnits = totalUnitsBought * int(sellStrategy[0])/100
    
    transaction.stopLossLotSellPrice= tmpCurrentLowPrice + int(perUnitProfit[1])
    transaction.stopLossUnits = totalUnitsBought * int(sellStrategy[1])/100
    
    transaction.state = 'active'
    
    tmpList.append(transaction)
    
    return tmpList

#endregion

#region isDownwardsBreach    
def isDownwardsBreach(config,significantLowPrice,tmpCurrentLowPrice):
    
    breachLowerLevel = int(config["sigLowBreach"][0])
    breachUpperLevel = int(config["sigLowBreach"][1])
    breachQuantity = significantLowPrice - tmpCurrentLowPrice
     
    '''None = never tried before  1 = in-range -1 = flush'''
    flag = None
    
    if breachQuantity >= breachLowerLevel and breachQuantity <= breachUpperLevel:
        flag = 1
    
    if breachQuantity > breachUpperLevel: 
        flag = -1
    
    return flag
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

#region Print Ticker
def printInfo(ticker,transaction):
    
    print(f'Row Index: {ticker.idExcel if ticker.idExcel!= None else ""}, \t'
         f'@: {ticker.timeStamp if ticker.timeStamp != None else ""}, \t'
         f'Hi: {ticker.highPrice if ticker.highPrice != None else ""},\t '
         f'Lo: {ticker.lowPrice if ticker.lowPrice != None else ""}, \t'
         f'Lowest: {ticker.lowestPrice if ticker.lowestPrice != None else ""}\t',
         f'Comment: {ticker.tickerComment if ticker.tickerComment != None else ""}')
    
    if transaction is not None:
        for eachTransaction in transaction:
            print (f'\t\t\tBought #: {eachTransaction.unitsBought if eachTransaction.unitsBought != None else ""}, \t'
                f'@: {eachTransaction.boughtAt if eachTransaction.boughtAt != None else ""}, \t'
                f'Excel Row: {eachTransaction.idExcel}')          

#endregion
        
#region Update Ticker 
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

#region Print Exceptions
def printException(ex,excelRow,timeStamp):
    frame = inspect.trace()[-1]
    method_name = frame.function
    print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)
#endregion

#region Clear Significant Low List
def clearSignificantLowList(significantLowsList):
    
        return significantLowsList.clear()
        
#endregion
            
#region Is inside working hours
def isInsideWorkingHours(timeStamp,openingTime,closingTime):
    
    if timeStamp.time() > closingTime or timeStamp.time() < openingTime:
        return False
    return True

#endregion

#region main    
if __name__ == "__main__":
    
    fileName= '2024-09.csv'
    #fileName = '2024-Oct2nd.csv'
    #fileName = '2024-Trim_wip.csv'
    initiate(fileName)
#endregion