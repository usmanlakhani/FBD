import inspect
import json
import pandas as pd
import os
from datetime import datetime, timedelta,time
from config import Config
from fibonnaci import FibonnaciLevel
from transaction import Transaction


#region Initiate
def initiate(fileName):
    
    #region local variables
    # Create a time object for 9:20 AM
    openingTime = time(9, 20)    
    closingTime = time(16, 40)
    transactionsList = []
    fibonnaciLevel = FibonnaciLevel()
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

        try:
            timeStamp = datetime.strptime(row.iloc[0], "%Y-%m-%d %H:%M:%S")
        except Exception as ex: 
            timeStamp = datetime.strptime(row.iloc[0], "%Y-%m-%d %H:%M")   
        
        tmpCurrentHighPrice = float(row.iloc[1])
        tmpCurrentLowPrice = float(row.iloc[2])
        excelRow = idx + 1
        
        '''To do: Add selling logic here'''
        
        '''To do: Add working hour check here'''
        
        '''Either this is the first time this is being done or existing Fib is null and void'''
        try:
            if fibonnaciLevel.timeStamp is None:
                '''Either I invalidated this object or this is the first time'''
                ''' Why am I passing zero for previous price? Because I need to compare it and None wont work'''
                fibonnaciLevel = configureFibObject(tmpCurrentHighPrice,tmpCurrentLowPrice,excelRow,'active',timeStamp,"New Fib Cycle")
                printInfo(fibonnaciLevel)
                continue
            
            if fibonnaciLevel.timeStamp is not None:
                
                if tmpCurrentHighPrice >= fibonnaciLevel.peak:
                    ''' Indicates the peak is yet to be found'''
                    ''' 1st: Update the Fib object to latest values'''
                    comment = "Peak Increased by " + str(abs(tmpCurrentHighPrice-fibonnaciLevel.peak))
                    fibonnaciLevel = configureFibObject(tmpCurrentHighPrice,\
                        tmpCurrentLowPrice if tmpCurrentLowPrice < fibonnaciLevel.bottom else fibonnaciLevel.bottom, \
                            excelRow,fibonnaciLevel.state,timeStamp,comment)
                    printInfo(fibonnaciLevel)
                    continue
                
                if tmpCurrentHighPrice < fibonnaciLevel.peak:
                    
                    ''' Indicates a down trend and potential Fib opportunity'''
                    
                    ''' Confirm the bottom'''
                    if tmpCurrentLowPrice < fibonnaciLevel.bottom:
                        fibonnaciLevel.bottom = tmpCurrentLowPrice
                    
                    '''Verify if the new tmpCurrentLowPrice makes a Fib level'''
                    fibLevel,isFibLevel = isNewPriceAtFibLevel(fibonnaciLevel.peak,fibonnaciLevel.bottom,tmpCurrentLowPrice)
                    if isFibLevel:
                        comment = "Fib Level set to " + str(fibLevel)
                        fibonnaciLevel = configureFibObject(fibonnaciLevel.peak,fibonnaciLevel.bottom,excelRow,fibonnaciLevel.state,timeStamp,comment)
                        printInfo(fibonnaciLevel)
                        continue
                    else:
                        comment = "Nothing of note"
                        fibonnaciLevel = configureFibObject(fibonnaciLevel.peak,fibonnaciLevel.bottom,excelRow,fibonnaciLevel.state,timeStamp,comment)
                        printInfo(fibonnaciLevel)
                        continue         
                    
        except Exception as ex:
            printException(ex,excelRow,timeStamp)
            return
        
        
        
    #endregion Data Processing
    
    
#endregion


#region Check if price level makes Fib
def isNewPriceAtFibLevel(peak,bottom, tmpCurrentLowPrice):
    
    fibonacci_ratios = [0.0, 0.236, 0.382, 0.500, 0.618, 0.786, 1.0]
    
    retracement_levels = {}
    
    for ratio in fibonacci_ratios:
        level = peak - tmpCurrentLowPrice
    
    return None, False

#endregion

#region Print Exceptions
def printException(ex,excelRow,timeStamp):
    frame = inspect.trace()[-1]
    method_name = frame.function
    print("Method:" + method_name + ", excelRow", excelRow, "@ timestamp", timeStamp,ex)
#endregion

#region Instantiate Fib Object
def configureFibObject(peak,bottom,excelRow,state,timeStamp,comment):
    
    tmpFib = FibonnaciLevel()
    
    tmpFib.idExcel = excelRow
    tmpFib.timeStamp = timeStamp
    tmpFib.comment = comment
    tmpFib.peak = peak
    tmpFib.bottom = bottom
    tmpFib.state = state
    
    return tmpFib

#endregion

#region Print Ticker
def printInfo(fibObject):
    
    print(f'Row Index: {fibObject.idExcel if fibObject.idExcel!= None else ""}, \t'
         f'@: {fibObject.timeStamp if fibObject.timeStamp != None else ""}, \t'
         f'Peak: {fibObject.peak if fibObject.peak != None else ""}, \t'
         f'Bottom: {fibObject.bottom if fibObject.bottom != None else ""}, \t'
         f'Fib Level: {fibObject.fibLevel if fibObject.fibLevel != None else ""}, \t'
         f'Comment: {fibObject.comment if fibObject.comment != None else ""}')         

#endregion


#region main    
if __name__ == "__main__":
    
    fileName= '2024-Oct2nd.csv'
    #fileName = '2024-Oct2nd.csv'
    #fileName = '2024-Trim_wip.csv'
    initiate(fileName)
    
#endregion