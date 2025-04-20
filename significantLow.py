import json
import pandas as pd
import os

class SignificantLow:
    def __init__(self):
        self.idExcel = None
        self.timeStamp = None
        self.price = None
        self.downBreach = None
        self.upBreach = None
        self.sigLowComment = None
        self.expiresOn = None
        self.state = None
        self.searchingForUpwardBreach = None
        