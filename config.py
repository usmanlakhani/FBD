import json
import pandas as pd
import os

class Config:
    def __init__(self):
        self.config = self.loadConfig()

    def loadConfig(self):
        try:
            with open('config.json', 'r') as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"Error loading config: {e}")
            return None
            
            
