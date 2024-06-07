from pydantic import BaseModel,Field
from typing import Optional

class PrimaryValidationFields(BaseModel):
    corporation_id: str = Field(..., example="0x0A3BF741342744B043FFAA6EE07AB7270000")
    profit_center_id: Optional[str] = Field(None, example="0x6D08EDC0DEC39B8249C6441B9CA0697D0001")
    
    
class WeekData(PrimaryValidationFields):
    startdate: str = Field(...,example="2023-04-02")
    enddate: str = Field(..., example="2023-04-08")
    
class WeeklyData(PrimaryValidationFields):
    week_start_date:str = Field(...,example="2023-04-02")
    week_end_date:str = Field(...,example="2023-04-22")   

class MonthData(PrimaryValidationFields):
    year:str = Field(...,example="2023")
    month:str = Field(...,example="4")

class MonthlyData(PrimaryValidationFields):
    year:str = Field(...,example="2023")
    
class YearlyData(PrimaryValidationFields):
    years_selected:str = Field(...,example="2") 

#Basemodels for All Corps endpoints

class listOfCorporations(BaseModel):
    corporations: list = ["0x0A3BF741342744B043FFAA6EE07AB7270000","7212B912A3B481B54B3A6477D49256090000"]
    sheet: str = Field(...,example="adr")
        
class AllCorpWeekData(listOfCorporations):
    startdate: str = Field(...,example="2023-04-02")
    enddate: str = Field(..., example="2023-04-08")
    
    
class AllCorpWeeklyData(listOfCorporations):
    week_start_date:str = Field(...,example="2023-04-02")
    week_end_date:str = Field(...,example="2023-04-22")
    
    
class AllCorpMonthData(listOfCorporations):
    year:str = Field(...,example="2023")
    month:str = Field(...,example="4")
    
    
class AllCorpMonthlyData(listOfCorporations):
    year:str = Field(...,example="2023")
    
class AllCorpYearlyData(listOfCorporations):
    years_selected:str = Field(...,example="2")   
    
# latest upload file basemodel 

class latestUploadData(BaseModel):
    type:str = Field(...,example="Weekly")
    corporation_id:str = Field(...,example="0x0A3BF741342744B043FFAA6EE07AB7270000")
        
       