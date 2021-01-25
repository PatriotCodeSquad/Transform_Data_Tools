from decimal import Decimal 
from datetime import datetime
from pandas import DataFrame
from dateparser import parse
from functools import singledispatch
from dataclasses import dataclass
from typing import Any

@dataclass
class make_ints_in_list_float:
    cost: Any

@dataclass
class addtoint:
    inint: int
    
@dataclass
class CapitalizeString:
    instr: str 

@dataclass
class addtostring:
    instr: str
    
@dataclass
class addanoda:
    instr: str
        

def singledisptrans(obj,mapschema):
    """This Function Iterates through an Array and formats all values for MongoDB Import"""
    
    mapschema = mapschema
    
    @singledispatch
    def transformdict(obj):
        return obj
    
    @transformdict.register(dict)
    def _(obj):
        # Add if key in dict check here -- Else do standard type mapping
        for k,v in obj.items():
            if k not in mapschema:
                obj[k] = transformdict(v)
            elif isinstance(mapschema[k],list):
                for trans in mapschema[k]:
                    v = transformdict(trans(v))
                    obj[k] = transformdict(v)
            else:
                v = mapschema[k](v)
                #print(v)
                obj[k] = transformdict(v)
        
        return obj

    @transformdict.register(list)
    def _(obj):
        return [transformdict(it) for it in obj]

    @transformdict.register(DataFrame)
    def _(obj):
        return obj.applymap(transformdict).to_dict('records')[0]

    @transformdict.register(float)
    @transformdict.register(Decimal)
    @transformdict.register(int)
    def _(obj):
        return float("{:.2f}".format(obj))
    
    @transformdict.register(datetime)
    def _(obj):
        return obj.isoformat()

    @transformdict.register(make_ints_in_list_float)
    def _(obj):
        return [float("{:.2f}".format(ob)) for ob in obj.cost]

    @transformdict.register(CapitalizeString)
    def _(obj):
        return str(obj.instr).upper()
    
    @transformdict.register(addtostring)
    def _(obj):
        return str(obj.instr)+' Hi'
    
    @transformdict.register(addtoint)
    def _(obj):
        return obj.inint+100
    
    @transformdict.register(addanoda)
    def _(obj):
        return str(obj.instr)+' anoda'
    
    
    @transformdict.register(type(None))
    def _(obj):
        return ""

    return transformdict(obj)
