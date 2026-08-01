from ib_insync import IB, Contract, Option, Stock, Index, Future, util
from datetime import datetime 
import asyncio 

symbol = "SPX"
expiry = datetime(2026, 6, 17)
strike = 7500
right = 'C'

ib = IB()

ib.connect('127.0.0.1', 7496, clientId=3)

expiry_str = expiry.strftime('%Y%m%d')
dte = (datetime.strptime(expiry_str, '%Y%m%d').date() - datetime.now().date()).days
print(dte) 

try: 
    if symbol == "SPX" and dte <=30: # workaround to handle ambiguous contracts when expiry is near 
        # contract = Option(symbol, expiry_str, strike, right, 'SMART', tradingClass = "SPXW")
        contract = Index(symbol, 'CBOE', 'USD')
    else:
        contract = Option(symbol, expiry_str, strike, right, 'SMART')

    qualified_contracts = ib.qualifyContracts(contract)

except Exception as e:
    error_msg = str(e)
    if 'Ambiguous contract' in error_msg and symbol == 'SPX':
        contract = Option(symbol, expiry_str, strike, right, 'SMART', tradingClass = "SPXW")
        qualified_contracts = ib.qualifyContracts(contract)
    else:        
        qualified_contracts = []

print(qualified_contracts)
chains = ib.reqSecDefOptParams(contract.symbol, '', contract.secType, contract.conId)

# print(chains) 


chain = next(c for c in chains)

print(chain.expirations)
## _______________________________________________________________________________________________


# contract = Option(symbol, expiry_str, strike, right, 'SMART')

# # Container to capture async error information
# error_context = {"is_ambiguous": False}

# # 1. Define an error event handler to intercept TWS messages
# def onError(reqId, errorCode, errorString, contract):
#     if "Ambiguous contract" in errorString:
#         error_context["is_ambiguous"] = True
#         error_context["error_msg"] = errorString

# # Connect the handler
# ib.errorEvent += onError

# qualified_contracts = ib.qualifyContracts(contract)

# if not qualified_contracts and error_context["is_ambiguous"]:
#     print(f"Ambiguous contract error for {symbol} - trying with tradingClass SPXW")
#     contract = Option(symbol, expiry_str, strike, right, 'SMART', tradingClass = "SPXW")
#     qualified_contracts = ib.qualifyContracts(contract)

# print(qualified_contracts)
# print(error_context)

ib.disconnect() 