#from dateutil.parser import parse as dt_parse
import json 
import requests as r 

def parse_blockcypher(wallet):
    resp = r.get(
        f'https://api.blockcypher.com/v1/btc/main/addrs/{wallet}/full'
    ).json()

    src,dst,weight,ts = [],[],[],[]
    for tx in resp['txs']:
        # Would prefer to use timestamps, but not all 
        # APIs provide this info. 
        #t = dt_parse(tx['recieved']).timestamp()
        #ts.append(t)
        t = tx['block_height']

        ns = 0
        for s in tx['inputs']:
            # Should always be len 1 but idk
            src += s['addresses']
            ns += len(s['addresses'])
        
        nd = 0 
        for d in tx['outputs']:
            nd += 1 
            dst.append(d['addresses'][0])
            weight.append(d['value'])

        # Add edge from source to other dst nodes
        # if needed 
        while nd > ns: 
            src.append(src[-1])
            ts.append(t)
            ns += 1

        if ns > nd: 
            print("Huh? Found weird transaction:")
            print(json.dumps(tx,indent=1))
            exit()

    return [src,dst], weight, ts


def parse_btc_com(wallet):
    resp = r.get(
        f'https://chain.api.btc.com/v3/address/{wallet}/tx'
    ).json() 
    resp = resp['data']

    src,dst,weight,ts = [],[],[],[]
    for tx in resp['list']:
        t = tx['block_height']

        ns = 0
        for s in tx['inputs']:
            src += s['prev_addresses']
            ns += len(s['prev_addresses'])

        nd = 0 
        for d in tx['outputs']:
            dst.append(d['addresses'][0])
            weight.append(d['value'])
            nd += 1 

        # Add edge from source to other dst nodes
        # if needed 
        while nd > ns: 
            src.append(src[-1])
            ts.append(t)
            ns += 1

        # TODO this isn't right. Need ability to 
        # track several incoming payments to 
        # a single node: 
        # ns > nd implies weights come from src transactions
        # nd >= ns implies weights come from dst transaction
        if ns > nd: 
            print("Huh? Found weird transaction:")
            print(json.dumps(tx,indent=1))
            exit()

    return [src,dst], weight, ts


# Will keep increasing this, so when parallelized, can give
# to threadpool to use whichever API isn't busy 
AVAILABLE = [parse_blockcypher, parse_btc_com]

# Debugging
if __name__ == '__main__':
    wallets = [
        '38Jc5gsauXPrFzA85EMUR88KG8qyVPcK52',
        '1122NYbAT2KkZDZ5TFvGy4D2Ut7eYfx4en'
    ]

    for i,parser in enumerate(AVAILABLE):
        print(parser(wallets[i]))