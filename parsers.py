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
        t = tx['block_height']

        n_in = tx['vin_sz']
        n_out = tx['vout_sz']
        in_tot = tx['total'] + tx['fees']

        in_addrs, in_weights = [],[]
        for inpt in tx['inputs']:
            in_addrs.append(inpt['addresses'][0])
            in_weights.append(inpt['output_value'])

        out_addrs, out_vals = [],[]
        for outpt in tx['outputs']:
            out_addrs.append(outpt['addresses'][0])
            out_vals.append(outpt['value'] / in_tot)

        for i in range(n_in):
            for j in range(n_out):
                src.append(in_addrs[i])
                dst.append(out_addrs[j])
                ts.append(t)
                weight.append(int(in_weights[i] * out_vals[j]))

    return [src,dst], weight, ts


def parse_btc_com(wallet):
    resp = r.get(f'https://chain.api.btc.com/v3/address/{wallet}/tx').json() 
    resp = resp['data']

    src,dst,weight,ts = [],[],[],[]
    for tx in resp['list']:
        t = tx['block_height']

        n_in = tx['inputs_count']
        n_out = tx['outputs_count']
        in_tot = tx['inputs_value']

        in_addrs, in_weights = [],[]
        for inpt in tx['inputs']:
            in_addrs.append(inpt['prev_addresses'][0])
            in_weights.append(inpt['prev_value'])

        out_addrs, out_vals = [],[]
        for outpt in tx['outputs']:
            out_addrs.append(outpt['addresses'][0])
            out_vals.append(outpt['value'] / in_tot)

        for i in range(n_in):
            for j in range(n_out):
                src.append(in_addrs[i])
                dst.append(out_addrs[j])
                ts.append(t)
                weight.append(int(in_weights[i] * out_vals[j]))

    return [src,dst], weight, ts


# Will keep increasing this, so when parallelized, can give
# to threadpool to use whichever API isn't busy 
AVAILABLE = [parse_btc_com] # , parse_blockcypher] #< Rate limited

# Debugging
if __name__ == '__main__':
    wallets = [
        '1122NYbAT2KkZDZ5TFvGy4D2Ut7eYfx4en'
    ]

    print(parse_blockcypher(wallets[0]))