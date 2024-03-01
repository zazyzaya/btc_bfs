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

        n_in = tx['vin_sz']
        n_out = tx['vout_sz']

        if n_in > n_out:
            assert n_out == 1, f'Found weird tx:\n{json.dumps(tx,indent=1)}'
            d = tx['outputs'][0]['addresses'][0]

            # Need to distribute fee across all inputs. 
            # This is a bit rough, but its better than nothing
            fee = tx['fees']
            total_sent = tx['total'] + fee 
            
            for a in tx['inputs']:
                src.append(a['addresses'][0])
                dst.append(d)

                # Estimate fee
                w = a['output_value']
                w = int(w - (w/total_sent)*fee)

                weight.append(w)
                ts.append(t)

        elif n_out > n_in: 
            assert n_in == 1, f'Found weird tx:\n{json.dumps(tx,indent=1)}'
            s = tx['inputs'][0]['addresses'][0]

            for a in tx['outputs']:
                src.append(s)
                dst.append(a['addresses'][0])
                weight.append(a['value'])
                ts.append(t)

        else:
            assert n_in == 1 and n_out == 1, f'Found weird tx:\n{json.dumps(tx,indent=1)}'
            src.append(tx['inputs'][0]['addresses'][0])
            dst.append(tx['outputs'][0]['addresses'][0])
            weight.append(tx['outputs'][0]['value'])
            ts.append(t)

    return [src,dst], weight, ts


def parse_btc_com(wallet):
    resp = r.get(f'https://chain.api.btc.com/v3/address/{wallet}/tx').json() 
    resp = resp['data']

    src,dst,weight,ts = [],[],[],[]
    for tx in resp['list']:
        t = tx['block_height']

        n_in = tx['inputs_count']
        n_out = tx['outputs_count']

        if n_in > n_out:
            assert n_out == 1, f'Found weird tx:\n{json.dumps(tx,indent=1)}'
            d = tx['outputs'][0]['addresses'][0]
            
            # Need to distribute fee across all inputs. 
            # This is a bit rough, but its better than nothing
            fee = tx['fee']
            total_sent = tx['inputs_value']

            for a in tx['inputs']:
                src.append(a['prev_addresses'][0])
                dst.append(d)

                # Estimate fee
                w = a['prev_value']
                w = int(w - (w/total_sent)*fee)

                weight.append(w) 
                ts.append(t)

        elif n_out > n_in: 
            assert n_in == 1, f'Found weird tx:\n{json.dumps(tx,indent=1)}'
            s = tx['inputs'][0]['prev_addresses'][0]

            for a in tx['outputs']:
                src.append(s)
                dst.append(a['addresses'][0])
                weight.append(a['value'])
                ts.append(t)

        else:
            assert n_in == 1 and n_out == 1, f'Found weird tx:\n{json.dumps(tx,indent=1)}'
            src.append(tx['inputs'][0]['prev_addresses'][0])
            dst.append(tx['outputs'][0]['addresses'][0])
            weight.append(tx['outputs'][0]['value'])
            ts.append(t)

    return [src,dst], weight, ts


# Will keep increasing this, so when parallelized, can give
# to threadpool to use whichever API isn't busy 
AVAILABLE = [parse_blockcypher, parse_btc_com]

# Debugging
if __name__ == '__main__':
    wallets = [
        '1122NYbAT2KkZDZ5TFvGy4D2Ut7eYfx4en'
    ]

    print(parse_blockcypher(wallets[0]))