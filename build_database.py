from globals import BLOCK_DIR, BLOCK_IDX, IGNORE
import pickle 
import time 

from blockchain_parser.blockchain import Blockchain
from tqdm import tqdm 

def get_block_height():
    with open(BLOCK_IDX, 'r') as f:
        return [
            int(i) for i in 
            f.read().split('\n')[:-1]
        ]

def build_output_to_wallet_job(fnum):
    '''
    Runs per file. Can be parallelized.
    TODO Should probably use a real database for this
    Neo4j would be ideal. Make tx's relations e.g. 

    Use transaction_hash : transaction_index for uuid keys

    MERGE
        (tx:TX {value: tx.hash}) 
            -[:OWNED_BY]-> 
        (wallet:WALLET {value: tx.address})
    '''
    addr_db = dict() 
    value_db = dict()
    bc = Blockchain(f'{BLOCK_DIR}/blk{str(fnum).zfill(5)}.dat')

    for block in bc.get_unordered_blocks():
        for tx in block.transactions:
            for i,out in enumerate(tx.outputs):
                if out.type in IGNORE:
                    continue 

                key = f'{tx.txid}:{i}'
                val = out.addresses[0].address

                addr_db[key] = val 
                value_db[key] = out.value

    return addr_db, value_db

def build_graph(fnum, addr_db, value_db):
    db = dict() 
    bc = Blockchain(f'{BLOCK_DIR}/blk{str(fnum).zfill(5)}.dat')

    bh = get_block_height()
    num_blocks = bh[fnum+1]-bh[fnum]

    edges = []
    for block in tqdm(bc.get_unordered_blocks(), total=num_blocks):
        for tx in block.transactions:
            ins = []
            outs = []
            edge_weights = []
            
            for out in tx.outputs:
                if out.type in IGNORE:
                    continue 

                outs.append(out.addresses[0].address)
                edge_weights.append(out.value)


            in_weights = []
            uncertain = []
            for i,input in enumerate(tx.inputs):
                key = f'{input.transaction_hash}:{input.transaction_index}'
                maybe_addr = addr_db.get(key, key)
                
                # Ignore giving yourself change
                if maybe_addr in outs: 
                    idx = outs.index(maybe_addr)
                    outs.pop(idx)
                    edge_weights.pop(idx)

                    continue 

                if (maybe_weight := value_db.get(key, 0)) == 0:
                    uncertain.append(i)

                ins.append(maybe_addr)
                in_weights.append(maybe_weight)

            tot = sum(edge_weights)
            
            # Not sure why this happens..
            if tot == 0:
                continue 

            dif = tot - sum(in_weights)
            
            # Need to distribute remaining tx weight evenly 
            # across unknown tx inputs
            if uncertain:
                uncertain_weight = dif / len(uncertain)
                for i in uncertain:
                    in_weights[i] = uncertain_weight

            # Else dif should == 0

            # Spread weights evenly according to input amount
            for i in range(len(in_weights)):
                in_weights[i] /= tot 

            # Add edge from every input to every output 
            # (Not strictly correct, but no way to really seperate  
            # them if they're in  the same transaction)
            for i in range(len(ins)): 
                for j in range(len(outs)):
                    edges.append(
                        (ins[i], outs[j], str(int(in_weights[i] * edge_weights[j])))
                    )

    return edges

def test_output_mapper():
    st = time.time()
    db,_ = build_output_to_wallet_job(10)
    uq = set(list(db.values()))
    print(f'{len(db)-len(uq)}/{len(db)} addresses seen only once')
    print(f"Runtime: {time.time()-st}s")

    '''
    Output: 

    434882/655745 addresses seen only once
    Runtime: 21.117431640625s
    '''

def test_build_graph(start_db,end_db):
    '''
    addrs, vals = dict(), dict()
    
    # Build small, in-memory database of first 3 files
    for i in range(start_db,end_db):
        a,v = build_output_to_wallet_job(i)
        addrs = addrs | a 
        vals = vals | v

    # Save to disk so don't have to run above every time
    with open('dbs.pkl', 'wb+') as f:
        pickle.dump((addrs,vals), f)
    '''
    with open('dbs.pkl', 'rb') as f:
        addrs,vals = pickle.load(f)
        
    # Try to build a graph of just the last file 
    g = build_graph(end_db-1, addrs, vals)
    
    with open('subgraph.csv', 'w') as f:
        for row in g:
            f.write(','.join(row) + '\n')

if __name__ == '__main__':
    test_build_graph(0,3)