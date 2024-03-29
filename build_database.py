from globals import BLOCK_DIR, BLOCK_IDX, IGNORE, NEWEST

from joblib import Parallel, delayed
from blockchain_parser.blockchain import Blockchain
from tqdm import tqdm 

JOBS = 32

def get_block_height():
    with open(BLOCK_IDX, 'r') as f:
        return [
            int(i) for i in 
            f.read().split('\n')[:-1]
        ]

def build_wallet_map_from_file(fnum):
    '''
    Runs per file. Should be parallelized and results
    put into database of some sort. 
    '''
    db = dict()
    bc = Blockchain(f'{BLOCK_DIR}/blk{str(fnum).zfill(5)}.dat')

    for block in bc.get_unordered_blocks():
        for tx in block.transactions:
            for i,out in enumerate(tx.outputs):
                if out.type in IGNORE:
                    continue 

                key = f'{tx.txid}:{i}'
                addr = out.addresses[0].address
                db[key] = (addr, out.value)

    return db

def build_graph(fnum):
    db = dict() # TODO db should be connector to wallet_map db 
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

def build_wallet_map_database_job(f):
    db = build_wallet_map_from_file(f)

    # Database connector goes here:
    '''
    # Pseudocode: 
    setup.psql: 
        create database btc; 
        \connect btc

        create table if not exists tx (
            tx_id text, 
            wallet text, 
            value bigint,
            PRIMARY KEY (tx_id)
        );


    # Python part
    # MAKE SURE THIS IS THREAD-SAFE
    # Use a Lock() if necessary 

    cur = Cursor('btc', write=True)
    query = 'INSERT INTO tx (tx_id, wallet, value)\n'

    for k,(w,v) in db.items():
        query += f'({k},{w},{v}), '

    query += ';' 
    cur.execute(query)
    '''

def build_wallet_map_database():
    '''
    Spins up JOBS=32 processes that each parse one block file,
    and upload its contents to the database 
    '''
    Parallel(JOBS, prefer='processes')(
        delayed(build_wallet_map_database_job)(i) for i in range(NEWEST)
    )

if __name__ == '__main__':
    build_wallet_map_database()