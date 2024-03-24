from globals import BLOCK_DIR, NEWEST, IGNORE
import time 

from blockchain_parser.blockchain import Blockchain
from tqdm import tqdm 

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
    db = dict() 
    bc = Blockchain(f'{BLOCK_DIR}/blk{str(fnum).zfill(5)}.dat')

    for block in bc.get_unordered_blocks():
        for tx in block.transactions:
            for i,out in enumerate(tx.outputs):
                if out.type in IGNORE:
                    continue 

                key = f'{tx.txid}:{i}'
                val = out.addresses[0].address

                db[key] = val 

    return db 

def test_output_mapper():
    st = time.time()
    db = build_output_to_wallet_job(10)
    uq = set(list(db.values()))
    print(f'{len(db)-len(uq)}/{len(db)} addresses seen only once')
    print(f"Runtime: {time.time()-st}s")

    '''
    Output: 

    434882/655745 addresses seen only once
    Runtime: 21.117431640625s
    '''

if __name__ == '__main__':
    test_output_mapper()