from globals import BLOCK_DIR, NEWEST

from blockchain_parser.blockchain import Blockchain
from tqdm import tqdm 


def build_index():
    heights =[0]
    height = 0
    for fidx in tqdm(range(NEWEST)):
        bc = Blockchain(f'{BLOCK_DIR}/blk{str(fidx).zfill(5)}.dat')
        for blk in bc.get_unordered_blocks():
            height += 1 

        heights.append(height)

    with open('blk_index.txt', 'w+') as f:
        for h in heights:
            f.write(f'{h}\n')

if __name__ == '__main__':
    build_index()