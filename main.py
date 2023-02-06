"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.ns import rm_ns_module
from mirutil.ns import update_ns_module

update_ns_module()
import ns

gdu = ns.GDU()
c = ns.Col()


def main() :
    pass

    ##
    gdsn2f = GitHubDataRepo(gdu.src_n2f)
    gdsn2f.clone_overwrite()

    ##
    dfa = gdsn2f.read_data()

    ##
    gdsmd = GitHubDataRepo(gdu.src_md)
    gdsmd.clone_overwrite()

    ##
    dfb = gdsmd.read_data()

    ##
    df = pd.concat([dfa , dfb] , axis = 0)

    ##
    df = df.drop_duplicates()

    ##
    gdt = GitHubDataRepo(gdu.trg)
    gdt.clone_overwrite()

    ##
    df_fp = gdt.local_path / 'data.prq'
    df.to_parquet(df_fp , index = False)

    ##
    msg = 'Updated by: '
    msg += gdu.slf

    ##
    gdt.commit_and_push(msg)

    ##
    gdsn2f.rmdir()
    gdsmd.rmdir()
    gdt.rmdir()
    
    rm_ns_module()

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
