import sys
import os
import pyproc
import pandas as pd
import logging
from sqlalchemy import create_engine

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@db:5432/{os.getenv('DB_NAME')}",
    isolation_level='AUTOCOMMIT'
)


def extract_transform(host):
    # initiate object lpse
    lpse = pyproc.Lpse(host)

    # tarik paket lelang
    paket_lelang = lpse.get_paket_tender(start=0, length=100)

    columns = [
        'id_paket',
        'nama_tender',
        'k_l_pd_instansi',
        'tahapan_kualifikasi',
        'hps',
        'jenis_kualifikasi',
        'jenis_tender',
        'metode_pengadaan',
        'jenis_pengadaan',
        'versi_spse',
        'fg1',
        'fg2',
        'fg3',
        'fg4',
        'fg5',
        'fg6',
    ]

    df = pd.DataFrame(paket_lelang['data'], columns=columns)

    # clean tahapan
    df['tahapan_kualifikasi'] = df.tahapan_kualifikasi.str.replace('[...]', '').str.strip()
    
    # create_id
    df['id_k_l_pd_instansi'] = pd.factorize(df.k_l_pd_instansi)[0]
    df['id_tahapan_kualifikasi'] = pd.factorize(df.tahapan_kualifikasi)[0]
    df['id_jenis_kualifikasi'] = pd.factorize(df.jenis_kualifikasi)[0]
    df['id_jenis_tender'] = pd.factorize(df.jenis_tender)[0]
    df['id_metode_pengadaan'] = pd.factorize(df.metode_pengadaan)[0]
    df['id_jenis_pengadaan'] = pd.factorize(df.jenis_pengadaan)[0]

    # load to temp table
    df.to_sql('paket_tender_tmp', if_exists='replace', con=engine, index=False)


def load():
    df = pd.read_sql('paket_tender_tmp', con=engine)

    # load data paket tender
    df[[
        'id_paket',
        'nama_tender',
        'id_k_l_pd_instansi',
        'id_tahapan_kualifikasi',
        'id_jenis_kualifikasi',
        'id_jenis_tender',
        'id_metode_pengadaan',
        'id_jenis_pengadaan',
        'hps'
    ]].set_index('id_paket').to_sql('paket_tender', if_exists='replace', con=engine)

    # load data dim
    for dim_table in ['k_l_pd_instansi', 'tahapan_kualifikasi', 'jenis_kualifikasi', 'jenis_tender',
                      'metode_pengadaan', 'jenis_pengadaan']:
        df[[
            f'id_{dim_table}',
            dim_table
        ]].drop_duplicates().set_index(f'id_{dim_table}').to_sql(dim_table, if_exists='replace', con=engine)
            

def main():
    host = sys.argv[1]
    logging.info(f"Get LPSE data from {host}")

    logging.info(f"Extract and transform")
    extract_transform(host)

    logging.info("Load data to new table")
    load()


if __name__ == '__main__':
    main()
