import decimal
import json
import numbers
import random
import socket
import time
import pymysql
import boto3
import argparse
import datetime
from datetime import datetime as dt
import pandas as pd
import json


def connect():
    client = boto3.client("secretsmanager", region_name="us-west-2")
    resp = client.get_secret_value(SecretId=DB_SECRET)
    creds = json.loads(resp['SecretString'])
    conn = pymysql.connect(
        host=DB_HOST,
        user=creds["username"],
        password=creds["password"]
    )
    return conn


def format_obs(obs: list, fmt='json', is_vip=False, eof=True):
    if fmt == 'json':
        # {"DMI001": {"202211191230": {"SNKT": 3.4, "TMPF": 70.2}, "202211191235": {"SNKT": 3.7, "TMPF": 70.2}}}
        # add vnum!
        # {"DMI001": {"202211191230": {"SNKT": {1: 3.4}, "TMPF": {1: 70.2}}, "202211191235": {"SNKT": {1: 3.7}, "TMPF": {1: 70.2}}}}
        head = "JSON,,,KEEP_UNITS\n"
        tail = "\n!EOF!" if eof else ''
        return "".join([head, json.dumps(obs), tail])
    elif fmt == 'ATMOS':
        atmos_str = ""
        yr_strs = [list(next(iter(x.values())).keys())[0] for x in obs]
        # stid = next(iter(obs.keys()))
        # just grab the first year string for now
        atmos_str += f"ATMOS,{yr_strs[0][:4]}"
        atmos_str += '' if not is_vip else ',VIP'
        atmos_str += "\n"
        # print(f"ATMOS header: \n {atmos_str} \n")
        ob0 = obs[0]
        # again just grab first and assume all have same parms
        unique_parms = [[list(ob[a].keys()) for a in ob.keys()] for k, ob in ob0.items()][0][0]
        # print(f'parms:{unique_parms}')
        # unique_parms = set([x for y in parms for x in y])
        atmos_str += f'PARM =  ' + ';'.join(unique_parms) + "\n\n\n"
        for parent_ob in obs:
            stid = next(iter(parent_ob.keys()))
            for ob in parent_ob[stid].items():
                dt_str = str(ob[0])
                dattim = f"{dt_str[2:8]}/{dt_str[-4:]}"
                atmos_str += "    ".join(['', stid, dattim] + [str(x[1]) for x in ob[1].values()]) + '\n'
        atmos_str += f"\n!EOF!"

        '''ATMOS,2022,VIP
         PARM =  PM25;FLOW;ITMP;INRH;ERRR
           MTMET  221109/1501     8888.00     2.00    48.38    28.00     0.00
           MTMET  221109/1502     8888.00     2.00    48.38    28.00     0.00
        !EOF!
        '''
        '''ATMOS,2019
         PARM =  PM25;FLOW;ITMP;INRH;ERRR
           MTMET  190225/1615     1.00     2.00    48.38    28.00     0.00
           MTMET  190422/1800     1.00     2.00    48.38    28.00     0.00
        !EOF!
        '''
        return atmos_str
    else:
        raise Exception("invalid format, should be 'json' or 'ATMOS'")


def rand_val(val_min, val_max):
    dec = max(get_decimals(val_min), get_decimals(val_max))
    dec = None if dec < 1 else dec
    val = round(random.uniform(val_min, val_max), dec)
    return val


def get_decimals(num: numbers.Number):
    if type(num) == int:
        return 0
    else:
        return abs(decimal.Decimal(str(num)).as_tuple().exponent)


def rand_var_val(metamoth: list, num_vars=1, num_obs=1, rmk=None):
    global prev_obs
    vargems = random.sample(metamoth, num_vars)
    # print([x['VARGEM'] for x in vargems])
    res = []
    for i in range(num_obs):
        ob = {}
        for vg in vargems:
            # print(vg)
            if args.rate_limit and prev_obs.get(vg['VARGEM']):
                pv = prev_obs.get(vg['VARGEM'])
                val = rand_val(pv-args.rate_limit, pv+args.rate_limit)
            else:
                val = rand_val(vg['MIN'], vg['MAX'])
            if vg['UNITS'] == 'code':
                val = int(val)
            ob[vg['VARGEM']] = {1: val}
            prev_obs[vg['VARGEM']] = val
        if rmk:
            ob['RMK'] = {1: rmk}
        res.append(ob)
    return res


def get_ob(stid: str, dattims: list, num_vars: int, metamoth, rmk=None):
    to_return = {}
    obs = rand_var_val(metamoth, num_vars, num_obs=len(dattims), rmk=rmk)
    for i, d in enumerate(dattims):
        to_return[d] = obs[i]
    return {stid: to_return}


def query_row_count(dattim_beg, dattim_end):
    connection = connect()
    # query DATA_2022 where DATTIM > dattim
    curs = connection.cursor(pymysql.cursors.DictCursor)
    curs.execute(f"use moth;")
    q_str = f"select count(*) from DATA_2023 where DATTIM >= {dattim_beg} and DATTIM <= {dattim_end};"
    curs.execute(q_str)
    count = curs.fetchall()[0]['count(*)']
    connection.close()
    return count


def query_ids(dattim_beg, dattim_end, id_field = "STATION_ID"):
    connection = connect()
    curs = connection.cursor(pymysql.cursors.DictCursor)
    curs.execute(f"use moth;")
    q_str = f"select {id_field} from DATA_2022 where DATTIM >= {dattim_beg} and DATTIM <= {dattim_end};"
    curs.execute(q_str)
    res = curs.fetchall()
    connection.close()
    return res

def map_ids(ids: list, to_name='ID', from_name='STID'):
    res = []
    if from_name == 'STID':
        for stid in ids:
            gen = next((obs for obs in station_meta if obs[from_name]==stid))
            res.append(gen[to_name])
    return res


def main(args):
    # params here for now
    global DB_HOST
    global DB_SECRET
    global POE_HOST
    global POE_PORT
    global prev_obs
    global station_meta
    prev_obs = {}
    DB_HOST = args.db_host
    DB_SECRET = args.db_secret # this works on all?
    POE_HOST = args.poe_host
    POE_PORT = args.poe_port
    # proceed
    conn = connect()
    curs = conn.cursor(pymysql.cursors.DictCursor)
    curs.execute(f"use metamoth;")
    curs.execute(f"select * from VARIABLE;")
    meta_var = curs.fetchall()
    curs.execute(f"select STID, ID, MNET_ID from STATION WHERE MNET_ID != 276 and ID < 100000;")

    station_meta = curs.fetchall()
    stids = [x['STID'] for x in station_meta]
    # filter parms (might want these later?)
    parm_skips = ['QFLG', 'UTIM', 'ITIM']
    meta_var = [x for x in meta_var if x['VARGEM'] not in parm_skips and x['MIN'] is not None and x['MAX'] is not None]

    if args.stream == 'ATMOS':
        # df = pd.DataFrame(columns=['dattim', 'row_diff'])
        # 60k - 80k unique stations per hour
        # avg of 3-5 vargems per obs
        # ~ 105,000,000 svv/obs per DAY - so 4375000 per hour
        # 1215 svv-obs per second from ~17 stations

        ## Real stats based on QC
        '''
        250 reports (station, dattim, multiple vars) per second
        so 250*5 = 1250 svv per second - just need to tweak the number of variables
        '''
        dattim_dt = dt.strptime(str(args.dt_start), "%Y%m%d%H%M")
        dattim_start = dattim_dt.strftime("%Y%m%d%H%M")
        dattim_offset = (dattim_dt + datetime.timedelta(days=5)).strftime("%Y%m%d%H%M")

        started_at = dt.utcnow()

        # offset by x days
        initial_row_count = query_row_count(dattim_start, dattim_offset)
        print(f' initial db row count >= {dattim_start} = {initial_row_count}')
        start = dt.now()
        runtime_minutes = args.runtime_minutes
        print(f'running for {runtime_minutes} minutes')
        # batch_size = int(args.rate)
        svv_per_second = args.rate_svv
        dattim_count = 1
        vargems = args.num_parms
        unique_stations_per_second = int(svv_per_second / dattim_count / vargems)
        print(f'{unique_stations_per_second} unique stations per second')
        sent_count = 0
        loop = 0

        while True:
            dattim_dt += datetime.timedelta(minutes=dattim_count)
            start_1 = dt.now()
            total_elapsed = start_1 - start
            if total_elapsed.total_seconds() > runtime_minutes * 60:
                break
            loop += 1
            # some stations
            stations = random.sample(stids, unique_stations_per_second)

            dattims = [(dattim_dt + datetime.timedelta(minutes=n)).strftime("%Y%m%d%H%M") for n in range(dattim_count)]
            obs = [get_ob(s, dattims, num_vars=vargems, metamoth=meta_var, rmk='ATMOS_flog') for s in stations]
            atmos_ob = format_obs(obs, fmt="ATMOS", is_vip=True)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            con_receipt = sock.connect((POE_HOST, POE_PORT))
            # send
            sock.sendall(str.encode(atmos_ob))
            sock.close()
            sent_count += len(stations) * dattim_count
            # print(f'sent count: {sent_count}')
            elapsed = dt.now() - start_1
            sleep_rand = random.uniform(0, 2)
            _sleep = max([0, sleep_rand - elapsed.total_seconds()])
            print(f'sleep: {_sleep}')
            time.sleep(_sleep)
            current_rows = query_row_count(dattim_start, dattim_offset)
            # df = pd.DataFrame(columns=['dattim', 'cpu', 'ram_pct', 'ram_gb', 'row_diff'])
            # df.append({'dattim': str(dt.now()), 'cpu': psutil.cpu_percent(1), 'ram_pct': psutil.virtual_memory()[2]
            #               , 'ram_gb': psutil.virtual_memory()[3] / 1000000000, 'row_diff': current_rows})
            # print(f'current db row count > {str(dattim_dt)} = {current_rows}')
            print(f'\nexpected: {initial_row_count + sent_count} vs. actual: {current_rows}')
            print(f'row diff = {initial_row_count + sent_count - current_rows}')
            print(f'elapsed time: {(dt.utcnow()-started_at).total_seconds()}')
        end = dt.now() - start
        print(f'end dattim: {dattim_dt.strftime("%Y%m%d%H%M")}')
        print(f'took : {end}')
        print(f'actual sent: {sent_count}')

        # finally
        time.sleep(5)
        print('\n after 5 seconds \n')
        current_rows = query_row_count(dattim_start, dattim_offset)
        print(f' db row count >= {str(dattim_start)} and <= {dattim_offset} = {current_rows}')
        print(f'expected: {initial_row_count + sent_count} vs. actual: {current_rows}')
        print(f'row diff = {initial_row_count + sent_count - current_rows}')

        # finally
        time.sleep(10)
        print('\n after 10 seconds \n')
        current_rows = query_row_count(dattim_start, dattim_offset)
        print(f' db row count >= {str(dattim_start)} and <= {dattim_offset} = {current_rows}')
        print(f'expected: {initial_row_count + sent_count} vs. actual: {current_rows}')
        print(f'row diff = {initial_row_count + sent_count - current_rows}')

    if args.stream == 'json':
        # | ID | SHORTNAME |
        # +-----+-----------+
        # | 276 | DMI |
        # stids = [x['STID'] for x in station_meta if x['MNET_ID'] == 276]
        dattim_dt = dt.strptime(str(args.dt_start), "%Y%m%d%H%M")
        dattim_start = dattim_dt.strftime("%Y%m%d%H%M")
        dattim_offset = (dattim_dt + datetime.timedelta(days=1)).strftime("%Y%m%d%H%M")
        initial_row_count = query_row_count(dattim_start, dattim_offset)
        print(f' initial db row count >= {dattim_start} = {initial_row_count}')
        print(f'starting at : {dattim_start}')

        start = dt.now()
        dattim_count = 1
        vargems = args.num_parms
        batch_size = args.num_obs
        batches = args.batches
        total_obs = batch_size*batches*dattim_count
        print(f'sending {total_obs} total rows and {batch_size * batches * dattim_count * vargems} sidvidvarnums')
        # now send in batches of 2,000
        for b in range(batches):
            ob_dict = {}
            dattim_dt = dattim_dt + datetime.timedelta(hours=1)
            rand_stids = random.sample(stids, k=batch_size)
            for stid in rand_stids:
                dattims = [(dattim_dt + datetime.timedelta(minutes=n)).strftime("%Y%m%d%H%M") for n in range(dattim_count)]

                base_ob = get_ob(stid, dattims, num_vars=vargems,
                                 metamoth=meta_var, rmk='json_flog')
                # json_ob = format_obs(base_ob, fmt='json', eof=False, is_vip=True)
                ob_dict = ob_dict | base_ob

            formatted_obs = format_obs(ob_dict, fmt='json', eof=True, is_vip=True)
            # print(formatted_obs)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((POE_HOST, POE_PORT))
            sock.sendall(str.encode(formatted_obs))
            sock.close()
        with open(f"json_{batch_size}_{dt.now().strftime('%Y%m%d%H%M%S')}.json", "w") as final:
            json.dump(json.dumps(ob_dict), final)
        end = dt.now() - start
        print(f'took {end.total_seconds()} seconds to submit {batch_size*dattim_count*batches} obs in {batches} batches')
        print(f'ended at : {dattim_dt.strftime("%Y%m%d%H%M")}')
        time.sleep(1)
        print('\n ... after 1 seconds \n')
        current_rows = query_row_count(dattim_start, dattim_offset)
        sent_count = batch_size*dattim_count*batches
        print(f'expected: {initial_row_count + sent_count} vs. actual: {current_rows}')
        print(f'row diff = {initial_row_count + sent_count - current_rows}')
        time.sleep(4)
        print('\n ... after 5 seconds \n')
        current_rows = query_row_count(dattim_start, dattim_offset)
        print(f'expected: {initial_row_count + sent_count} vs. actual: {current_rows}')
        print(f'row diff = {initial_row_count + sent_count - current_rows}')
        # more stuff for debugging id mismatch:
        # time.sleep(5)
        # print('\n ... after 10 seconds \n')
        # current_rows = query_row_count(dattim_start, dattim_offset)
        # print(f'expected: {initial_row_count + sent_count} vs. actual: {current_rows}')
        # print(f'row diff = {initial_row_count + sent_count - current_rows}')
        # stids_submitted = list(ob_dict.keys())
        # # ... this is bad and slow
        # ids_submitted = map_ids(stids_submitted)
        # db_station_ids = [x['STATION_ID'] for x in query_ids(dattim_start, dattim_offset)]




if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # Required positional argument
    parser.add_argument("stream", help="Required positional argument. options are [ATMOS, json]",
                        choices=['ATMOS', 'json'])

    # Optional argument
    parser.add_argument("-runtime_minutes", action="store", dest="runtime_minutes", type=int, help="runtime in minutes",
                        default=1)
    parser.add_argument("-rate_svv", action="store", dest="rate_svv", type=int, help="svv send rate per second",
                        default=1000)
    parser.add_argument("-num_obs", action="store", dest="num_obs", type=int,
                        help="number of rows to send per batch(json only)", default=1500)
    parser.add_argument("-batches", action="store", dest="batches", type=int,
                        help="number of batches of n observations to send", default=1)
    parser.add_argument("-dt_start", action="store", dest="dt_start", type=int,
                        help="beginning datetime for observations, '%Y%m%d%H%M'  ", default=202301060000)
    parser.add_argument("-num_parms", action="store", dest="num_parms", type=int,
                        help='Number of variables/vargems per obs', default=5)
    parser.add_argument("-db_host", action="store", dest="db_host", type=str,
                        help='Database Host Name/address', default="10.0.0.222")
    parser.add_argument("-db_secret", action="store", dest="db_secret", type=str,
                        help='Database secret string for aws secret manager', default="database/mothreplica/webapi")
    parser.add_argument("-poe_host", action="store", dest="poe_host", type=str,
                        help='POE Host Name/address', default='10.0.0.203') # dev3 py2 poe/dbinsert
    parser.add_argument("-poe_port", action="store", dest="poe_port", type=int,
                        help='POE port', default=8095)
    parser.add_argument("-rate_limit", action="store", dest="rate_limit", type=int,
                        help='limit delta between current and n-1 variable value to the following range +/- ')
    global args
    args = parser.parse_args()
    main(args)
