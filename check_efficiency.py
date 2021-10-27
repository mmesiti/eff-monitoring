#!/usr/bin/env python3
import subprocess
import datetime
import pandas as pd
import numpy as np
from io import StringIO
from sys import argv, exit
from tabulate import tabulate
from allfields import get_all_fields, types


def get_args(argvs):
    '''
    Parse command line arguments.
    '''
    try:
        user = argvs[1]
        start = datetime.date.today() - datetime.timedelta(days=int(argvs[2]))
        end = datetime.date.today()
    except:
        print(f"Usage {argv[0]} user how-many-days-ago-start")
        print(f"e.g.: {argv[0]} s.michele.mesiti 7 # for a week ago")
        print(f"or: ")
        print(
                f"Usage {argv[0]} user how-many-days-ago-start how-many-days-ago-end"
                )
        exit()
    try:
        end = end - datetime.timedelta(days=int(argvs[3]))
    except:
        pass
    return user, start, end


def get_df_from_sacct(user, start, end):
    '''
    Query `sacct` and get a dataframe with all the job data.
    '''
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    cmd = f"sacct -u {user} --start {start_str} --end {end_str} --parsable2"
    format_string = ' --format ' + ','.join(get_all_fields())
    cmd = cmd + format_string
    output = subprocess.run(cmd.split(), capture_output=True)
    str_output = output.stdout.decode("utf-8")
    str_err = output.stderr.decode("utf-8")

    print("Command:")
    print(cmd)
    print("Error stream:")
    print(str_err)

    ss = StringIO(str_output)

    return pd.read_csv(ss, delimiter="|",dtype=types)


def reindex_df(df):
    '''
    Reindex dataframes using the JobID and the job step
    (from the JobID column).
    '''
    index = df.JobID.str.extract("(?P<JobID>\d+)\.?(?P<JobStep>.*)",
            expand=True)

    return (df[[col for col in df.columns if col != "JobID"]]  #
            .join(index)  #
            .set_index(["JobID", "JobStep"]))


def convert_totcpu(totcpu_series):
    '''
    Convert TotalCPU to timedelta.
    complicated because the timedelta format 
    of slurm and of pandas 
    do not  agree.
    '''

    units = ['days', 'hours', 'minutes', 'seconds']

    days_pattern = r"(?P<days>\d*)-"
    hours_pattern = r"(?P<hours>\d+):"
    days_hour_pattern = f"({days_pattern})?{hours_pattern}"

    minutes_pattern = r"(?P<minutes>\d+)"
    seconds_pattern = r"(?P<seconds>.*)"

    totcpu = totcpu_series.str.extract(
            f"({days_hour_pattern})?{minutes_pattern}:{seconds_pattern}",
            expand=True)[units]

    totcpu = totcpu.fillna(0)

    return sum((pd.to_timedelta(totcpu[unit].astype(float), unit=unit)
        for unit in units), pd.Timedelta(0))


def convert_cputime_raw(cputime_raw):
    return pd.to_timedelta(cputime_raw, unit='sec')


def add_efficiency_columns(df):
    '''
    Compute efficiency of single jobs
    and add it to the dataframe 
    as a new column.
    '''
    df['Efficiency'] = df.TotalCPU / df.CPUTimeRAW
    df["Effective CPUS"] = (df.Efficiency * df.NCPUS)
    return df

def good_jobsteps(index):
    def good_jobstep(jobstep):
        bad_strings = ['batch', 'extern']
        return (jobstep != '') and all(bs not in jobstep for bs in bad_strings)

    return [ jobstep for _,jobstep in index if good_jobstep(jobstep)]



def compute_global_efficiency(df):
    '''
    Compute the total average efficiency of all included jobs.
    '''
    # selecting only the main job step 
    jobsteps = ''
    # cpu_time
    actively_used = df.TotalCPU.loc[(slice(None), jobsteps)]
    consumed = df.CPUTime.loc[(slice(None), jobsteps)]

    actively_used_sum = actively_used.sum()
    consumed_sum = consumed.sum()

    efficiency = actively_used_sum / consumed_sum
    return efficiency

def compute_global_efficiency_v2(df):
    '''
    Compute the total average efficiency of all included jobs.
    '''
    # selecting only the main job step
    # literal '.'  must be escaped
    condition = ~df.JobIDRaw.str.contains('\.')
    # cpu_time
    actively_used = df.TotalCPU.loc[condition]
    consumed = df.CPUTime.loc[condition]

    actively_used_sum = actively_used.sum()
    consumed_sum = consumed.sum()

    efficiency = actively_used_sum / consumed_sum
    return efficiency


def save_csvs(df):
    '''
    Save dataframes to disk as PSQL-decorated CSV.
    Low efficiency jobs are saved in a different file.
    '''
    jobsteps = slice(None) #good_jobsteps(df.index)
    columns = [ 'Efficiency',  #           
            'NCPUS',  #      
            'Effective CPUS',  #               
            'ReqMem',  #       
            'ExitCode',  #         
            'JobName',  #        
            'Submit',  #       
            'Start',  #      
            'Elapsed'
            ]

    out_df = df.loc[(slice(None),''),columns]  # Only main jobs
    loweff_threshold = .6
    out_df_loweff = out_df.loc[out_df.Efficiency < loweff_threshold, :]
    out_df_all = df.loc[:,columns]  # Substeps as well

    for df, filename in [(out_df, f"eff_{user}.csv"),
                         (out_df_loweff, f"loweff_{user}.csv"),
                         (out_df_all, f"alleff_{user}.csv")]:
                         
        with open(filename, 'w') as f:
            f.write(
                    tabulate(df.reset_index().sort_values(by=["JobID", "JobStep"]),
                        headers='keys',
                        tablefmt='psql',
                        showindex=False))


if __name__ == "__main__":
    user, start, end = get_args(argv)
    df = get_df_from_sacct(user, start, end)
    df = reindex_df(df)
    # convert TimeRAW to timedelta
    df.TotalCPU = convert_totcpu(df.TotalCPU)
    df.CPUTime = convert_totcpu(df.CPUTime)
    df.CPUTimeRAW = convert_cputime_raw(df.CPUTimeRAW)

    # Compute efficiency
    df = add_efficiency_columns(df)
    print("Efficiency:", compute_global_efficiency_v2(df))
    save_csvs(df)
