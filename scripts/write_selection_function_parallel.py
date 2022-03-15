
import os
import sys

gal = sys.argv[1]
lsr = sys.argv[2]
rslice = sys.argv[3]
walltime = sys.argv[4]

print(gal, lsr, rslice)

in_dir = f'/scratch/05328/tg846280/FIRE_Public_Simulations/ananke_dr3/{gal}/lsr_{lsr}'
out_dir = f'/scratch/05328/tg846280/FIRE_Public_Simulations/ananke_dr3/{gal}_select/lsr_{lsr}'
rs_dir = f'/scratch/05328/tg846280/FIRE_Public_Simulations/ananke_dr3/{gal}_select/lsr_{lsr}/random_states'
rand_state_ls = 'save'
sf_data_dir = '/scratch/05328/tg846280/FIRE_Public_Simulations/ananke_dr3/selectionfunction_data'
submit_dir = f'/work2/08052/tg873515/stampede2/ananke_fire_gaia_dr3/submit_{gal}_lsr{lsr}_rslice{rslice}'

os.makedirs(out_dir, exist_ok=True)
os.makedirs(submit_dir, exist_ok=True)

in_file_temp = "lsr-{}-rslice-{}.{}-res7100-md-sliced-gcat-dr3.{}.hdf5" #lsr-0-rslice-0.m12i-res7100-md-sliced-gcat-dr3.0.hdf5
out_file_temp = "lsr-{}-rslice-{}.{}-res7100-md-sliced-gcat-dr3.{}.hdf5"
rs_file_temp = "lsr-{}-rslice-{}.{}-res7100-md-sliced-gcat-dr3.{}.pkl"

Njob = 10
cmd_template = 'python /work2/08052/tg873515/stampede2/ananke_fire_gaia_dr3/ananke_dr3/selection_function_ruwe.py --in-file {} --out-file {} --random-state {} {} --selection ruwe1p4 rvs --sf-data-dir {}'

sbatch_files = []
for ijob in range(Njob):
    print(ijob)
    out_file = os.path.join(out_dir, out_file_temp.format(lsr, rslice, gal, ijob))
    print(out_file)
    in_file = os.path.join(in_dir, in_file_temp.format(lsr, rslice, gal, ijob))
    rs_file = os.path.join(rs_dir, rs_file_temp.format(lsr, rslice, gal, ijob))
    cmd = cmd_template.format(in_file, out_file, rand_state_ls, rs_file, sf_data_dir)

    log_out_fn = f'out-{ijob}.out'
    log_err_fn = f'err-{ijob}.err'

    # Create sbatch file and write
    sbatch_fn = f'submit-{ijob}.submit'
    sbatch_fn = os.path.join(submit_dir, sbatch_fn)
    sbatch_files.append(sbatch_fn)

    with open(sbatch_fn, 'w') as f:
        f.write('#!/bin/bash\n')
        f.write('#SBATCH -p normal\n')
        f.write('#SBATCH -A TG-PHY210118\n')
        f.write('#SBATCH --job-name selection_function\n')
        f.write('#SBATCH --time {}\n'.format(walltime))
        f.write('#SBATCH --mail-type begin\n')
        f.write('#SBATCH --mail-type end\n')
        f.write('#SBATCH --mail-user xwou@mit.edu\n')
        f.write('#SBATCH -o {}\n'.format(log_out_fn))
        f.write('#SBATCH -e {}\n'.format(log_err_fn))
        f.write('#SBATCH --nodes=1\n')
        f.write('#SBATCH --ntasks=1\n')
        
        f.write('source /home1/08052/tg873515/.bashrc\n')
        f.write('conda init\n')
        f.write('cd $WORK/ananke_fire_gaia_dr3\n')
        f.write('conda activate ananke_gdr3\n')
        f.write('export PYTHONPATH=/home1/08052/tg873515/anaconda3/envs/ananke_gdr3/lib/python3.9/site-packages\n')
        f.write('date\n')
        
        f.write(cmd + '\n')

# script to submit all jobs
submit_file = os.path.join(submit_dir, 'submit.sh')
with open(submit_file, 'w') as f:
    for sbatch_fn in sbatch_files:
        f.write('sbatch {}\n'.format(sbatch_fn))
os.chmod(submit_file, 0o744)

