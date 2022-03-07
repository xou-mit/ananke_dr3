# Expect input: python selection_function.py input.hdf5 output.hdf5 load/save random_state.pkl
# Import libraries
import sys
import numpy as np
import pickle
import h5py
import os

# Set up the selection function maps
from selectionfunctions.config import config
config['data_dir'] = './selectionfunctions/data/' # Change this line to where you want to store the selection function maps

import selectionfunctions.cog_ii as CoGII
import selectionfunctions.cog_v as CoGV
from selectionfunctions.source import Source
from selectionfunctions.map import coord2healpix

# Fetch data
print("Fetching data...")
CoGII.fetch()
CoGV.fetch(version='cog_v', subset='astrometry')
CoGV.fetch(version='cog_v', subset='ruwe1p4')
CoGV.fetch(version='cog_v', subset='rvs')


print("Loading general selection function...")
dr3_sf = CoGII.dr3_sf(version='modelAB',crowding=True)

print("Loading astrometry selection function...")
ast_sf = CoGV.subset_sf(map_fname='astrometry_cogv.h5', nside=32,
                basis_options={'needlet':'chisquare', 'p':1.0, 'wavelet_tol':1e-2},
                spherical_basis_directory='SphericalBasis')

print("Loading RUWE selection function...")
ruwe_cm_sf = CoGV.subset_sf(map_fname='ruwe1p4_cogv.h5', nside=32,
                basis_options={'needlet':'chisquare', 'p':1.0, 'wavelet_tol':1e-2},
                spherical_basis_directory='SphericalBasis')

print("Loading RVS selection function...")
rvs_sf = CoGV.subset_sf(map_fname='rvs_cogv.h5', nside=32,
                basis_options={'needlet':'chisquare', 'p':1.0, 'wavelet_tol':1e-2},
                spherical_basis_directory='SphericalBasis')

def append_dataset(fobj, key, data, overwrite=False):
    ''' Append an hdf5 dataset '''
    dataset = fobj.get(key)
    if dataset is None:
        fobj.create_dataset(key, data=data, maxshape=(None,), chunks=True)
    else:
        if overwrite:
            del dataset
            fobj.create_dataset(key, data=data, maxshape=(None,), chunks=True)
        else:
            N = dataset.shape[0]
            N_data = len(data)
            dataset.resize(N + N_data, axis=0)
            dataset[-N_data:] = data

def append_dataset_dict(fobj, data_dict, overwrite=False):
    ''' Append multiple hdf5 dataset '''
    for key, data in data_dict.items():
        append_dataset(fobj, key, data, overwrite)

def selection_function(data,output,indices=(None, None)):
    i_start, i_stop = indices
    
    # read ra, dec, g mag, and rp mag modulus
    ra = data['ra_true'][i_start: i_stop]
    dec = data['dec_true'][i_start: i_stop]
    gmag = data['phot_g_mean_mag'][i_start: i_stop]
    rpmag = data['phot_rp_mean_mag'][i_start: i_stop]
    
    
    selec_data = {}
    # Pre-compute color needed for selection function
    g_grp_mag = gmag-rpmag

    # Pre-compute the index of data that requires manual adjustment in fitting ruwe and RVS selection functions
    G_GRP_floor = -1.
    G_GRP_ceil = 7.
    id_G_GRP_ruwe_l = np.where(g_grp_mag < G_GRP_floor)[0]
    id_G_GRP_ruwe_u = np.where(g_grp_mag > G_GRP_ceil)[0]
    print(len(id_G_GRP_ruwe_l),"sources found to have color redder than ruwe sf edge.")
    print(len(id_G_GRP_ruwe_u),"sources found to have color bluer than ruwe sf edge.")
    # Manually set the out of bound colors to the closest bin value
    g_grp_mag[id_G_GRP_ruwe_l] = G_GRP_floor + 0.1 # Add a margin to prevent numerical error
    g_grp_mag[id_G_GRP_ruwe_u] = G_GRP_ceil - 0.1


    G_GRP_floor = -0.6
    G_GRP_ceil = 2.6
    id_G_GRP_rvs = np.where((g_grp_mag < G_GRP_floor) | (g_grp_mag > G_GRP_ceil))[0]
    print(len(id_G_GRP_rvs),"sources found to have color redder/bluer than RVS sf edge.")
    
    source = Source(ra=ra, dec=dec, unit='deg', frame='icrs', 
                    photometry={'gaia_g':gmag,'gaia_g_gaia_rp':g_grp_mag})
    
    id_G_mag_u = np.where(gmag < 3)[0]
    
    selec_data['prob_selection'] = dr3_sf(source)
    selec_data['selected'] = prng.rand(len(selec_data['prob_selection']))<selec_data['prob_selection']
    
    # Find the expected number of observaitons
    # Get healpix pixels
    hpxidx = coord2healpix(source.coord, 'icrs', dr3_sf._nside, nest=True)
    # Get number of transits from scanning law. The healpix id has to be in icrs coordinate
    selec_data['n_transit'] = dr3_sf._n_field[hpxidx]
    
    # Find the selection probability and use random sampling for whether the object is selected in subsets of the sample
    selec_data['prob_astrometry'] = dr3_sf(source)*ast_sf(source)
    selec_data['selected_astrometry'] = prng.rand(len(selec_data['prob_astrometry']))<selec_data['prob_astrometry']

    # For probability of RUWE<1.4, we are using color dependent selection function without knowledge of G_RP selection function
    # Note that we have manually addressed the edge cases where stars are too blue or too red when we import the data
    # so no extra adjustment needed here
    selec_data['prob_ruwe'] = dr3_sf(source)*ruwe_cm_sf(source)
    selec_data['selected_ruwe'] = prng.rand(len(selec_data['prob_ruwe']))<selec_data['prob_ruwe']

    # For RVS, after applying selection method, manually set the out of bound colors to zero
    selec_data['prob_rvs'] = dr3_sf(source)*rvs_sf(source)
    selec_data['selected_rvs'] = prng.rand(len(selec_data['prob_rvs']))<selec_data['prob_rvs']

    selec_data['prob_rvs'][id_G_GRP_rvs] = 0
    selec_data['selected_rvs'][id_G_GRP_rvs] = False
    
    # Make sure that we remove G < 3 stars as the selection functions are dominated by the prior there
    selec_data['selected'][id_G_mag_u] = False
    selec_data['selected_astrometry'][id_G_mag_u] = False
    selec_data['selected_ruwe'][id_G_mag_u] = False
    selec_data['selected_rvs'][id_G_mag_u] = False
    
    # Reduce the size to only those selected for good ruwe
    # Count how many are left for this batch
    N_batch_selected = len(ra[selec_data['selected_ruwe']])
    print(f'Number of selected sources for this batch {N_batch_selected}')
    # Include the original and selection data columns
    data_slice = {}
    
    # Create an index list for identifying what stars are selected
    index_selec = np.arange(i_start, i_start+len(selec_data['selected_ruwe']))
    data_slice['index_in_mock'] = index_selec[selec_data['selected_ruwe']]
    
    for key in data.keys():
        data_slice[key] = data[key][i_start: i_stop][selec_data['selected_ruwe']]

    for key in selec_data.keys():
        data_slice[key] = selec_data[key][selec_data['selected_ruwe']]
    
    append_dataset_dict(output, data_slice, overwrite=False)


        
# Process the data
f_input = sys.argv[1]

f_selected = sys.argv[2]

batch_size = 100000

# Overwrite file
if os.path.exists(f_selected):
    os.remove(f_selected)

f = h5py.File(f_input, 'r')
fo = h5py.File(f_selected, 'a')
N = len(f['dmod_true'])
print(f'Total number of sources:{N}')
N_batch = (N + batch_size - 1) // batch_size


# Save or load the random state
if sys.argv[3] == 'save':
    print("Saving random state")
    prng = np.random.RandomState()
    state = prng.get_state()
    with open(sys.argv[4], 'wb') as frand: 
        pickle.dump(state, frand)
elif sys.argv[3] == 'load':
    print('Loading random state')
    with open(sys.argv[4], 'rb') as frand:
        state = pickle.load(frand)
    prng = np.random.RandomState()
    prng.set_state(state)
else:
    print('Unexpected save/load instruction for random state')
    sys.exit()
    
for i_batch in range(N_batch):
    print(f'[{i_batch}/{N_batch}]')

    i_start = i_batch * batch_size
    i_stop = i_start + batch_size

    selection_function(f,fo,indices=(i_start, i_stop))
    
f.close()
fo.close()