print("Read drifter data")
print("Read id, month, day, year, latitude, longitude, temperature, zonal velocity, meridional velocity, speed")

import datetime
import numpy as np

fn = '/vagrant/shared/test_data/drifters/buoydata_15001_sep16.dat'

# load drifter id's
ids = np.loadtxt(fn,usecols=(0,))
drifters = {}
for id in np.unique(ids):
    drifters[id] = {}

# load drifter date (month)
mon=np.loadtxt(fn,usecols=(1,))

# load drifter date (day)
day=np.loadtxt(fn,usecols=(2,))

# load drifter date (year)
year=np.loadtxt(fn,usecols=(3,))

# Create numpy array of np.datetime64
tt = np.array([np.datetime64(datetime.datetime(y,m,d)) for y,m,d in zip(year,
    mon, day)])

# load drifter location (latitude)
lat=np.loadtxt(fn,usecols=(4,))

# load drifter location (longitude)
lon=np.loadtxt(fn,usecols=(5,))

# load temperature
temp=np.loadtxt(fn,usecols=(6,))

# load zonal velocity
zon=np.loadtxt(fn,usecols=(7,))

# load meridional velocity
mer=np.loadtxt(fn,usecols=(8,))

# load speed
speed=np.loadtxt(fn,usecols=(9,))

# load error (variance in latitude)
varlat=np.loadtxt(fn,usecols=(10,))

# load error (variance in longitude)
varlon=np.loadtxt(fn,usecols=(11,))

# load error (variance in temperature)
vartemp=np.loadtxt(fn,usecols=(12,))


#print('id',id)
#a=id.shape
#print('id',a)
#print(lon.shape)
#print(lon)
