print("READ DRIFTER METADATA!")
print("READ buoy identification number(id), deployment date(depd), deployment time (dept), deployment latitude (deplat), deployment longitude (deplon), end date (endd), end time (endt) end latitude (endlat),end longitude (end lon), drogue lost date (drolostd), drogue lost time (drolostt)")



import numpy as np
# load drifter id's
id=np.loadtxt('dirfl_15001_sep16.dat',usecols=(0,))

# load drifter deployment date
depd=np.loadtxt('dirfl_15001_sep16.dat',usecols=(4,),dtype='str')

# load drifter deployment time (minutes)
dept=np.loadtxt('dirfl_15001_sep16.dat',usecols=(5,),dtype='str')

# load drifter deployment latitude
deplat=np.loadtxt('dirfl_15001_sep16.dat',usecols=(6,))

# load drifter deployment longitude
deplon=np.loadtxt('dirfl_15001_sep16.dat',usecols=(7,))

# load drifter enddate
endd=np.loadtxt('dirfl_15001_sep16.dat',usecols=(8,),dtype='str')

# load drifter end time (minutes)
endt=np.loadtxt('dirfl_15001_sep16.dat',usecols=(9,),dtype='str')

# load drifter end location (latitude)
endlat=np.loadtxt('dirfl_15001_sep16.dat',usecols=(10,))

# load drifter end location (longitude)
endlon=np.loadtxt('dirfl_15001_sep16.dat',usecols=(11,))

# load drifter drogue lost date
drolostd=np.loadtxt('dirfl_15001_sep16.dat',usecols=(12,),dtype='str')

# load drifter drogue lost time (minutes)
drolostt=np.loadtxt('dirfl_15001_sep16.dat',usecols=(13,),dtype='str')

#print('id',id)
#print('drogue lost date',drolostd)
#a=id.shape
#print('id',a)
