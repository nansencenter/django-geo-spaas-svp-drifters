from __future__ import unicode_literals

import re, datetime
import numpy as np
import pandas as pd

from django.db import models
from django.contrib.gis.geos import LineString

from geospaas.catalog.models import Dataset as CatalogDataset
from geospaas.utils import nansat_filename
from svp_drifters.managers import SVPDrifterManager

class SVPDrifter(CatalogDataset):
    class Meta:
        proxy = True
    objects = SVPDrifterManager()

    def get_trajectory(self, start_time, end_time):
        if not type(start_time)==datetime.datetime:
            raise ValueError('Given times must be of type datetime.datetime')
        if not type(end_time)==datetime.datetime:
            raise ValueError('Given times must be of type datetime.datetime')

        # Could also take the trajectory directly from the geometry given 0.25
        # day sampling frequency...

        m = re.search('^.*drifter\s{1}no\.\s{1}(\d+)$', self.entry_title)
        id = int(m.group(1))
        uu = self.dataseturi_set.get(uri__contains='buoydata')
        fn = nansat_filename(uu.uri)

        # Get all drifter ID's
        ids = np.loadtxt(fn, usecols=(0,))
        # Get indices of current drifter
        ind = np.where(ids==id)
        # Get year, month, day and hour of each sample
        year = np.loadtxt(fn,usecols=(3,))[ind]
        month = np.loadtxt(fn,usecols=(1,))[ind]
        day = np.loadtxt(fn,usecols=(2,))[ind]
        hour = np.remainder(day, np.floor(day))*24
        # Get longitudes and latitudes
        lat = np.loadtxt(fn,usecols=(4,))[ind]
        lon = np.loadtxt(fn,usecols=(5,))[ind]

        # Pandas DataFrame
        df = pd.DataFrame({'year': year, 'month': month, 'day': np.floor(day),
            'hour': hour})
        # Create datetime64 array
        datetimes = pd.to_datetime(df)

        # Pick indices of required trajectory
        t0_diff = np.min(np.abs(datetimes - start_time.replace(tzinfo=None)))
        t1_diff = np.min(np.abs(datetimes - end_time.replace(tzinfo=None)))
        indt0 = np.argmin(np.abs(datetimes - start_time.replace(tzinfo=None)))
        indt1 = np.argmin(np.abs(datetimes - end_time.replace(tzinfo=None)))

        # Return geometry of required trajectory
        return LineString(zip(lon[indt0:indt1], lat[indt0:indt1]))
